// Package audio implements the WHEP (downlink, on-air monitor) and WHIP
// (uplink, host-mic ingest) endpoints. It speaks WebRTC to the browser
// via Pion and exchanges streaming WAV with Liquidsoap over Unix domain
// sockets.
//
// Audio path:
//
//   - WHEP: PCMHub accepts a streaming WAV connection from Liquidsoap on
//     the on-air socket, parses the header (16/24-bit, any sample rate),
//     resamples to 48 kHz if necessary, slices into 20 ms frames, and
//     fans them out to all subscribed WHEP sessions. Each session has
//     its own libopus encoder; encoded packets become RTP samples.
//   - WHIP: each session decodes incoming Opus RTP with libopus,
//     downmixes to mono if needed, and writes 48 kHz mono int16 frames
//     into PCMSink. PCMSink wraps them in a streaming WAV and serves a
//     single Liquidsoap reader on the mic socket.
package audio

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"
)

// Session is one live WebRTC peer connection. Both WHEP and WHIP create a
// Session; what differs is the direction of the audio track.
type Session struct {
	ID        string
	Direction string // "down" (whep) or "up" (whip)
	CreatedAt time.Time

	pc       *webrtc.PeerConnection
	closed   atomic.Bool
	stopOnce sync.Once
	stopFn   func()
}

// Manager keeps every active session indexed by ID so DELETE requests and
// graceful shutdown can find them.
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewManager() *Manager {
	return &Manager{sessions: map[string]*Session{}}
}

func (m *Manager) Add(s *Session) {
	m.mu.Lock()
	m.sessions[s.ID] = s
	m.mu.Unlock()
}

func (m *Manager) Get(id string) (*Session, bool) {
	m.mu.RLock()
	s, ok := m.sessions[id]
	m.mu.RUnlock()
	return s, ok
}

func (m *Manager) Remove(id string) {
	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
}

// CloseAll terminates every session. Used at shutdown.
func (m *Manager) CloseAll() {
	m.mu.Lock()
	all := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		all = append(all, s)
	}
	m.sessions = map[string]*Session{}
	m.mu.Unlock()
	for _, s := range all {
		s.Close()
	}
}

// Close tears down the peer connection.
func (s *Session) Close() {
	s.stopOnce.Do(func() {
		s.closed.Store(true)
		if s.stopFn != nil {
			s.stopFn()
		}
		if s.pc != nil {
			_ = s.pc.Close()
		}
	})
}

// Closed reports whether Close has been called.
func (s *Session) Closed() bool { return s.closed.Load() }

func newSessionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// newPeerConnection builds a Pion peer connection with a MediaEngine that
// advertises stereo Opus. Pion's default MediaEngine registers Opus
// without `stereo=1; sprop-stereo=1` in the fmtp; Chrome respects that
// and decodes mono regardless of what bytes we send. We register Opus
// ourselves so the answer SDP carries the stereo signal Chrome needs.
func newPeerConnection() (*webrtc.PeerConnection, error) {
	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:     webrtc.MimeTypeOpus,
			ClockRate:    48000,
			Channels:     2,
			SDPFmtpLine:  "minptime=10;useinbandfec=1;stereo=1;sprop-stereo=1",
			RTCPFeedback: nil,
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, err
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(me))
	pc, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: []webrtc.ICEServer{}})
	if err != nil {
		return nil, err
	}
	return pc, nil
}

// answerOffer applies the offer SDP, generates an answer, sets it as local
// description, and waits for ICE gathering to finish before returning the
// final SDP.
func answerOffer(pc *webrtc.PeerConnection, offerSDP string) (string, error) {
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP,
	}); err != nil {
		return "", err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return "", err
	}
	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		return "", err
	}
	select {
	case <-gatherComplete:
	case <-time.After(5 * time.Second):
	}
	desc := pc.LocalDescription()
	if desc == nil {
		return "", errors.New("local description not set")
	}
	return forceStereoOpus(desc.SDP), nil
}

// forceStereoOpus rewrites the Opus a=fmtp line in an SDP to advertise
// stereo. Pion's answer copies the offerer's fmtp verbatim; if the
// browser's offer didn't include stereo=1 / sprop-stereo=1 then Chrome
// decodes the incoming Opus as mono regardless of how many channels the
// payload actually carries. We're a server with a known stereo egress —
// hard-code the params on the way out.
func forceStereoOpus(sdp string) string {
	lines := strings.Split(sdp, "\r\n")
	useCRLF := len(lines) > 1
	if !useCRLF {
		lines = strings.Split(sdp, "\n")
	}
	// Find Opus payload type from the first matching rtpmap.
	pt := ""
	for _, l := range lines {
		if strings.HasPrefix(l, "a=rtpmap:") && strings.Contains(strings.ToLower(l), "opus/") {
			rest := strings.TrimPrefix(l, "a=rtpmap:")
			sp := strings.IndexByte(rest, ' ')
			if sp > 0 {
				pt = rest[:sp]
				break
			}
		}
	}
	if pt == "" {
		return sdp
	}
	prefix := "a=fmtp:" + pt + " "
	want := []string{"stereo=1", "sprop-stereo=1"}
	found := false
	for i, l := range lines {
		if !strings.HasPrefix(l, prefix) {
			continue
		}
		found = true
		params := strings.TrimPrefix(l, prefix)
		parts := strings.Split(params, ";")
		for _, w := range want {
			already := false
			for _, p := range parts {
				if strings.TrimSpace(p) == w {
					already = true
					break
				}
			}
			if !already {
				parts = append(parts, w)
			}
		}
		lines[i] = prefix + strings.Join(parts, ";")
	}
	if !found {
		// Inject an fmtp line right after the Opus rtpmap.
		out := make([]string, 0, len(lines)+1)
		injected := false
		for _, l := range lines {
			out = append(out, l)
			if !injected && strings.HasPrefix(l, "a=rtpmap:"+pt+" ") {
				out = append(out, prefix+strings.Join(want, ";"))
				injected = true
			}
		}
		lines = out
	}
	if useCRLF {
		return strings.Join(lines, "\r\n")
	}
	return strings.Join(lines, "\n")
}
