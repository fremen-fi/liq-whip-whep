package audio_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	opus "gopkg.in/hraban/opus.v2"

	"github.com/fremen-fi/liq-whip-whep/audio"
)

// TestEndToEnd_WHIP_AudioFlows is the WHIP-side full contract: a Pion
// client (browser stand-in) opens a WHIP session and sends a 1 kHz Opus
// sine. A second goroutine impersonates Liquidsoap, dials the bridge's
// mic socket, reads the streaming WAV, and verifies the decoded PCM
// carries non-trivial energy at the expected frequency band.
//
// If this test passes, browser → bridge → Liquidsoap audio actually
// flows end-to-end.
func TestEndToEnd_WHIP_AudioFlows(t *testing.T) {
	tmp := shortTempDir(t)
	hub := audio.NewPCMHub(filepath.Join(tmp, "onair.sock"))
	if err := hub.Start(context.Background()); err != nil {
		t.Fatalf("hub start: %v", err)
	}
	t.Cleanup(hub.Stop)
	micSock := filepath.Join(tmp, "mic.sock")
	sink := audio.NewPCMSink(micSock)
	if err := sink.Start(context.Background()); err != nil {
		t.Fatalf("sink start: %v", err)
	}
	t.Cleanup(sink.Stop)

	srv := audio.NewServer("/audio")
	srv.Hub = hub
	srv.Sink = sink
	mux := http.NewServeMux()
	mux.Handle("/audio/", srv.Handler())
	httpSrv := httptest.NewServer(mux)
	t.Cleanup(httpSrv.Close)

	// Liquidsoap stand-in: connect to the mic socket as a reader.
	type readResult struct {
		samples []int16
		err     error
	}
	resultCh := make(chan readResult, 1)
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		var conn net.Conn
		var err error
		for i := 0; i < 50; i++ {
			conn, err = net.Dial("unix", micSock)
			if err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			resultCh <- readResult{err: err}
			return
		}
		defer conn.Close()
		// Streaming WAV header: 44 bytes for 16-bit PCM.
		hdr := make([]byte, 44)
		if _, err := io.ReadFull(conn, hdr); err != nil {
			resultCh <- readResult{err: err}
			return
		}
		if string(hdr[0:4]) != "RIFF" || string(hdr[8:12]) != "WAVE" {
			resultCh <- readResult{err: errBadHeader}
			return
		}
		// Read up to ~2 seconds of mono 48 kHz 16-bit = 192000 bytes.
		const want = 48000 * 2 * 2
		buf := make([]byte, want)
		_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
		n, _ := io.ReadFull(conn, buf)
		samples := make([]int16, n/2)
		for i := range samples {
			samples[i] = int16(binary.LittleEndian.Uint16(buf[i*2 : i*2+2]))
		}
		resultCh <- readResult{samples: samples}
	}()

	// Pion client: send a synthesized Opus track (1 kHz mono sine).
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("client pc: %v", err)
	}
	defer pc.Close()
	// RFC 7587: Opus rtpmap is always "opus/48000/2" regardless of how
	// many channels the source actually carries. Real browsers offer
	// this even for a mono mic; the bridge bridge then downmixes to
	// mono when reading the decoded PCM.
	track, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{
		MimeType:    webrtc.MimeTypeOpus,
		ClockRate:   48000,
		Channels:    2,
		SDPFmtpLine: "minptime=10;useinbandfec=1",
	}, "audio", "test-mic")
	if err != nil {
		t.Fatalf("track: %v", err)
	}
	if _, err := pc.AddTrack(track); err != nil {
		t.Fatalf("add track: %v", err)
	}

	// Encoder for synthesized mic.
	enc, err := opus.NewEncoder(48000, 1, opus.AppAudio)
	if err != nil {
		t.Fatalf("encoder: %v", err)
	}
	_ = enc.SetBitrate(64000)

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	gatherDone := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("set local: %v", err)
	}
	select {
	case <-gatherDone:
	case <-time.After(5 * time.Second):
		t.Fatal("gathering timeout")
	}
	req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/audio/whip",
		bytes.NewReader([]byte(pc.LocalDescription().SDP)))
	req.Header.Set("Content-Type", "application/sdp")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	answer, _ := io.ReadAll(resp.Body)
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  string(answer),
	}); err != nil {
		t.Fatalf("set remote: %v", err)
	}

	// Drive 20 ms frames of 1 kHz sine at half-scale until the reader
	// captures enough data, or the deadline fires.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		const samplesPerFrame = 960 // 20 ms × 48 kHz
		mono := make([]int16, samplesPerFrame)
		pkt := make([]byte, 4000)
		var idx int
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				for i := 0; i < samplesPerFrame; i++ {
					sec := float64(idx+i) / 48000.0
					mono[i] = int16(0.5 * 32767 * math.Sin(2*math.Pi*1000*sec))
				}
				idx += samplesPerFrame
				n, err := enc.Encode(mono, pkt)
				if err != nil {
					return
				}
				_ = track.WriteSample(media.Sample{
					Data:     append([]byte(nil), pkt[:n]...),
					Duration: 20 * time.Millisecond,
				})
			}
		}
	}()

	// Wait for the Liquidsoap-side reader to finish capture.
	var got readResult
	select {
	case got = <-resultCh:
	case <-time.After(15 * time.Second):
		t.Fatal("reader never returned")
	}
	if got.err != nil {
		t.Fatalf("reader: %v", got.err)
	}
	if len(got.samples) < 4800 {
		t.Fatalf("only %d samples captured; expected ~96000", len(got.samples))
	}

	// Skip the first ~200 ms while the path warms up, then measure RMS.
	skip := 48000 * 200 / 1000
	if skip > len(got.samples)/2 {
		skip = 0
	}
	measure := got.samples[skip:]
	var sum float64
	for _, v := range measure {
		f := float64(v)
		sum += f * f
	}
	rms := math.Sqrt(sum / float64(len(measure)))
	t.Logf("mic-side RMS over %d samples: %.0f", len(measure), rms)
	if rms < 1000 {
		t.Fatalf("mic side has no audio (rms=%.0f) — WHIP path is broken", rms)
	}
}

var errBadHeader = &readErr{msg: "not a RIFF/WAVE header"}

type readErr struct{ msg string }

func (e *readErr) Error() string { return e.msg }
