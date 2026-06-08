package audio

import (
	"log/slog"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	opus "gopkg.in/hraban/opus.v2"
)

// startWHEPSession creates a peer connection that PUSHES Opus audio to
// the browser. It subscribes to the bridge's PCMHub for 20 ms frames at
// 48 kHz, encodes each frame with libopus, and writes it as a sample on
// a TrackLocalStaticSample.
//
// While the hub has no upstream Liquidsoap stream connected, the
// subscriber channel is idle — we don't push anything, and Pion's RTP
// scheduler simply doesn't emit packets. WebRTC's Opus PLC on the
// browser side handles the gap as silence.
func (s *Server) startWHEPSession(sess *Session, offerSDP string) (string, error) {
	pc, err := newPeerConnection()
	if err != nil {
		return "", err
	}
	sess.pc = pc

	// Stereo Opus is fine even when the upstream is mono — libopus will
	// duplicate the single channel internally. Choosing stereo at SDP
	// time avoids renegotiation if the operator switches Liquidsoap to
	// stereo output.
	const channels = 2
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    channels,
			SDPFmtpLine: "minptime=10;useinbandfec=1;stereo=1;sprop-stereo=1",
		},
		"audio", "cr-monitor",
	)
	if err != nil {
		_ = pc.Close()
		return "", err
	}
	if _, err := pc.AddTrack(track); err != nil {
		_ = pc.Close()
		return "", err
	}

	enc, err := opus.NewEncoder(48000, channels, opus.AppAudio)
	if err != nil {
		_ = pc.Close()
		return "", err
	}
	// 96 kbps stereo is more than enough for monitor-quality music; we
	// can wire RTCP REMB later to adapt this live.
	_ = enc.SetBitrate(160000)
	_ = enc.SetInBandFEC(true)

	frameSamples := 48000 * frameMS / 1000 // per channel, before interleave
	pktBuf := make([]byte, 4000)           // libopus packets max ~1275 bytes; 4 KB is safe

	stop := make(chan struct{})
	sess.stopFn = func() { close(stop) }

	sub := s.Hub.Subscribe()

	go func() {
		defer s.Hub.Unsubscribe(sub)
		// We always write 20 ms regardless of source channel count; if
		// the source is mono we expand to stereo before encoding.
		stereo := make([]int16, frameSamples*2)

		for {
			select {
			case <-stop:
				return
			case frame, ok := <-sub.Frames():
				if !ok {
					return
				}
				if sess.Closed() {
					return
				}
				expandTo(stereo, frame, channelsOf(frame, frameSamples))
				n, err := enc.Encode(stereo, pktBuf)
				if err != nil {
					slog.Debug("whep: encode", "session", sess.ID, "err", err)
					continue
				}
				if err := track.WriteSample(media.Sample{
					Data:     append([]byte(nil), pktBuf[:n]...),
					Duration: frameMS * time.Millisecond,
				}); err != nil {
					slog.Debug("whep: write sample", "err", err)
					return
				}
			}
		}
	}()

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		slog.Info("whep: connection state", "session", sess.ID, "state", state.String())
		switch state {
		case webrtc.PeerConnectionStateClosed,
			webrtc.PeerConnectionStateFailed,
			webrtc.PeerConnectionStateDisconnected:
			sess.Close()
		}
	})

	answer, err := answerOffer(pc, offerSDP)
	if err != nil {
		_ = pc.Close()
		return "", err
	}
	return answer, nil
}

// channelsOf infers whether the hub frame is mono or stereo by length:
// the hub guarantees frame length == frameSamples * channels.
func channelsOf(frame []int16, frameSamples int) int {
	if len(frame) == frameSamples {
		return 1
	}
	return 2
}

// expandTo copies src into dst as interleaved stereo. If src is already
// stereo (length 2×frameSamples), it's a straight copy. If mono, each
// sample is duplicated to L/R.
func expandTo(dst []int16, src []int16, srcChannels int) {
	frameSamples := len(dst) / 2
	if srcChannels == 2 {
		copy(dst, src)
		return
	}
	for i := range frameSamples {
		dst[2*i] = src[i]
		dst[2*i+1] = src[i]
	}
}
