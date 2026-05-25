package audio

import (
	"errors"
	"io"
	"log/slog"

	"github.com/pion/webrtc/v4"
	opus "gopkg.in/hraban/opus.v2"
)

// startWHIPSession creates a peer connection that RECEIVES Opus audio
// from the browser (host mic). Each incoming RTP packet is decoded with
// libopus to int16 PCM at 48 kHz; we downmix to mono and feed it to the
// PCMSink, which forwards it to Liquidsoap as streaming WAV.
//
// We negotiate stereo so the browser is free to send either, then mix
// down to mono on our side — that way the WHIP encoder isn't constrained
// and Liquidsoap's mic input stays a stable mono format.
func (s *Server) startWHIPSession(sess *Session, offerSDP string) (string, error) {
	pc, err := newPeerConnection()
	if err != nil {
		return "", err
	}
	sess.pc = pc

	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	}); err != nil {
		_ = pc.Close()
		return "", err
	}

	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		// Decoder is created at 48 kHz with the channel count the
		// browser actually negotiated. libopus accepts mono or stereo
		// at either decoder configuration; we read it from the track.
		channels := int(track.Codec().Channels)
		if channels != 1 && channels != 2 {
			channels = 2 // safe default
		}
		dec, err := opus.NewDecoder(48000, channels)
		if err != nil {
			slog.Warn("whip: decoder", "session", sess.ID, "err", err)
			return
		}
		slog.Info("whip: track started",
			"session", sess.ID, "kind", track.Kind().String(),
			"codec", track.Codec().MimeType, "channels", channels)

		// Max Opus packet at 48 kHz is 120 ms × 48 = 5760 samples per
		// channel. Allocate once and reuse.
		const maxSamples = 5760
		decoded := make([]int16, maxSamples*channels)
		mono := make([]int16, maxSamples)

		for {
			if sess.Closed() {
				return
			}
			pkt, _, err := track.ReadRTP()
			if err != nil {
				if errors.Is(err, io.EOF) {
					slog.Info("whip: track ended", "session", sess.ID)
					return
				}
				slog.Debug("whip: read rtp", "session", sess.ID, "err", err)
				return
			}
			if len(pkt.Payload) == 0 {
				continue
			}
			n, err := dec.Decode(pkt.Payload, decoded)
			if err != nil {
				slog.Debug("whip: decode", "session", sess.ID, "err", err)
				continue
			}
			// dec.Decode returns frame count (samples per channel).
			frame := decoded[:n*channels]
			out := mono[:n]
			if channels == 1 {
				copy(out, frame)
			} else {
				// Stereo → mono by averaging L+R. Avoids clipping vs
				// summing, costs nothing here.
				for i := 0; i < n; i++ {
					out[i] = int16((int32(frame[2*i]) + int32(frame[2*i+1])) / 2)
				}
			}
			if err := s.Sink.WriteFrame(out); err != nil {
				slog.Debug("whip: sink write", "err", err)
			}
		}
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		slog.Info("whip: connection state", "session", sess.ID, "state", state.String())
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
