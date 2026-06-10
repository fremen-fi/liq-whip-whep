package audio

import (
	"errors"
	"io"
	"log/slog"

	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media/samplebuilder"
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
	logICE(pc, "whip", sess.ID, offerSDP)

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

		// De-jitter before decode: SampleBuilder reorders RTP by sequence
		// number and holds up to whipJitterDelay for a late/missing packet,
		// then emits Opus frames in order. Out-of-order arrivals no longer
		// decode out of order. Gaps (packets dropped past the window) are
		// filled with SILENCE of the exact gap length — NOT Opus PLC, which
		// extrapolates the recent waveform and, fired on every late packet,
		// smears "fragments of the same speech" across the audio. Steady-
		// state cadence smoothing stays downstream in the paced sink.
		const maxLate = 100 // hard cap on buffered packets; the time bound governs
		sb := samplebuilder.New(maxLate, &codecs.OpusPacket{}, 48000,
			samplebuilder.WithMaxTimeDelay(s.whipJitterDelay()))

		slog.Info("whip: track started",
			"session", sess.ID, "kind", track.Kind().String(),
			"codec", track.Codec().MimeType, "channels", channels,
			"jitter", s.whipJitterDelay())

		// Max Opus packet at 48 kHz is 120 ms × 48 = 5760 samples per
		// channel. Allocate once and reuse.
		const maxSamples = 5760
		decoded := make([]int16, maxSamples*channels)
		mono := make([]int16, maxSamples)

		// Downmix one decoded frame (n samples/channel) to mono and push it
		// to the sink.
		writeFrame := func(frame []int16, n int) {
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

		// Fill gaps with silence of the exact missing duration so timing
		// stays correct, capped so a real dropout can't spew unbounded
		// frames. silence stays all-zero (mono) and is never written to.
		const maxConceal = 10 // ~200 ms at 20 ms frames
		silence := make([]int16, maxSamples)
		conceal := func(count uint16) {
			dur, e := dec.LastPacketDuration()
			if e != nil || dur <= 0 || dur > maxSamples {
				dur = 960 // 20 ms @ 48 kHz fallback
			}
			if count > maxConceal {
				count = maxConceal
			}
			for k := uint16(0); k < count; k++ {
				if err := s.Sink.WriteFrame(silence[:dur]); err != nil {
					slog.Debug("whip: sink write", "err", err)
					return
				}
			}
		}

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
			sb.Push(pkt)
			for {
				sample := sb.Pop()
				if sample == nil {
					break
				}
				if sample.PrevDroppedPackets > 0 {
					conceal(sample.PrevDroppedPackets)
				}
				if len(sample.Data) == 0 {
					continue
				}
				// dec.Decode returns frame count (samples per channel).
				n, err := dec.Decode(sample.Data, decoded)
				if err != nil {
					slog.Debug("whip: decode", "session", sess.ID, "err", err)
					continue
				}
				writeFrame(decoded[:n*channels], n)
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
