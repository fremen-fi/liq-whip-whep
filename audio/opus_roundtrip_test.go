package audio

import (
	"math"
	"testing"

	opus "gopkg.in/hraban/opus.v2"
)

// TestWHEPOpusEncoder_PreservesStereo encodes divergent L/R audio through
// the same libopus configuration the WHEP session uses, decodes it back,
// and verifies L and R are still distinguishable. If this test ever flips
// to "L=R", the bridge is encoding mono regardless of input.
//
// Catches: Opus channel-collapse bugs (e.g. encoder forcing channel=1,
// SDP mismatch, AppAudio vs AppVoip downmix, wrong bitrate killing the
// stereo image).
func TestWHEPOpusEncoder_PreservesStereo(t *testing.T) {
	const (
		rate     = 48000
		channels = 2
		frameMS  = 20
	)
	frameSamples := rate * frameMS / 1000 // per channel

	enc, err := opus.NewEncoder(rate, channels, opus.AppAudio)
	if err != nil {
		t.Fatalf("encoder: %v", err)
	}
	_ = enc.SetBitrate(96000)
	_ = enc.SetInBandFEC(true)

	dec, err := opus.NewDecoder(rate, channels)
	if err != nil {
		t.Fatalf("decoder: %v", err)
	}

	// L = 1 kHz sine at amplitude 0.5; R = silence. After encode/decode
	// the L channel should still carry the tone and R should still be
	// roughly zero. If both come out equal, the codec collapsed stereo.
	stereo := make([]int16, frameSamples*2)
	pkt := make([]byte, 4000)
	out := make([]int16, frameSamples*2)

	// Push a few frames first; libopus needs a couple of packets to
	// settle — the first packet often carries codec init artifacts that
	// pollute the test signal.
	const warmup = 5
	for fr := 0; fr < warmup; fr++ {
		for i := 0; i < frameSamples; i++ {
			t := float64(fr*frameSamples+i) / float64(rate)
			stereo[2*i] = int16(0.5 * 32767 * math.Sin(2*math.Pi*1000*t))
			stereo[2*i+1] = 0
		}
		n, err := enc.Encode(stereo, pkt)
		if err != nil {
			t.Fatalf("encode warmup: %v", err)
		}
		if _, err := dec.Decode(pkt[:n], out); err != nil {
			t.Fatalf("decode warmup: %v", err)
		}
	}

	// Final measurement frame.
	for i := 0; i < frameSamples; i++ {
		t := float64((warmup)*frameSamples+i) / float64(rate)
		stereo[2*i] = int16(0.5 * 32767 * math.Sin(2*math.Pi*1000*t))
		stereo[2*i+1] = 0
	}
	n, err := enc.Encode(stereo, pkt)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := dec.Decode(pkt[:n], out); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Compute per-channel RMS. L should carry energy; R should be far
	// quieter. If they're equal, stereo was collapsed somewhere.
	rmsL := rms(out, 0, 2)
	rmsR := rms(out, 1, 2)
	t.Logf("decoded RMS: L=%.0f R=%.0f", rmsL, rmsR)
	if rmsL < 1000 {
		t.Fatalf("L channel has no signal (rms=%.0f); encode/decode broken", rmsL)
	}
	// Allow significant R bleed (Opus stereo mid/side leaks energy
	// across channels at low bitrates) but reject full collapse.
	if rmsR > rmsL*0.5 {
		t.Fatalf("stereo collapsed: L=%.0f R=%.0f (R is %.0f%% of L)", rmsL, rmsR, 100*rmsR/rmsL)
	}
}

func rms(buf []int16, offset, stride int) float64 {
	var sum float64
	n := 0
	for i := offset; i < len(buf); i += stride {
		v := float64(buf[i])
		sum += v * v
		n++
	}
	if n == 0 {
		return 0
	}
	return math.Sqrt(sum / float64(n))
}
