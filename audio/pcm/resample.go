package pcm

import (
	"math"
)

// Resampler converts interleaved int16 PCM from one sample rate to
// another using rational polyphase filtering with a Kaiser-windowed sinc
// kernel. It's stateful: feed it samples in any chunk size, pull output
// samples whenever you need them.
//
// libopus accepts 8/12/16/24/48 kHz input directly, so the typical "is
// resampling required" check is `inRate != outRate && !isOpusRate(inRate)`;
// callers can skip this when no conversion is needed.
type Resampler struct {
	channels    int
	up          int         // L: upsample factor
	down        int         // M: downsample factor
	taps        [][]float32 // taps[phase][i], length = numPhases × tapsPerPhase
	tpp         int         // taps per phase
	hist        [][]float32 // per-channel history of last (tpp-1) input samples
	phase       int         // current polyphase phase ∈ [0, up)
	passthrough bool        // inRate == outRate; skip filtering entirely
}

// NewResampler builds a resampler for the given conversion. inRate and
// outRate are reduced to lowest terms internally. Channels must match the
// stream you'll feed in.
//
// transBW is the transition bandwidth as a fraction of the lower Nyquist
// (e.g. 0.1 = 10%). Smaller is sharper but more taps. 0.1 is reasonable
// for monitoring.
func NewResampler(inRate, outRate, channels int) *Resampler {
	if inRate == outRate {
		return &Resampler{channels: channels, passthrough: true}
	}
	g := gcd(inRate, outRate)
	up := outRate / g
	down := inRate / g

	// Cutoff is the lower Nyquist scaled to the upsampled rate's units.
	// In normalized frequency (1.0 = upsampled rate), cutoff = min(1/up, 1/down)/2.
	cutoff := 0.5 / float64(max(up, down))
	// Transition band 10% of cutoff — a Kaiser β≈8 gives ~80 dB stopband.
	transBW := cutoff * 0.1
	beta := 8.0
	// Tap count from Kaiser design: N ≈ (A−8)/(2.285·Δω). For A=80 dB, Δω = 2π·transBW.
	N := int(math.Ceil((80.0-8.0)/(2.285*2*math.Pi*transBW))) | 1 // make odd
	N = max(N, 31)
	// Ensure N is a multiple of up so each phase has the same tap count.
	if r := N % up; r != 0 {
		N += up - r
	}
	tpp := N / up

	taps := make([][]float32, up)
	for p := range taps {
		taps[p] = make([]float32, tpp)
	}
	mid := float64(N-1) / 2.0
	for n := 0; n < N; n++ {
		x := float64(n) - mid
		var s float64
		if x == 0 {
			s = 2 * cutoff
		} else {
			s = math.Sin(2*math.Pi*cutoff*x) / (math.Pi * x)
		}
		// Kaiser window
		r := 2.0*float64(n)/float64(N-1) - 1.0
		w := i0(beta*math.Sqrt(1-r*r)) / i0(beta)
		h := s * w
		// Scale by up so the polyphase output preserves amplitude.
		h *= float64(up)
		// Polyphase decomposition: phase p = n mod up, position i = n / up.
		p := n % up
		i := n / up
		taps[p][i] = float32(h)
	}

	hist := make([][]float32, channels)
	for c := range hist {
		hist[c] = make([]float32, tpp)
	}

	return &Resampler{
		channels: channels,
		up:       up,
		down:     down,
		taps:     taps,
		tpp:      tpp,
		hist:     hist,
	}
}

// Process consumes interleaved int16 input samples and appends interleaved
// int16 output samples to dst, returning the new dst slice. Output sample
// count varies with input due to fractional phase tracking.
func (r *Resampler) Process(in []int16, dst []int16) []int16 {
	if len(in) == 0 {
		return dst
	}
	if r.passthrough {
		return append(dst, in...)
	}
	frames := len(in) / r.channels
	for f := range frames {
		// Shift each channel's history left and append the new sample at the end.
		for c := 0; c < r.channels; c++ {
			copy(r.hist[c], r.hist[c][1:])
			r.hist[c][r.tpp-1] = float32(in[f*r.channels+c])
		}
		// Advance one input sample → emit zero or more output samples
		// depending on how many phases fall in this input step.
		// In rational resampling, output samples per input ≈ up/down.
		// We loop while phase < up*down (conceptually) and emit at the
		// step boundaries. Concretely: each input bumps a virtual counter
		// by up; each output consumes down from it.
		r.phase += r.up
		for r.phase >= r.down {
			r.phase -= r.down
			// Output phase index = (up - phase) mod up — but our taps were
			// indexed so phase 0 corresponds to the most-recent sample.
			p := r.phase % r.up
			tp := r.taps[p]
			for c := 0; c < r.channels; c++ {
				h := r.hist[c]
				var acc float32
				// Convolve: most-recent sample is h[tpp-1], align with tp[0].
				// Reverse iteration keeps the loop tight.
				for i := 0; i < r.tpp; i++ {
					acc += tp[i] * h[r.tpp-1-i]
				}
				// Clip to int16 range.
				v := acc
				if v > 32767 {
					v = 32767
				} else if v < -32768 {
					v = -32768
				}
				dst = append(dst, int16(v))
			}
		}
	}
	return dst
}

// IsOpusRate reports whether libopus accepts this rate as direct input.
func IsOpusRate(r int) bool {
	switch r {
	case 8000, 12000, 16000, 24000, 48000:
		return true
	}
	return false
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	if a < 0 {
		return -a
	}
	return a
}

// i0 computes the modified Bessel function of the first kind, order 0.
// Series converges fast for the Kaiser β values we use (β ≤ 12).
func i0(x float64) float64 {
	sum := 1.0
	term := 1.0
	xx := x * x / 4
	for k := 1; k < 50; k++ {
		term *= xx / float64(k*k)
		sum += term
		if term < 1e-12*sum {
			break
		}
	}
	return sum
}
