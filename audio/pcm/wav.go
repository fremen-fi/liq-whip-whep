// Package pcm decodes a streaming WAV input from Liquidsoap and produces
// 20 ms frames of int16 PCM at 48 kHz, suitable for handing to libopus.
//
// Liquidsoap's WAV output isn't a fixed format — the operator picks the
// sample rate and bit depth in their .liq script. We commit to handling
// the realistic combinations:
//
//   - PCM_FORMAT (fmt code 1) at 16-bit
//   - PCM_FORMAT (fmt code 1) at 24-bit
//   - mono or stereo
//   - any sample rate Liquidsoap will emit (we resample to 48 kHz)
//
// Float WAV (fmt code 3) and 32-bit int aren't supported — Liquidsoap can
// emit them but we don't need them and rejecting upfront is clearer than
// silent miscoding.
package pcm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Format describes a WAV stream the reader has identified.
type Format struct {
	SampleRate    int
	Channels      int
	BitsPerSample int
}

// WAVReader streams interleaved int16 samples from a WAV input. It blocks
// on the underlying reader, so callers should put it in its own goroutine.
type WAVReader struct {
	r   io.Reader
	fmt Format
	buf []byte // staging for one chunk read
}

// NewWAVReader parses the RIFF/WAVE header and stops when it has the data
// chunk header in hand. Subsequent reads return interleaved int16 samples.
//
// The data chunk size in the header is ignored: Liquidsoap writes streaming
// WAV with a sentinel size (often 0xFFFFFFFF or 0), and we want to keep
// reading until EOF.
func NewWAVReader(r io.Reader) (*WAVReader, error) {
	var hdr [12]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("read riff header: %w", err)
	}
	if string(hdr[0:4]) != "RIFF" || string(hdr[8:12]) != "WAVE" {
		return nil, errors.New("not a RIFF/WAVE stream")
	}
	wr := &WAVReader{r: r, buf: make([]byte, 0, 4096)}

	for {
		var ch [8]byte
		if _, err := io.ReadFull(r, ch[:]); err != nil {
			return nil, fmt.Errorf("read chunk header: %w", err)
		}
		id := string(ch[0:4])
		size := binary.LittleEndian.Uint32(ch[4:8])
		switch id {
		case "fmt ":
			if size < 16 {
				return nil, fmt.Errorf("fmt chunk too small: %d", size)
			}
			body := make([]byte, size)
			if _, err := io.ReadFull(r, body); err != nil {
				return nil, fmt.Errorf("read fmt: %w", err)
			}
			fmtCode := binary.LittleEndian.Uint16(body[0:2])
			channels := int(binary.LittleEndian.Uint16(body[2:4]))
			rate := int(binary.LittleEndian.Uint32(body[4:8]))
			bps := int(binary.LittleEndian.Uint16(body[14:16]))
			if fmtCode != 1 {
				return nil, fmt.Errorf("unsupported wav fmt code %d (need PCM=1)", fmtCode)
			}
			if channels != 1 && channels != 2 {
				return nil, fmt.Errorf("unsupported channel count %d", channels)
			}
			if bps != 16 && bps != 24 {
				return nil, fmt.Errorf("unsupported bit depth %d (need 16 or 24)", bps)
			}
			if rate <= 0 {
				return nil, fmt.Errorf("invalid sample rate %d", rate)
			}
			wr.fmt = Format{SampleRate: rate, Channels: channels, BitsPerSample: bps}
		case "data":
			if wr.fmt.SampleRate == 0 {
				return nil, errors.New("data chunk before fmt chunk")
			}
			return wr, nil
		default:
			// Unknown chunk; skip its body. WAV chunks are word-aligned.
			padded := int64(size) + int64(size&1)
			if _, err := io.CopyN(io.Discard, r, padded); err != nil {
				return nil, fmt.Errorf("skip %s chunk: %w", id, err)
			}
		}
	}
}

// Format reports the input format the reader detected.
func (w *WAVReader) Format() Format { return w.fmt }

// ReadSamples fills dst with up to len(dst) interleaved int16 samples and
// returns the count actually read. It returns io.EOF when the underlying
// reader is exhausted.
//
// 24-bit input is sign-extended and shifted right by 8 bits to fit int16.
// We accept the small precision loss because libopus consumes int16
// directly, and the broadcast monitor doesn't need 24-bit headroom.
func (w *WAVReader) ReadSamples(dst []int16) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	bytesPer := w.fmt.BitsPerSample / 8
	need := len(dst) * bytesPer
	if cap(w.buf) < need {
		w.buf = make([]byte, need)
	} else {
		w.buf = w.buf[:need]
	}
	n, err := io.ReadFull(w.r, w.buf)
	// Decode whatever whole samples we got, even on a short read at EOF.
	whole := n / bytesPer
	switch w.fmt.BitsPerSample {
	case 16:
		for i := range whole {
			dst[i] = int16(binary.LittleEndian.Uint16(w.buf[i*2 : i*2+2]))
		}
	case 24:
		for i := range whole {
			b0 := uint32(w.buf[i*3])
			b1 := uint32(w.buf[i*3+1])
			b2 := uint32(w.buf[i*3+2])
			s := int32(b0 | b1<<8 | b2<<16)
			// Sign-extend from 24 to 32 bits.
			if s&0x800000 != 0 {
				s |= ^0xFFFFFF
			}
			dst[i] = int16(s >> 8)
		}
	}
	if err == io.ErrUnexpectedEOF && whole > 0 {
		err = nil
	}
	return whole, err
}
