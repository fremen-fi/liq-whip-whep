package pcm_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"

	"github.com/fremen-fi/liq-whip-whep/audio/pcm"
)

func buildWAV(t *testing.T, rate int, channels int, bps int, samples [][]int32) []byte {
	t.Helper()
	bytesPerSample := bps / 8
	dataLen := len(samples) * channels * bytesPerSample
	buf := &bytes.Buffer{}
	buf.WriteString("RIFF")
	binary.Write(buf, binary.LittleEndian, uint32(36+dataLen))
	buf.WriteString("WAVE")
	buf.WriteString("fmt ")
	binary.Write(buf, binary.LittleEndian, uint32(16))
	binary.Write(buf, binary.LittleEndian, uint16(1))
	binary.Write(buf, binary.LittleEndian, uint16(channels))
	binary.Write(buf, binary.LittleEndian, uint32(rate))
	binary.Write(buf, binary.LittleEndian, uint32(rate*channels*bytesPerSample))
	binary.Write(buf, binary.LittleEndian, uint16(channels*bytesPerSample))
	binary.Write(buf, binary.LittleEndian, uint16(bps))
	buf.WriteString("data")
	binary.Write(buf, binary.LittleEndian, uint32(dataLen))
	for _, frame := range samples {
		for _, s := range frame {
			switch bps {
			case 16:
				binary.Write(buf, binary.LittleEndian, int16(s))
			case 24:
				b := []byte{byte(s), byte(s >> 8), byte(s >> 16)}
				buf.Write(b)
			}
		}
	}
	return buf.Bytes()
}

func TestWAVReader_16bitMono(t *testing.T) {
	frames := [][]int32{{100}, {-200}, {32000}, {-32000}}
	wav := buildWAV(t, 48000, 1, 16, frames)
	r, err := pcm.NewWAVReader(bytes.NewReader(wav))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if r.Format().SampleRate != 48000 || r.Format().Channels != 1 || r.Format().BitsPerSample != 16 {
		t.Fatalf("format = %+v", r.Format())
	}
	dst := make([]int16, 4)
	n, err := r.ReadSamples(dst)
	if err != nil && err != io.EOF {
		t.Fatalf("read: %v", err)
	}
	if n != 4 {
		t.Fatalf("n=%d", n)
	}
	want := []int16{100, -200, 32000, -32000}
	for i := range want {
		if dst[i] != want[i] {
			t.Errorf("dst[%d]=%d want %d", i, dst[i], want[i])
		}
	}
}

func TestWAVReader_24bitStereo(t *testing.T) {
	// 24-bit values that, after >>8, give recognizable int16s.
	frames := [][]int32{
		{0x010000, -0x010000}, // shift right 8 = 0x100, -0x100
		{0x7FFF00, -0x7FFF00}, // ≈ +/-32767
	}
	wav := buildWAV(t, 44100, 2, 24, frames)
	r, err := pcm.NewWAVReader(bytes.NewReader(wav))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if r.Format().BitsPerSample != 24 {
		t.Fatalf("bps=%d", r.Format().BitsPerSample)
	}
	dst := make([]int16, 4)
	n, _ := r.ReadSamples(dst)
	if n != 4 {
		t.Fatalf("n=%d", n)
	}
	if dst[0] != 0x100 || dst[1] != -0x100 {
		t.Errorf("first frame = %d,%d", dst[0], dst[1])
	}
	if dst[2] < 32700 || dst[3] > -32700 {
		t.Errorf("second frame magnitude wrong: %d,%d", dst[2], dst[3])
	}
}

func TestWAVReader_RejectsFloat(t *testing.T) {
	buf := &bytes.Buffer{}
	buf.WriteString("RIFF")
	binary.Write(buf, binary.LittleEndian, uint32(36))
	buf.WriteString("WAVE")
	buf.WriteString("fmt ")
	binary.Write(buf, binary.LittleEndian, uint32(16))
	binary.Write(buf, binary.LittleEndian, uint16(3)) // IEEE float
	binary.Write(buf, binary.LittleEndian, uint16(1))
	binary.Write(buf, binary.LittleEndian, uint32(48000))
	binary.Write(buf, binary.LittleEndian, uint32(48000*4))
	binary.Write(buf, binary.LittleEndian, uint16(4))
	binary.Write(buf, binary.LittleEndian, uint16(32))
	buf.WriteString("data")
	binary.Write(buf, binary.LittleEndian, uint32(0))
	if _, err := pcm.NewWAVReader(bytes.NewReader(buf.Bytes())); err == nil {
		t.Fatal("expected float rejection")
	}
}

func TestResampler_PassthroughIdentity(t *testing.T) {
	// Same-rate resampling is degenerate: up=down=1, taps reduce to identity.
	r := pcm.NewResampler(48000, 48000, 1)
	in := []int16{100, 200, 300, 400, 500, 600, 700, 800}
	out := r.Process(in, nil)
	if len(out) != len(in) {
		t.Fatalf("len out=%d in=%d", len(out), len(in))
	}
	// Single-tap identity should produce exact passthrough.
	for i, v := range in {
		if out[i] != v {
			t.Errorf("out[%d]=%d in=%d", i, out[i], v)
		}
	}
}

func TestResampler_44100to48000_SineEnergy(t *testing.T) {
	// Generate a 1 kHz sine at 44.1 kHz, resample to 48 kHz, check that
	// the output has comparable RMS energy and approximately the right
	// length. We don't check sample-by-sample; we just confirm the
	// resampler isn't producing garbage.
	const inRate = 44100
	const outRate = 48000
	const hz = 1000.0
	const dur = 1.0
	n := int(inRate * dur)
	in := make([]int16, n)
	for i := range in {
		in[i] = int16(20000 * math.Sin(2*math.Pi*hz*float64(i)/inRate))
	}
	r := pcm.NewResampler(inRate, outRate, 1)
	out := r.Process(in, nil)

	expected := int(float64(n) * outRate / inRate)
	if math.Abs(float64(len(out)-expected)) > float64(expected)*0.01 {
		t.Errorf("output length %d, expected ~%d", len(out), expected)
	}

	// Energy ratio should be close to 1 (within 10%).
	var inE, outE float64
	for _, v := range in {
		inE += float64(v) * float64(v)
	}
	for _, v := range out {
		outE += float64(v) * float64(v)
	}
	inRMS := math.Sqrt(inE / float64(len(in)))
	outRMS := math.Sqrt(outE / float64(len(out)))
	if math.Abs(outRMS-inRMS)/inRMS > 0.10 {
		t.Errorf("RMS drift: in=%.0f out=%.0f", inRMS, outRMS)
	}
}

func TestWriteStreamingWAVHeader_RoundTrip(t *testing.T) {
	buf := &bytes.Buffer{}
	if err := pcm.WriteStreamingWAVHeader(buf, pcm.Format{SampleRate: 48000, Channels: 1, BitsPerSample: 16}); err != nil {
		t.Fatalf("write header: %v", err)
	}
	// Sentinel data size means the reader should still parse the format.
	if err := pcm.WriteInt16LE(buf, []int16{1, 2, 3, 4}); err != nil {
		t.Fatalf("write samples: %v", err)
	}
	r, err := pcm.NewWAVReader(buf)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	got := make([]int16, 4)
	n, _ := r.ReadSamples(got)
	if n != 4 || got[0] != 1 || got[3] != 4 {
		t.Fatalf("samples = %v (n=%d)", got, n)
	}
}
