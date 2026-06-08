package pcm

import (
	"encoding/binary"
	"fmt"
	"io"
)

// WriteStreamingWAVHeader emits a RIFF/WAVE/fmt /data preamble whose size
// fields are the streaming-friendly maximum (0xFFFFFFFF). Liquidsoap's
// input.external can read this indefinitely; downstream tools that insist
// on truthful sizes won't be happy, but Liquidsoap is what we're feeding.
//
// The data layout that follows must be interleaved little-endian PCM at
// the format requested.
func WriteStreamingWAVHeader(w io.Writer, fmt_ Format) error {
	if fmt_.BitsPerSample%8 != 0 || fmt_.Channels < 1 {
		return invalidFormat(fmt_)
	}
	bytesPerSample := fmt_.BitsPerSample / 8
	blockAlign := uint16(fmt_.Channels * bytesPerSample)
	byteRate := uint32(fmt_.SampleRate * fmt_.Channels * bytesPerSample)

	var hdr [44]byte
	copy(hdr[0:4], "RIFF")
	binary.LittleEndian.PutUint32(hdr[4:8], 0xFFFFFFFF)
	copy(hdr[8:12], "WAVE")
	copy(hdr[12:16], "fmt ")
	binary.LittleEndian.PutUint32(hdr[16:20], 16)
	binary.LittleEndian.PutUint16(hdr[20:22], 1) // PCM
	binary.LittleEndian.PutUint16(hdr[22:24], uint16(fmt_.Channels))
	binary.LittleEndian.PutUint32(hdr[24:28], uint32(fmt_.SampleRate))
	binary.LittleEndian.PutUint32(hdr[28:32], byteRate)
	binary.LittleEndian.PutUint16(hdr[32:34], blockAlign)
	binary.LittleEndian.PutUint16(hdr[34:36], uint16(fmt_.BitsPerSample))
	copy(hdr[36:40], "data")
	binary.LittleEndian.PutUint32(hdr[40:44], 0xFFFFFFFF)
	_, err := w.Write(hdr[:])
	return err
}

// WriteInt16LE writes interleaved int16 samples in little-endian order.
// One call per 20 ms frame is fine — this is just a Write — but callers
// who care about syscall count should buffer.
func WriteInt16LE(w io.Writer, samples []int16) error {
	buf := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(buf[i*2:i*2+2], uint16(s))
	}
	_, err := w.Write(buf)
	return err
}

func invalidFormat(f Format) error {
	return fmt.Errorf("pcm: invalid format rate=%d ch=%d bps=%d", f.SampleRate, f.Channels, f.BitsPerSample)
}
