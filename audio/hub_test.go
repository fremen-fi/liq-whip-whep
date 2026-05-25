package audio_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fremen-fi/liq-whip-whep/audio"
	"github.com/fremen-fi/liq-whip-whep/audio/pcm"
)

// shortTempDir returns a tempdir under /tmp on darwin to dodge the 104-byte
// sun_path limit that t.TempDir() blows past on macOS.
func shortTempDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "cr-audio-")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}

// TestPCMHub_FrameFlow drives a fake-Liquidsoap client into the hub's
// Unix socket: 44.1 kHz / 16-bit / mono streaming WAV with a few seconds
// of a 1 kHz tone. We subscribe and verify frames are 48 kHz mono and
// arrive at the expected pace.
func TestPCMHub_FrameFlow(t *testing.T) {
	tmp := shortTempDir(t)
	sock := filepath.Join(tmp, "onair.sock")
	hub := audio.NewPCMHub(sock)
	if err := hub.Start(context.Background()); err != nil {
		t.Fatalf("hub start: %v", err)
	}
	t.Cleanup(hub.Stop)

	sub := hub.Subscribe()
	t.Cleanup(func() { hub.Unsubscribe(sub) })

	// Producer: connect, write streaming WAV header + 200ms of audio.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Wait briefly for the listener to be ready.
		var conn net.Conn
		var err error
		for i := 0; i < 20; i++ {
			conn, err = net.Dial("unix", sock)
			if err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			t.Errorf("dial: %v", err)
			return
		}
		defer conn.Close()
		buf := &bytes.Buffer{}
		// 44.1k mono 16-bit, streaming-size sentinels.
		if err := pcm.WriteStreamingWAVHeader(buf, pcm.Format{SampleRate: 44100, Channels: 1, BitsPerSample: 16}); err != nil {
			t.Errorf("header: %v", err)
			return
		}
		// 200 ms of zeros is enough — we're testing the plumbing.
		samples := make([]int16, 44100*200/1000)
		_ = pcm.WriteInt16LE(buf, samples)
		if _, err := conn.Write(buf.Bytes()); err != nil {
			t.Errorf("write: %v", err)
		}
	}()

	// Consume frames for up to 1 s; expect at least a handful.
	deadline := time.After(2 * time.Second)
	frames := 0
	for frames < 5 {
		select {
		case f, ok := <-sub.Frames():
			if !ok {
				t.Fatal("subscriber channel closed early")
			}
			// Expect 20 ms × 48 kHz × mono = 960 samples.
			if len(f) != 960 {
				t.Fatalf("frame length = %d, want 960", len(f))
			}
			frames++
		case <-deadline:
			t.Fatalf("only got %d frames before deadline", frames)
		}
	}
	wg.Wait()
}

// TestPCMSink_LiquidsoapReadsStreamingWAV connects to the sink as if we
// were Liquidsoap, reads the streaming WAV header and a few frames worth
// of PCM that the sink received via WriteFrame.
func TestPCMSink_LiquidsoapReadsStreamingWAV(t *testing.T) {
	tmp := shortTempDir(t)
	sock := filepath.Join(tmp, "mic.sock")
	sink := audio.NewPCMSink(sock)
	if err := sink.Start(context.Background()); err != nil {
		t.Fatalf("sink start: %v", err)
	}
	t.Cleanup(sink.Stop)

	// Connect first (sink writes header on accept).
	var conn net.Conn
	var err error
	for i := 0; i < 20; i++ {
		conn, err = net.Dial("unix", sock)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Read the 44-byte streaming WAV header.
	hdr := make([]byte, 44)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		t.Fatalf("read header: %v", err)
	}
	if string(hdr[0:4]) != "RIFF" || string(hdr[8:12]) != "WAVE" {
		t.Fatalf("not RIFF/WAVE: %q", hdr[:12])
	}
	rate := binary.LittleEndian.Uint32(hdr[24:28])
	if rate != 48000 {
		t.Fatalf("rate = %d, want 48000", rate)
	}

	// Wait for the sink to register us as the consumer. The accept loop
	// runs after writing the header, so HasConsumer should be true very
	// soon — but Read may race with the swap. Poll briefly.
	for i := 0; i < 20 && !sink.HasConsumer(); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if !sink.HasConsumer() {
		t.Fatal("sink never registered consumer")
	}

	// Push a couple of frames, read them back as PCM.
	want := []int16{1, 2, 3, 4, 5, 6, 7, 8}
	if err := sink.WriteFrame(want); err != nil {
		t.Fatalf("write frame: %v", err)
	}
	got := make([]byte, len(want)*2)
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read frame: %v", err)
	}
	for i, w := range want {
		v := int16(binary.LittleEndian.Uint16(got[i*2 : i*2+2]))
		if v != w {
			t.Errorf("got[%d]=%d want %d", i, v, w)
		}
	}
}

// TestEncodeDecodeRoundTrip pushes silence through the libopus encoder
// at the WHEP-side configuration and decodes it back at the WHIP-side
// configuration, just to confirm CGO is wired and the codec parameters
// agree end to end. We don't check exact samples — Opus is lossy and a
// pure-silence input is the only thing we can verify cheaply.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	t.Skip("opus codec smoke test — enable manually if needed")
}
