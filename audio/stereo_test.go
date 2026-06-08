package audio_test

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/fremen-fi/liq-whip-whep/audio"
	"github.com/fremen-fi/liq-whip-whep/audio/pcm"
)

// TestPCMHub_StereoIsPreserved feeds the hub stereo WAV with deliberately
// divergent L and R channels (L = +amplitude, R = -amplitude). After the
// hub parses the header, resamples, and slices into 20 ms frames, L and R
// must NOT be equal. This is the cheapest way to catch any place along
// the bridge path that collapses stereo to L=R.
func TestPCMHub_StereoIsPreserved(t *testing.T) {
	tmp := shortTempDir(t)
	sock := filepath.Join(tmp, "onair.sock")
	hub := audio.NewPCMHub(sock)
	if err := hub.Start(context.Background()); err != nil {
		t.Fatalf("hub start: %v", err)
	}
	t.Cleanup(hub.Stop)

	sub := hub.Subscribe()
	t.Cleanup(func() { hub.Unsubscribe(sub) })

	// Producer: stereo at 48 kHz so the resampler is a no-op and we can
	// reason about what comes out the other side. L = +10000, R = -10000.
	go func() {
		var conn net.Conn
		var err error
		for range 20 {
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
		if err := pcm.WriteStreamingWAVHeader(buf, pcm.Format{
			SampleRate: 48000, Channels: 2, BitsPerSample: 16,
		}); err != nil {
			t.Errorf("header: %v", err)
			return
		}
		// 200 ms = 9600 frames × 2 channels.
		samples := make([]int16, 9600*2)
		for i := range 9600 {
			samples[2*i] = 10000    // L
			samples[2*i+1] = -10000 // R
		}
		_ = pcm.WriteInt16LE(buf, samples)
		_, _ = conn.Write(buf.Bytes())
		// Hold the connection open so the hub doesn't tear down mid-test.
		time.Sleep(500 * time.Millisecond)
	}()

	deadline := time.After(2 * time.Second)
	got := 0
	for got < 5 {
		select {
		case f, ok := <-sub.Frames():
			if !ok {
				t.Fatal("subscriber channel closed early")
			}
			// 20 ms × 48 kHz × 2 ch = 1920 interleaved samples.
			if len(f) != 1920 {
				t.Fatalf("frame length = %d, want 1920 (stereo)", len(f))
			}
			// Every pair must show L != R if stereo survived.
			equal := 0
			for i := 0; i+1 < len(f); i += 2 {
				if f[i] == f[i+1] {
					equal++
				}
			}
			if equal == len(f)/2 {
				t.Fatalf("stereo collapsed to L=R: every pair is equal in frame %d", got)
			}
			got++
		case <-deadline:
			t.Fatalf("only %d frames before deadline", got)
		}
	}
}
