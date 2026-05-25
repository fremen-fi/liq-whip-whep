package audio

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fremen-fi/liq-whip-whep/audio/pcm"
)

// uplinkRate / uplinkChannels define the WAV format we emit to Liquidsoap
// on the mic socket. 48 kHz mono 16-bit is what libopus decodes to
// natively, so we avoid an extra resample on the way out.
const (
	uplinkRate     = 48000
	uplinkChannels = 1
	uplinkBits     = 16
)

// PCMSink accepts decoded mic PCM from a WHIP session and forwards it as
// a streaming WAV to a single Liquidsoap consumer connected over a Unix
// socket.
//
// Concurrency model: at most one upstream Liquidsoap reader, at most one
// active WHIP session writing into the sink. A new WHIP session
// preempts the previous one (host-mic semantics — only one mic on air).
// If no Liquidsoap reader is connected, written frames are dropped.
type PCMSink struct {
	socketPath string

	mu       sync.Mutex
	consumer net.Conn // current Liquidsoap reader; nil if none

	stop chan struct{}
}

func NewPCMSink(socketPath string) *PCMSink {
	return &PCMSink{
		socketPath: socketPath,
		stop:       make(chan struct{}),
	}
}

// Start binds the Unix listener and accepts one Liquidsoap connection at
// a time. Returns once the listener is up.
func (s *PCMSink) Start(ctx context.Context) error {
	if s.socketPath == "" {
		return errors.New("pcm sink: socket path required")
	}
	if dir := filepath.Dir(s.socketPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		_ = os.Chmod(dir, 0o755)
	}
	_ = os.Remove(s.socketPath)
	ln, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return err
	}
	// World-accessible socket so the Liquidsoap user (different uid) can
	// connect without any group membership. The parent dir's perms still
	// gate who can reach the path.
	_ = os.Chmod(s.socketPath, 0o666)
	go s.acceptLoop(ctx, ln)
	return nil
}

func (s *PCMSink) Stop() {
	close(s.stop)
	s.mu.Lock()
	if s.consumer != nil {
		_ = s.consumer.Close()
		s.consumer = nil
	}
	s.mu.Unlock()
	_ = os.Remove(s.socketPath)
}

func (s *PCMSink) acceptLoop(ctx context.Context, ln net.Listener) {
	defer ln.Close()
	go func() {
		select {
		case <-ctx.Done():
		case <-s.stop:
		}
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-s.stop:
				return
			default:
			}
			slog.Warn("pcm sink: accept", "err", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		slog.Info("pcm sink: liquidsoap connected", "socket", s.socketPath)
		// Write the streaming WAV header up front. Once written we leave
		// the connection open and feed it frames as they arrive.
		if err := pcm.WriteStreamingWAVHeader(conn, pcm.Format{
			SampleRate:    uplinkRate,
			Channels:      uplinkChannels,
			BitsPerSample: uplinkBits,
		}); err != nil {
			slog.Warn("pcm sink: write header", "err", err)
			_ = conn.Close()
			continue
		}
		s.swapConsumer(conn)

		// Block until this consumer disconnects. We don't read from it,
		// but Read() returns when the peer closes; we use that as our
		// disconnect signal.
		one := make([]byte, 1)
		_, _ = conn.Read(one)
		s.swapConsumer(nil)
		slog.Info("pcm sink: liquidsoap disconnected")
	}
}

func (s *PCMSink) swapConsumer(c net.Conn) {
	s.mu.Lock()
	old := s.consumer
	s.consumer = c
	s.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

// WriteFrame writes one frame of int16 mono samples at uplinkRate to the
// current consumer. Returns nil if no consumer is connected (drop) — we
// don't want a missing Liquidsoap to fail mic ingest, just to lose audio
// until it comes back.
func (s *PCMSink) WriteFrame(samples []int16) error {
	s.mu.Lock()
	c := s.consumer
	s.mu.Unlock()
	if c == nil {
		return nil
	}
	if err := pcm.WriteInt16LE(c, samples); err != nil {
		// Tear down so the next Liquidsoap connection gets a fresh
		// header rather than a half-written frame.
		s.swapConsumer(nil)
		if errors.Is(err, io.ErrClosedPipe) {
			return nil
		}
		return err
	}
	return nil
}

// HasConsumer reports whether a Liquidsoap reader is currently connected.
// Used by the WHIP loop to decide whether to bother decoding.
func (s *PCMSink) HasConsumer() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consumer != nil
}
