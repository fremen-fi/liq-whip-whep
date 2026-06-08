package audio

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fremen-fi/liq-whip-whep/audio/pcm"
)

// uplinkRate / uplinkChannels define the raw PCM format we emit to
// Liquidsoap on the mic socket: interleaved s16le. Consumed by
// input.external.rawaudio(channels=2, samplerate=48000, ...) — no WAV
// header, no parsing surface. The mic is fundamentally mono (single
// source) but we interleave L=R so Liquidsoap's mic source is already
// 2-ch and no stereo() wrap is needed downstream.
const (
	uplinkRate     = 48000
	uplinkChannels = 2
	uplinkBits     = 16
)

// Playout pacing. WHIP delivers Opus frames at the network's (bursty,
// jittery) arrival cadence, but Liquidsoap consumes the mic socket at a
// rock-steady 48 kHz. If we wrote straight through, the reader's input
// buffer would underrun between bursts — emitting track marks, flipping to
// blank, replaying buffered chunks ("frames from here and there") and
// leaking sources. So we decouple: WHIP fills a ring, and a wall-clock
// ticker drains exactly one frame per interval, padding with silence on
// underrun. The result is a continuous real-time stream regardless of how
// lumpy the network delivery is.
const (
	paceMillis         = 20
	frameSamples       = uplinkRate * paceMillis / 1000 // 960 @ 48k/20ms
	prebufferSamples   = frameSamples * 3               // ~60 ms primed before real audio
	maxBufferedSamples = uplinkRate / 2                 // ~500 ms latency cap
)

var paceInterval = paceMillis * time.Millisecond

// PCMSink accepts decoded mic PCM from a WHIP session and forwards it as
// raw interleaved s16le to a single Liquidsoap consumer connected over a
// Unix socket, paced at real-time.
//
// Concurrency model: at most one upstream Liquidsoap reader, at most one
// active WHIP session writing into the sink. A new WHIP session preempts
// the previous one (host-mic semantics — only one mic on air). Written
// frames accumulate in a ring; the pacer drains it at 48 kHz. With no
// Liquidsoap reader the ring is discarded so a reconnect doesn't replay a
// stale backlog.
type PCMSink struct {
	socketPath string

	mu       sync.Mutex
	consumer net.Conn // current Liquidsoap reader; nil if none
	ring     []int16  // pending mic samples awaiting paced emission
	primed   bool     // true once the ring first reached prebufferSamples

	// TapPath, if set before Start, makes the pacer also write the exact
	// stream it emits to Liquidsoap into this WAV file — a non-invasive
	// debug capture (the live consumer is unaffected). Set via env.
	TapPath string
	tap     *os.File

	stop chan struct{}
}

func NewPCMSink(socketPath string) *PCMSink {
	return &PCMSink{
		socketPath: socketPath,
		stop:       make(chan struct{}),
		ring:       make([]int16, 0, maxBufferedSamples),
	}
}

// Start binds the Unix listener, accepts one Liquidsoap connection at a
// time, and runs the real-time pacer. Returns once the listener is up.
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
	go s.paceLoop(ctx)
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
		// Fresh connection starts from a clean, re-primed buffer.
		s.mu.Lock()
		s.ring = s.ring[:0]
		s.primed = false
		s.mu.Unlock()
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

// paceLoop emits one frame every paceInterval to the connected consumer,
// draining the ring and padding with silence when it runs dry. This is the
// single place that writes audio to the socket, so the stream Liquidsoap
// sees is continuous at exactly 48 kHz on average.
func (s *PCMSink) paceLoop(ctx context.Context) {
	ticker := time.NewTicker(paceInterval)
	defer ticker.Stop()
	// Ring holds mono; we interleave L=R into `frame` on each tick.
	mono := make([]int16, frameSamples)
	frame := make([]int16, frameSamples*uplinkChannels)

	// Optional debug tap: write the exact emitted stream to a WAV file.
	if s.TapPath != "" {
		if f, err := os.Create(s.TapPath); err == nil {
			if err := pcm.WriteStreamingWAVHeader(f, pcm.Format{
				SampleRate: uplinkRate, Channels: uplinkChannels, BitsPerSample: uplinkBits,
			}); err == nil {
				s.tap = f
				slog.Info("pcm sink: debug tap open", "path", s.TapPath)
			} else {
				_ = f.Close()
			}
		} else {
			slog.Warn("pcm sink: tap open", "err", err)
		}
	}
	defer func() {
		if s.tap != nil {
			_ = s.tap.Close()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case <-ticker.C:
		}

		s.mu.Lock()
		c := s.consumer
		if c == nil {
			// No reader: drop the backlog so a reconnect starts clean
			// rather than replaying stale audio.
			s.ring = s.ring[:0]
			s.primed = false
			s.mu.Unlock()
			continue
		}
		if !s.primed {
			if len(s.ring) < prebufferSamples {
				// Still filling the playout buffer — emit silence to hold
				// the stream at real-time until we have headroom.
				for i := range frame {
					frame[i] = 0
				}
				s.mu.Unlock()
				if err := pcm.WriteInt16LE(c, frame); err != nil {
					s.swapConsumer(nil)
				}
				if s.tap != nil {
					_ = pcm.WriteInt16LE(s.tap, frame)
				}
				continue
			}
			s.primed = true
		}
		n := copy(mono, s.ring)
		// Compact the remainder to the front (overlap-safe) so the backing
		// array stays bounded instead of growing via head-advance.
		rem := copy(s.ring, s.ring[n:])
		s.ring = s.ring[:rem]
		for i := n; i < frameSamples; i++ {
			mono[i] = 0 // silence pad on underrun
		}
		s.mu.Unlock()
		// Interleave mono → L=R stereo.
		for i := 0; i < frameSamples; i++ {
			frame[2*i] = mono[i]
			frame[2*i+1] = mono[i]
		}

		if err := pcm.WriteInt16LE(c, frame); err != nil {
			s.swapConsumer(nil)
		}
		if s.tap != nil {
			_ = pcm.WriteInt16LE(s.tap, frame)
		}
	}
}

// WriteFrame enqueues one frame of int16 mono samples at uplinkRate for
// paced emission. The pacer interleaves L=R into the stereo socket stream.
// Never blocks on the socket and never writes directly; the pacer owns the
// socket. If the ring exceeds the latency cap (a catch-up burst after
// jitter) the oldest samples are dropped.
func (s *PCMSink) WriteFrame(samples []int16) error {
	s.mu.Lock()
	s.ring = append(s.ring, samples...)
	if len(s.ring) > maxBufferedSamples {
		drop := len(s.ring) - maxBufferedSamples
		rem := copy(s.ring, s.ring[drop:])
		s.ring = s.ring[:rem]
	}
	s.mu.Unlock()
	return nil
}

// HasConsumer reports whether a Liquidsoap reader is currently connected.
// Used by the WHIP loop to decide whether to bother decoding.
func (s *PCMSink) HasConsumer() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.consumer != nil
}
