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

// downlinkRate is the rate the WHEP encoder runs at; libopus accepts 48 kHz
// directly and that's what WebRTC uses on the wire.
const downlinkRate = 48000

// frameMS is the Opus packet duration we emit. 20 ms is the sweet spot
// for low-latency speech/music — short enough to keep mouth-to-ear delay
// down, long enough that overhead from RTP headers stays sane.
const frameMS = 20

// PCMHub reads a streaming WAV input (typically Liquidsoap's on-air bus
// via Unix socket) and fans out 20 ms PCM frames at 48 kHz to any number
// of WHEP subscribers. One Liquidsoap source feeds N browsers without
// re-encoding the stream — each peer gets its own libopus encoder and
// RTP stream, but they all chew the same PCM frames.
//
// The hub auto-reconnects: if the upstream WAV stream ends or the socket
// peer disconnects, it accepts the next one. While disconnected, sub
// channels stop receiving and WHEP loops emit nothing (the browser gets
// silence implicitly via Opus PLC).
type PCMHub struct {
	socketPath string

	mu       sync.RWMutex
	channels int // 1 or 2; 0 until first stream connects
	subs     map[*pcmSub]struct{}

	stop chan struct{}
}

// pcmSub is one downstream listener — typically a WHEP session.
type pcmSub struct {
	// ch carries 20ms frames as []int16, interleaved if stereo. Buffered
	// so a momentarily slow encoder doesn't stall the hub; on overflow
	// we drop the oldest so we stay roughly real-time.
	ch chan []int16
}

// NewPCMHub creates a hub that listens on the given Unix socket path. The
// socket is created on Start and removed on Stop. Liquidsoap connects to
// it as a client (e.g. via `output.external(... | socat - UNIX-CONNECT:...)`)
// and writes streaming WAV.
func NewPCMHub(socketPath string) *PCMHub {
	return &PCMHub{
		socketPath: socketPath,
		subs:       make(map[*pcmSub]struct{}),
		stop:       make(chan struct{}),
	}
}

// Channels reports the channel count of the current stream (1 or 2). It
// is 0 before the first WAV header is parsed; subscribers should treat 0
// as "no upstream yet" and wait.
func (h *PCMHub) Channels() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.channels
}

// Subscribe registers a listener. The returned channel delivers []int16
// frames of (downlinkRate * frameMS / 1000) samples per channel,
// interleaved. Always call Unsubscribe when done.
func (h *PCMHub) Subscribe() *pcmSub {
	// 4 × 20 ms = 80 ms; matches one Liquidsoap default-frame burst. Smaller
	// drops frames per burst; larger adds latency without much benefit.
	s := &pcmSub{ch: make(chan []int16, 4)}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

// Unsubscribe removes the listener and closes its channel.
func (h *PCMHub) Unsubscribe(s *pcmSub) {
	h.mu.Lock()
	if _, ok := h.subs[s]; ok {
		delete(h.subs, s)
		close(s.ch)
	}
	h.mu.Unlock()
}

// Frames returns the listener's frame channel.
func (s *pcmSub) Frames() <-chan []int16 { return s.ch }

// Start begins accepting connections on the Unix socket and reading WAV.
// It returns once the listener is bound; the read loop runs in the
// background until ctx is cancelled or Stop is called.
func (h *PCMHub) Start(ctx context.Context) error {
	if h.socketPath == "" {
		return errors.New("pcm hub: socket path required")
	}
	if dir := filepath.Dir(h.socketPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		_ = os.Chmod(dir, 0o755)
	}
	// Best-effort cleanup of a stale socket from a previous crash.
	_ = os.Remove(h.socketPath)
	ln, err := net.Listen("unix", h.socketPath)
	if err != nil {
		return err
	}
	// World-accessible socket so the Liquidsoap user (different uid) can
	// connect without any group membership.
	_ = os.Chmod(h.socketPath, 0o666)
	go h.acceptLoop(ctx, ln)
	return nil
}

// Stop terminates the accept loop and removes the socket.
func (h *PCMHub) Stop() {
	close(h.stop)
	_ = os.Remove(h.socketPath)
}

func (h *PCMHub) acceptLoop(ctx context.Context, ln net.Listener) {
	defer ln.Close()
	go func() {
		select {
		case <-ctx.Done():
		case <-h.stop:
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
			case <-h.stop:
				return
			default:
			}
			slog.Warn("pcm hub: accept", "err", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		slog.Info("pcm hub: upstream connected", "socket", h.socketPath)
		h.handleStream(conn)
		slog.Info("pcm hub: upstream disconnected", "socket", h.socketPath)
		h.mu.Lock()
		h.channels = 0
		h.mu.Unlock()
	}
}

// handleStream reads one WAV stream end-to-end, resampling as needed and
// broadcasting frames. Returns when the stream ends.
func (h *PCMHub) handleStream(conn io.ReadCloser) {
	defer conn.Close()
	wr, err := pcm.NewWAVReader(conn)
	if err != nil {
		slog.Warn("pcm hub: wav header", "err", err)
		return
	}
	in := wr.Format()
	slog.Info("pcm hub: format", "rate", in.SampleRate, "channels", in.Channels, "bps", in.BitsPerSample)
	h.mu.Lock()
	h.channels = in.Channels
	h.mu.Unlock()

	rs := pcm.NewResampler(in.SampleRate, downlinkRate, in.Channels)
	frameSamples := downlinkRate * frameMS / 1000 * in.Channels // per frame, interleaved

	// Read in chunks of 100 ms worth at the input rate; small enough to
	// stay live, big enough to amortize syscalls and resampler overhead.
	chunkFrames := in.SampleRate / 10
	rawBuf := make([]int16, chunkFrames*in.Channels)
	pending := make([]int16, 0, frameSamples*4)

	// Sample L/R divergence over the first second so a glance at the log
	// shows "real stereo" vs "L=R upstream of bridge".
	const diagFrames = 50 // 50 × 20 ms = 1 s
	var (
		diagFramesLeft = diagFrames
		diagPairs      int64
		diagEqual      int64
		diagMaxDiff    int32
	)

	for {
		n, err := wr.ReadSamples(rawBuf)
		if n > 0 {
			pending = rs.Process(rawBuf[:n], pending)
			for len(pending) >= frameSamples {
				// Copy out the frame so subscribers don't share backing memory.
				frame := make([]int16, frameSamples)
				copy(frame, pending[:frameSamples])
				pending = pending[frameSamples:]
				if diagFramesLeft > 0 && in.Channels == 2 {
					for i := 0; i+1 < len(frame); i += 2 {
						l, r := int32(frame[i]), int32(frame[i+1])
						d := l - r
						if d < 0 {
							d = -d
						}
						if d > diagMaxDiff {
							diagMaxDiff = d
						}
						if d == 0 {
							diagEqual++
						}
						diagPairs++
					}
					diagFramesLeft--
					if diagFramesLeft == 0 {
						slog.Info("pcm hub: stereo divergence (1 s window)",
							"pairs", diagPairs,
							"l_eq_r", diagEqual,
							"max_abs_diff", diagMaxDiff,
						)
					}
				}
				h.broadcast(frame)
			}
		}
		if err != nil {
			if err != io.EOF {
				slog.Debug("pcm hub: read", "err", err)
			}
			return
		}
	}
}

func (h *PCMHub) broadcast(frame []int16) {
	h.mu.RLock()
	subs := make([]*pcmSub, 0, len(h.subs))
	for s := range h.subs {
		subs = append(subs, s)
	}
	h.mu.RUnlock()
	for _, s := range subs {
		select {
		case s.ch <- frame:
		default:
			// Slow consumer: drop the oldest frame and try again so we
			// don't accumulate latency. One drop ≈ 20 ms glitch.
			select {
			case <-s.ch:
			default:
			}
			select {
			case s.ch <- frame:
			default:
			}
		}
	}
}
