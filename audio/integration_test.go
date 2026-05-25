package audio_test

import (
	"bytes"
	"context"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	opus "gopkg.in/hraban/opus.v2"

	"github.com/fremen-fi/liq-whip-whep/audio"
	"github.com/fremen-fi/liq-whip-whep/audio/pcm"
)

// TestEndToEnd_WHEP_StereoPreserved is the full bridge contract:
//
//   - Liquidsoap-equivalent pumps stereo divergent audio (L=tone, R=silence)
//     into the on-air socket as streaming WAV.
//   - A WebRTC client (Pion, standing in for the browser) opens a WHEP
//     session against the bridge, negotiates Opus, and receives RTP.
//   - We Opus-decode the received packets and confirm L != R after the
//     full bridge → encoder → SDP → RTP → decoder path.
//
// This is the test that catches the user-reported "Liquidsoap sends
// stereo but it becomes L=R" bug. It exercises the same code path
// browsers see.
func TestEndToEnd_WHEP_StereoPreserved(t *testing.T) {
	tmp := shortTempDir(t)
	hub := audio.NewPCMHub(filepath.Join(tmp, "onair.sock"))
	if err := hub.Start(context.Background()); err != nil {
		t.Fatalf("hub start: %v", err)
	}
	t.Cleanup(hub.Stop)
	sink := audio.NewPCMSink(filepath.Join(tmp, "mic.sock"))
	if err := sink.Start(context.Background()); err != nil {
		t.Fatalf("sink start: %v", err)
	}
	t.Cleanup(sink.Stop)

	srv := audio.NewServer("/audio")
	srv.Hub = hub
	srv.Sink = sink
	mux := http.NewServeMux()
	mux.Handle("/audio/", srv.Handler())
	httpSrv := httptest.NewServer(mux)
	t.Cleanup(httpSrv.Close)

	// Producer: pretend to be Liquidsoap. Open the on-air socket, write
	// streaming WAV, push stereo divergent audio for the duration of the
	// test.
	producerStop := make(chan struct{})
	var producerWG sync.WaitGroup
	producerWG.Add(1)
	go func() {
		defer producerWG.Done()
		var conn net.Conn
		var err error
		for i := 0; i < 50; i++ {
			conn, err = net.Dial("unix", filepath.Join(tmp, "onair.sock"))
			if err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			t.Errorf("producer dial: %v", err)
			return
		}
		defer conn.Close()
		if err := pcm.WriteStreamingWAVHeader(conn, pcm.Format{
			SampleRate: 48000, Channels: 2, BitsPerSample: 16,
		}); err != nil {
			t.Errorf("producer header: %v", err)
			return
		}
		// Push 20 ms blocks (1920 samples × 2 ch) at real-time.
		const block = 960
		buf := make([]int16, block*2)
		idx := 0
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-producerStop:
				return
			case <-ticker.C:
				for i := 0; i < block; i++ {
					sec := float64(idx+i) / 48000.0
					// L: 1 kHz sine, half-scale.
					buf[2*i] = int16(0.5 * 32767 * math.Sin(2*math.Pi*1000*sec))
					// R: silence.
					buf[2*i+1] = 0
				}
				idx += block
				if err := pcm.WriteInt16LE(conn, buf); err != nil {
					return
				}
			}
		}
	}()
	t.Cleanup(func() {
		close(producerStop)
		producerWG.Wait()
	})

	// Wait for the hub to be reading the stream (channels populated).
	for i := 0; i < 50 && hub.Channels() == 0; i++ {
		time.Sleep(20 * time.Millisecond)
	}
	if hub.Channels() != 2 {
		t.Fatalf("hub channels = %d, want 2", hub.Channels())
	}

	// Pion WebRTC client (browser stand-in).
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("client pc: %v", err)
	}
	defer pc.Close()

	// We are the receiver in WHEP.
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	}); err != nil {
		t.Fatalf("add transceiver: %v", err)
	}

	rtpCh := make(chan []byte, 1024)
	negotiatedChannels := make(chan int, 1)
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		t.Logf("client OnTrack: codec=%s channels=%d clock=%d fmtp=%q",
			track.Codec().MimeType, track.Codec().Channels,
			track.Codec().ClockRate, track.Codec().SDPFmtpLine)
		select {
		case negotiatedChannels <- int(track.Codec().Channels):
		default:
		}
		for {
			pkt, _, err := track.ReadRTP()
			if err != nil {
				return
			}
			if len(pkt.Payload) == 0 {
				continue
			}
			cp := append([]byte(nil), pkt.Payload...)
			select {
			case rtpCh <- cp:
			default:
			}
		}
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create offer: %v", err)
	}
	gatherDone := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("set local: %v", err)
	}
	select {
	case <-gatherDone:
	case <-time.After(5 * time.Second):
		t.Fatal("client gathering timeout")
	}

	req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/audio/whep",
		bytes.NewReader([]byte(pc.LocalDescription().SDP)))
	req.Header.Set("Content-Type", "application/sdp")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, string(body))
	}
	answerBytes, _ := io.ReadAll(resp.Body)
	answerSDP := string(answerBytes)
	t.Logf("answer SDP fmtp lines:\n%s", grepLines(answerSDP, "a=fmtp"))
	t.Logf("answer SDP rtpmap lines:\n%s", grepLines(answerSDP, "a=rtpmap"))

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answerSDP,
	}); err != nil {
		t.Fatalf("set remote: %v", err)
	}

	connected := make(chan struct{})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateConnected {
			select {
			case <-connected:
			default:
				close(connected)
			}
		}
	})
	select {
	case <-connected:
	case <-time.After(8 * time.Second):
		t.Fatal("client never connected")
	}

	var ch int
	select {
	case ch = <-negotiatedChannels:
	case <-time.After(2 * time.Second):
		t.Fatal("never received track")
	}
	t.Logf("negotiated channels = %d", ch)
	if ch != 2 {
		t.Fatalf("WHEP negotiated %d channels — want 2 (stereo). The bridge is sending mono.", ch)
	}

	// Decode received RTP. Skip a few frames to let the encoder warm up.
	dec, err := opus.NewDecoder(48000, ch)
	if err != nil {
		t.Fatalf("decoder: %v", err)
	}
	out := make([]int16, 5760*ch)
	const skip = 25 // 500 ms warmup
	const measure = 50
	var sumL, sumR float64
	var nL, nR int
	deadline := time.After(5 * time.Second)
	for i := 0; i < skip+measure; {
		select {
		case payload := <-rtpCh:
			n, err := dec.Decode(payload, out)
			if err != nil {
				continue
			}
			i++
			if i <= skip {
				continue
			}
			frame := out[:n*ch]
			if ch == 2 {
				for j := 0; j+1 < len(frame); j += 2 {
					sumL += float64(frame[j]) * float64(frame[j])
					sumR += float64(frame[j+1]) * float64(frame[j+1])
					nL++
					nR++
				}
			} else {
				for _, v := range frame {
					sumL += float64(v) * float64(v)
					nL++
				}
			}
		case <-deadline:
			t.Fatalf("only got %d frames before deadline", i)
		}
	}
	rmsL := math.Sqrt(sumL / float64(max1(nL)))
	rmsR := math.Sqrt(sumR / float64(max1(nR)))
	t.Logf("decoded end-to-end RMS: L=%.0f R=%.0f", rmsL, rmsR)
	if rmsL < 1000 {
		t.Fatalf("L channel silent end-to-end (rms=%.0f)", rmsL)
	}
	if ch == 2 && rmsR > rmsL*0.5 {
		t.Fatalf("end-to-end stereo collapsed: L=%.0f R=%.0f (R/L=%.0f%%) — bridge is sending L=R",
			rmsL, rmsR, 100*rmsR/rmsL)
	}
}

func grepLines(s, needle string) string {
	var out []string
	for _, l := range strings.Split(s, "\r\n") {
		if strings.Contains(l, needle) {
			out = append(out, l)
		}
	}
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, needle) && !contains(out, l) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}
