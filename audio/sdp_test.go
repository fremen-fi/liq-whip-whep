package audio_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/fremen-fi/liq-whip-whep/audio"
)

// TestWHEP_AnswerAdvertisesStereo asserts the WHEP answer SDP carries
// `stereo=1; sprop-stereo=1` in the Opus fmtp. Without this, Chrome
// decodes the stream as mono regardless of how the bridge encodes it —
// which is exactly the user-reported "Liquidsoap is stereo but ends up
// L=R" symptom.
func TestWHEP_AnswerAdvertisesStereo(t *testing.T) {
	tmp := shortTempDir(t)
	hub := audio.NewPCMHub(filepath.Join(tmp, "onair.sock"))
	if err := hub.Start(context.Background()); err != nil {
		t.Fatalf("hub start: %v", err)
	}
	t.Cleanup(hub.Stop)

	srv := audio.NewServer("/audio")
	srv.Hub = hub
	mux := http.NewServeMux()
	mux.Handle("/audio/", srv.Handler())
	httpSrv := httptest.NewServer(mux)
	t.Cleanup(httpSrv.Close)

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("client pc: %v", err)
	}
	defer pc.Close()
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	}); err != nil {
		t.Fatalf("add transceiver: %v", err)
	}
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
		t.Fatal("gathering timeout")
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
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	answer, _ := io.ReadAll(resp.Body)
	answerSDP := string(answer)

	if !strings.Contains(answerSDP, "stereo=1") {
		t.Errorf("answer SDP missing stereo=1; Chrome will decode mono. fmtp:\n%s",
			grepLines(answerSDP, "a=fmtp"))
	}
	if !strings.Contains(answerSDP, "sprop-stereo=1") {
		t.Errorf("answer SDP missing sprop-stereo=1. fmtp:\n%s",
			grepLines(answerSDP, "a=fmtp"))
	}
}
