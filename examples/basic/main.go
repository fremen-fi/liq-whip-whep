// basic is a minimal WHIP/WHEP gateway for Liquidsoap. It exposes:
//
//	POST   /audio/whip                — browser sends mic audio in
//	POST   /audio/whep                — browser receives on-air audio out
//	DELETE /audio/sessions/<id>       — terminate a session
//
// Audio flows through two Unix sockets that Liquidsoap reads/writes via
// socat. See examples/basic/example.liq for the matching Liquidsoap side.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fremen-fi/liq-whip-whep/audio"
)

func main() {
	var (
		listen     = flag.String("listen", ":8080", "HTTP listen address")
		micSock    = flag.String("mic-sock", "/tmp/liq-whip/mic.pcm", "Unix socket where Liquidsoap reads mic PCM from")
		onAirSock  = flag.String("onair-sock", "/tmp/liq-whip/onair.pcm", "Unix socket where Liquidsoap writes on-air PCM to")
		origins    = flag.String("allow-origin", "*", "CORS allowed origin (\"*\" for any, comma-separated for many)")
	)
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	hub := audio.NewPCMHub(*onAirSock)
	if err := hub.Start(ctx); err != nil {
		slog.Error("hub start", "err", err)
		os.Exit(1)
	}
	defer hub.Stop()

	sink := audio.NewPCMSink(*micSock)
	if err := sink.Start(ctx); err != nil {
		slog.Error("sink start", "err", err)
		os.Exit(1)
	}
	defer sink.Stop()

	srv := audio.NewServer("/audio")
	srv.Hub = hub
	srv.Sink = sink
	srv.AllowedOrigins = []string{*origins}
	defer srv.Shutdown()

	mux := http.NewServeMux()
	mux.Handle("/audio/", srv.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	httpSrv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	slog.Info("listening", "addr", *listen, "mic_sock", *micSock, "onair_sock", *onAirSock)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
}
