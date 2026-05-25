# liq-whip-whep

A WHIP/WHEP WebRTC gateway for [Liquidsoap](https://www.liquidsoap.info/).

- **WHIP** — a browser sends mic audio in; you receive it in Liquidsoap as a `source`.
- **WHEP** — Liquidsoap sends a `source` out; browsers play it back with sub-second latency.

Audio crosses the Go↔Liquidsoap boundary as streaming WAV over Unix sockets. Liquidsoap is responsible for everything else — your music, your jingles, your mix, your show. This library just gives you the WebRTC ingress/egress.

## Install

```sh
go get github.com/fremen-fi/liq-whip-whep/audio
```

You need libopus *and* libopusfile available at build time (`gopkg.in/hraban/opus.v2` pkg-configs both via cgo even when only the encoder/decoder are used):

```sh
# Debian/Ubuntu
sudo apt-get install libopus-dev libopusfile-dev pkg-config
# Fedora
sudo dnf install opus-devel opusfile-devel pkgconf-pkg-config
# Alpine
sudo apk add opus-dev opusfile-dev pkgconf
# Arch
sudo pacman -S opus opusfile pkgconf
# macOS
brew install opus opusfile pkg-config
```

## Use

```go
import "github.com/fremen-fi/liq-whip-whep/audio"

hub  := audio.NewPCMHub("/tmp/onair.pcm")    // WHEP: Liquidsoap writes here, browsers read
sink := audio.NewPCMSink("/tmp/mic.pcm")     // WHIP: browsers write here, Liquidsoap reads
_ = hub.Start(ctx)
_ = sink.Start(ctx)

srv := audio.NewServer("/audio")
srv.Hub  = hub
srv.Sink = sink
srv.AllowedOrigins = []string{"https://your.site"}

http.Handle("/audio/", srv.Handler())
```

That gives you:

| Method | Path                       | Purpose                            |
| ------ | -------------------------- | ---------------------------------- |
| POST   | `/audio/whip`              | Browser sends mic audio in         |
| POST   | `/audio/whep`              | Browser receives on-air audio out  |
| DELETE | `/audio/sessions/<id>`     | Terminate a session                |

Both negotiate SDP over plain HTTP per the WHIP/WHEP drafts (no signaling server required).

On the Liquidsoap side, connect to the sockets with `socat`:

```liquidsoap
# Receive mic audio from a browser
mic = input.external.wav(
  buffer=0.1, restart=true, restart_on_error=true,
  "socat -u - UNIX-CONNECT:/tmp/mic.pcm"
)

# Send any source to browsers
output.external(
  fallible=true, flush=true, reopen_delay=2., self_sync=false,
  %wav(channels=2, header=true),
  "socat -u - UNIX-CONNECT:/tmp/onair.pcm",
  on_air_source
)
```

See [`examples/basic/`](examples/basic) for a complete runnable example.

## Notes

- **PCM format on the sockets.** The hub broadcasts 48 kHz stereo (or mono — it follows the WAV header from Liquidsoap). The sink emits 48 kHz mono — what libopus decodes to natively. Both sides are 16-bit little-endian.
- **One mic at a time.** A new WHIP session preempts any previous one — host-mic semantics. If you need multiple simultaneous mic sources, run multiple `PCMSink`s on different sockets.
- **Many listeners.** WHEP scales to N browsers off a single Liquidsoap source — each peer gets its own Opus encoder but they all share the same PCM hub.
- **Reconnects automatically.** If Liquidsoap restarts, the next connection is accepted; browsers get silence (Opus PLC) during the gap.

## License

Apache 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
