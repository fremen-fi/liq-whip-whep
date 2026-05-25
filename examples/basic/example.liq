# Minimal Liquidsoap script that pairs with examples/basic/main.go.
#
# The Go gateway exposes two Unix sockets:
#
#   MIC_SOCK   — WAV in (browser mic, decoded to 48 kHz mono S16LE)
#   ONAIR_SOCK — WAV out (whatever this script sends gets fanned out to WHEP listeners)
#
# Liquidsoap connects to both as a client via socat.

mic_sock = environment.get(default="/tmp/liq-whip/mic.pcm", "MIC_SOCK")
onair_sock = environment.get(default="/tmp/liq-whip/onair.pcm", "ONAIR_SOCK")

# Browser microphone, as a Liquidsoap source.
mic =
  mksafe(
    stereo(
      input.external.wav(
        id="mic",
        buffer=0.1,
        max=1.,
        restart=true,
        restart_on_error=true,
        "socat -u - UNIX-CONNECT:#{mic_sock}"
      )
    )
  )

# Replace this with whatever you actually want on air. A playlist, a
# radio stream, the mic itself, a fallback — anything that produces a
# Liquidsoap source. Here we just air the mic so you can verify the
# end-to-end loop.
on_air = mic

# Stream on_air to the WHEP fan-out socket.
output.external(
  id="onair",
  fallible=true,
  flush=true,
  reopen_delay=2.,
  reopen_on_error=fun (_) -> 2.,
  self_sync=false,
  start=true,
  %wav(channels=2, header=true),
  "socat -u - UNIX-CONNECT:#{onair_sock}",
  on_air
)
