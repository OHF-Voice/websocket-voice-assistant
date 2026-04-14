# Websocket Voice Assistant

Command-line voice assistant example using the [Home Assistant voice websocket API][api].

Receives 16Khz 16-bit mono PCM audio from `stdin`.

## Usage

With streaming wake word detection:

``` sh
arecord -r 16000 -c 1 -f S16_LE -t raw | \
    python3 app.py \
        --start-stage 'wake_word' \
        --token '<long lived access token>'
```

**NOTE:** Home Assistant will disconnect after 10 seconds of silence. Use `--wake-word-time <seconds>` to adjust.

Just speech-to-text:

``` sh
arecord -r 16000 -c 1 -f S16_LE -t raw | \
    python3 app.py \
        --start-stage 'stt' \
        --token '<long lived access token>'
```

Use `--pipeline <name>` to change which pipeline to use. Name must match exactly.

Change Home Assistant server with `--websocket-api` (e.g., `homeassistant.locws://homeassistant.local:8123/api/websocket`).

Add `--debug` to see all messages.

[api]: https://developers.home-assistant.io/docs/voice/pipelines/
