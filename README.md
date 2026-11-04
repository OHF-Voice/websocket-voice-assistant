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

Checking a wake word that a device already detected:

``` sh
python3 app.py \
    --start-stage 'stt' \
    --wav 'okay_nabu_turn_on_the_lights.wav' \
    --wake-word-phrase 'okay nabu' \
    --wake-word-stt-phrase 'okay now' \
    --verify-wake-word \
    --token '<long lived access token>'
```

The audio must start with the wake word. Home Assistant transcribes it, checks
that the transcript starts with the wake word, and removes it before the command
reaches the conversation agent. If the wake word is not there, the run ends with
a `no_wake_word` error instead.

This needs a pipeline whose speech-to-text engine reports the transcript before
the audio stream has ended, such as [wyoming-vosk][vosk]. Without one the run
ends with a `stt-no-streaming` error.

Speech-to-text systems spell wake words differently, and Home Assistant accepts
only the spellings it is given. Vosk hears "okay nabu" as "okay now", so without
`--wake-word-stt-phrase 'okay now'` the example above is rejected even though the
wake word is there. Repeat the option to accept more than one spelling. Only
case, accents, punctuation and spacing are ignored when comparing.

Use `--wake-word-preroll-ms` to say how much audio comes before the wake word.
Once the transcript covers that plus a margin without the wake word appearing at
the start, the run is rejected and the rest of the audio is never transcribed.
Raise it if a run is rejected even though the wake word is there.

Detecting the wake word locally, like a satellite that runs microWakeWord:

``` sh
pip3 install -r requirements-wake-word.txt

arecord -r 16000 -c 1 -f S16_LE -t raw | \
    python3 app.py \
        --start-stage 'stt' \
        --local-wake-word 'okay_nabu' \
        --token '<long lived access token>'
```

The app listens with [pymicro-wakeword][pymicro-wakeword] and only starts a run
once it detects the wake word. It then sends the last `--wake-word-buffer-ms`
(1500 by default) of audio from before the detection, followed by the live audio,
and tells Home Assistant how much of it is wake word audio with `wake_word_phrase`
and `wake_word_audio_ms`. Home Assistant accepts the wake word and removes that
audio before speech-to-text, or rejects it with `wake_word-rejected` when another
device just woke up with the same wake word.

`--local-wake-word` takes a built-in model (`okay_nabu`, `hey_jarvis`,
`hey_mycroft`, `alexa`) or the path to a model's JSON config. `--wake-word-phrase`
overrides the phrase sent to Home Assistant, which is the model's wake word by
default. This needs a Home Assistant version with `wake_word_audio_ms` in the
`assist_pipeline/run` command.

Use `--wav <path>` to stream a 16Khz 16-bit mono WAV file instead of stdin,
paced as if it were being recorded live.

Use `--pipeline <name>` to change which pipeline to use. Name must match exactly.

Change Home Assistant server with `--websocket-api` (e.g., `homeassistant.locws://homeassistant.local:8123/api/websocket`).

Add `--debug` to see all messages.

[api]: https://developers.home-assistant.io/docs/voice/pipelines/
[vosk]: https://github.com/rhasspy/wyoming-vosk
[pymicro-wakeword]: https://github.com/OHF-Voice/pymicro-wakeword
