"""Stream audio from stdin to Home Assistant websocket voice API."""

import argparse
import asyncio
import logging
import sys
import time
import wave
from collections.abc import Callable

import aiohttp

try:
    from pymicro_wakeword import MicroWakeWord, MicroWakeWordFeatures, Model
except ImportError:
    MicroWakeWord = None

_LOGGER = logging.getLogger(__name__)

SAMPLE_RATE = 16000
SAMPLE_WIDTH = 2
SAMPLE_CHANNELS = 1
BYTES_PER_MS = SAMPLE_RATE * SAMPLE_WIDTH * SAMPLE_CHANNELS // 1000


def _wav_reader(wav_path: str, bytes_per_chunk: int) -> Callable[[], bytes]:
    """Read a WAV file in chunks, pacing it so audio arrives as it would live.

    Home Assistant measures the wake word deadline in audio time rather than
    wall clock, so pacing is not needed for that. It is here so the timing a
    speech-to-text service sees matches what a device would send.
    """
    wav_file = wave.open(wav_path, "rb")
    if (
        wav_file.getframerate() != SAMPLE_RATE
        or wav_file.getsampwidth() != SAMPLE_WIDTH
        or wav_file.getnchannels() != SAMPLE_CHANNELS
    ):
        sys.exit(
            f"{wav_path} must be {SAMPLE_RATE}Hz {SAMPLE_WIDTH * 8}-bit mono, got "
            f"{wav_file.getframerate()}Hz {wav_file.getsampwidth() * 8}-bit "
            f"{wav_file.getnchannels()} channel(s)"
        )

    seconds_per_chunk = (bytes_per_chunk // SAMPLE_WIDTH) / SAMPLE_RATE
    next_chunk_at = time.monotonic()

    def read() -> bytes:
        nonlocal next_chunk_at
        chunk = wav_file.readframes(bytes_per_chunk // SAMPLE_WIDTH)
        next_chunk_at += seconds_per_chunk
        time.sleep(max(0, next_chunk_at - time.monotonic()))
        return chunk

    return read


def _stdin_reader(bytes_per_chunk: int) -> Callable[[], bytes]:
    """Read raw audio from stdin in chunks."""

    def read() -> bytes:
        return sys.stdin.buffer.read(bytes_per_chunk)

    return read


def _load_wake_word(model: str) -> "MicroWakeWord":
    """Load a built-in microWakeWord model by name, or one from its JSON config."""
    if model.endswith(".json"):
        return MicroWakeWord.from_config(model)

    return MicroWakeWord.from_builtin(Model(model))


async def _wait_for_wake_word(
    read_chunk: Callable[[], bytes], mww: "MicroWakeWord", buffer_bytes: int
) -> bytes | None:
    """Return the last buffer_bytes of audio once the wake word is detected."""
    features = MicroWakeWordFeatures()
    buffer = bytearray()
    # Reading blocks, so keep the websocket connection serviced meanwhile
    while chunk := await asyncio.to_thread(read_chunk):
        buffer += chunk
        del buffer[:-buffer_bytes]
        for chunk_features in features.process_streaming(chunk):
            if mww.process_streaming(chunk_features):
                return bytes(buffer)

    return None


async def main() -> None:
    """Create websocket session then POST audio data to HTTP API."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start-stage",
        required=True,
        choices=("wake_word", "stt"),
        help="Pipeline stage to start at",
    )
    parser.add_argument("--token", required=True, help="HA auth token")
    parser.add_argument(
        "--pipeline", help="Name of pipeline to use (default: preferred)"
    )
    parser.add_argument("--samples-per-chunk", type=int, default=1024)
    parser.add_argument(
        "--websocket-api",
        default="ws://homeassistant.local:8123/api/websocket",
        help="Websocket API URI for Home Assistant",
    )
    parser.add_argument(
        "--wake-word-timeout",
        type=float,
        default=5.0,
        help="Seconds of silence before wake word detection times out",
    )
    parser.add_argument(
        "--wav",
        help="Stream this WAV file instead of stdin (16Khz 16-bit mono)",
    )
    parser.add_argument(
        "--wake-word-phrase",
        help="Wake word a device detected before this audio (--start-stage stt)",
    )
    parser.add_argument(
        "--verify-wake-word",
        action="store_true",
        help=(
            "Check the wake word against the transcript. The audio must start"
            " with the wake word, and the pipeline needs a speech-to-text engine"
            " that reports the transcript before the audio stream has ended"
        ),
    )
    parser.add_argument(
        "--wake-word-stt-phrase",
        action="append",
        metavar="PHRASE",
        help=(
            "Transcription to accept for the wake word, on top of the ones Home"
            " Assistant knows. May be repeated"
        ),
    )
    parser.add_argument(
        "--wake-word-preroll-ms",
        type=int,
        help=(
            "Milliseconds of audio captured before the wake word was detected."
            " The wake word must be transcribed within this, plus a margin"
        ),
    )
    parser.add_argument(
        "--local-wake-word",
        metavar="MODEL",
        help=(
            "Detect this wake word locally with microWakeWord before starting the"
            " run, and send the audio from before the detection along with it"
            " (--start-stage stt). A built-in model name such as okay_nabu, or a"
            " model's JSON config. Needs pymicro-wakeword"
        ),
    )
    parser.add_argument(
        "--wake-word-buffer-ms",
        type=int,
        default=1500,
        help="Milliseconds of audio from before a local detection to send",
    )
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)

    if args.verify_wake_word and not args.wake_word_phrase:
        parser.error("--verify-wake-word requires --wake-word-phrase")

    if args.wake_word_phrase and (args.start_stage != "stt"):
        parser.error("--wake-word-phrase requires --start-stage stt")

    mww: "MicroWakeWord | None" = None
    if args.local_wake_word:
        if args.start_stage != "stt":
            parser.error("--local-wake-word requires --start-stage stt")

        if MicroWakeWord is None:
            parser.error(
                "--local-wake-word needs pymicro-wakeword:"
                " pip3 install -r requirements-wake-word.txt"
            )

        if args.wake_word_buffer_ms < 1:
            parser.error("--wake-word-buffer-ms must be at least 1")

        mww = _load_wake_word(args.local_wake_word)

    bytes_per_chunk = args.samples_per_chunk * SAMPLE_WIDTH
    read_chunk = (
        _wav_reader(args.wav, bytes_per_chunk)
        if args.wav
        else _stdin_reader(bytes_per_chunk)
    )

    id = 0

    def next_id():
        nonlocal id
        id += 1
        return id

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(args.websocket_api) as websocket:
            # Authenticate
            msg = await websocket.receive_json()
            assert msg["type"] == "auth_required", msg

            await websocket.send_json(
                {
                    "type": "auth",
                    "access_token": args.token,
                }
            )

            msg = await websocket.receive_json()
            _LOGGER.debug(msg)
            assert msg["type"] == "auth_ok", msg

            pipeline_id = None
            if args.pipeline:
                await websocket.send_json(
                    {"type": "assist_pipeline/pipeline/list", "id": next_id()}
                )
                msg = await websocket.receive_json()
                _LOGGER.debug(msg)
                assert msg["success"]
                pipeline_names = []
                for pipeline in msg["result"]["pipelines"]:
                    if args.pipeline == pipeline["name"]:
                        pipeline_id = pipeline["id"]
                        _LOGGER.debug(
                            "Selected pipeline %s for %s", pipeline_id, args.pipeline
                        )
                        break

                    pipeline_names.append(pipeline["name"])

                if not pipeline_id:
                    _LOGGER.error(
                        "Couldn't find pipeline '%s' in: %s",
                        args.pipeline,
                        sorted(pipeline_names),
                    )
                    sys.exit(1)

            # Run pipeline
            run_args = {}
            if pipeline_id:
                run_args["pipeline"] = pipeline_id

            input_args = {}
            if args.start_stage == "wake_word":
                input_args["timeout"] = args.wake_word_timeout
            else:
                if args.wake_word_phrase:
                    input_args["wake_word_phrase"] = args.wake_word_phrase

                if args.verify_wake_word:
                    input_args["verify_wake_word"] = True

                if args.wake_word_stt_phrase:
                    input_args["wake_word_stt_phrases"] = args.wake_word_stt_phrase

                if args.wake_word_preroll_ms is not None:
                    input_args["wake_word_preroll_ms"] = args.wake_word_preroll_ms

            wake_word_audio = b""
            if mww is not None:
                _LOGGER.info("Listening locally for %s...", mww.wake_word)
                detected_audio = await _wait_for_wake_word(
                    read_chunk, mww, args.wake_word_buffer_ms * BYTES_PER_MS
                )
                if detected_audio is None:
                    _LOGGER.info("Audio ended without the wake word")
                    return

                wake_word_audio = detected_audio
                _LOGGER.info(
                    "Detected locally: %s (sending %s ms from before the detection)",
                    mww.wake_word,
                    len(wake_word_audio) // BYTES_PER_MS,
                )
                input_args["wake_word_phrase"] = args.wake_word_phrase or mww.wake_word
                input_args["wake_word_audio_ms"] = len(wake_word_audio) // BYTES_PER_MS

            await websocket.send_json(
                {
                    "type": "assist_pipeline/run",
                    "id": next_id(),
                    "input": {"sample_rate": SAMPLE_RATE, **input_args},
                    "start_stage": args.start_stage,
                    "end_stage": "tts",
                    **run_args,
                }
            )

            msg = await websocket.receive_json()
            _LOGGER.debug(msg)
            assert msg["success"]

            # Get handler id
            msg = await websocket.receive_json()
            _LOGGER.debug(msg)
            handler_id = bytes(
                [msg["event"]["data"]["runner_data"]["stt_binary_handler_id"]]
            )

            # Stream audio, starting with the audio from before a local detection
            chunk = wake_word_audio or read_chunk()
            audio_task = asyncio.create_task(websocket.send_bytes(handler_id + chunk))
            event_task = asyncio.create_task(websocket.receive_json())
            done = set()
            pending = {audio_task, event_task}
            while True:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )

                # Determine which task finished first
                if audio_task in done:
                    chunk = read_chunk()
                    audio_task = asyncio.create_task(
                        websocket.send_bytes(handler_id + chunk)
                    )
                    pending.add(audio_task)

                if event_task in done:
                    msg = await event_task
                    _LOGGER.debug(msg)
                    event_type = msg["event"]["type"]
                    if event_type == "run-end":
                        audio_task.cancel()
                        break

                    if event_type == "wake_word-start":
                        _LOGGER.info("Listening for wake word...")
                    elif event_type == "wake_word-end":
                        _LOGGER.info(
                            "Detected: %s",
                            msg["event"]["data"]["wake_word_output"][
                                "wake_word_phrase"
                            ],
                        )
                    elif event_type == "wake_word-rejected":
                        _LOGGER.info(
                            "Rejected: %s (another device just woke up with it)",
                            msg["event"]["data"]["wake_word_output"][
                                "wake_word_phrase"
                            ],
                        )
                    elif event_type == "stt-vad-start":
                        _LOGGER.info("Recording...")
                    elif event_type == "stt-vad-end":
                        _LOGGER.info("Processing...")
                    elif event_type == "stt-progress":
                        _LOGGER.info(
                            "STT (partial): %s",
                            msg["event"]["data"]["stt_output"]["text"],
                        )
                    elif event_type == "stt-end":
                        _LOGGER.info(
                            "STT: %s", msg["event"]["data"]["stt_output"]["text"]
                        )
                    elif event_type == "tts-start":
                        _LOGGER.info("TTS: %s", msg["event"]["data"]["tts_input"])
                    elif event_type == "error":
                        data = msg["event"]["data"]
                        if data["code"] == "no_wake_word":
                            _LOGGER.info(
                                "Rejected: wake word was not in the transcript"
                            )
                        else:
                            _LOGGER.error("%s: %s", data["code"], data["message"])

                    event_task = asyncio.create_task(websocket.receive_json())
                    pending.add(event_task)


if __name__ == "__main__":
    asyncio.run(main())
