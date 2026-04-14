"""Stream audio from stdin to Home Assistant websocket voice API."""

import argparse
import asyncio
import logging
import sys

import aiohttp

_LOGGER = logging.getLogger(__name__)


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
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)

    bytes_per_chunk = args.samples_per_chunk * 2
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

            await websocket.send_json(
                {
                    "type": "assist_pipeline/run",
                    "id": next_id(),
                    "input": {"sample_rate": 16000, **input_args},
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

            # Stream audio
            chunk = sys.stdin.buffer.read(bytes_per_chunk)
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
                    chunk = sys.stdin.buffer.read(bytes_per_chunk)
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
                    elif event_type == "stt-vad-start":
                        _LOGGER.info("Recording...")
                    elif event_type == "stt-vad-end":
                        _LOGGER.info("Processing...")
                    elif event_type == "stt-end":
                        _LOGGER.info(
                            "STT: %s", msg["event"]["data"]["stt_output"]["text"]
                        )
                    elif event_type == "tts-start":
                        _LOGGER.info("TTS: %s", msg["event"]["data"]["tts_input"])

                    event_task = asyncio.create_task(websocket.receive_json())
                    pending.add(event_task)


if __name__ == "__main__":
    asyncio.run(main())
