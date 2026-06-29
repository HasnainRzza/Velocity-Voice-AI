"""
stt.py — Native async Deepgram STT.

Bypasses the synchronous Deepgram SDK to avoid threading deadlocks.

Event routing
─────────────
Deepgram sends three distinct message types we care about:

  SpeechStarted  (vad_events=true required)
      ↓ user voice activity detected — fires BEFORE any transcript exists
      → on_speech_started_callback()   ← earliest possible barge-in trigger

  is_final=False  (interim partial transcript)
      ↓ fallback barge-in signal if vad_events is not available
      → on_interim_callback()

  is_final=True  (complete utterance transcript)
      ↓ user finished speaking, full text ready
      → on_transcript_callback(text)   ← send to agent pipeline

Setting vad_events=true gives us the lowest-latency interrupt:
  SpeechStarted fires ~50–150 ms after the user starts speaking,
  vs. interim transcripts which require at least one recognised word.
"""

import json
import asyncio
import logging
import websockets
from core.config import settings

logger = logging.getLogger(__name__)

DEEPGRAM_WSS = (
    "wss://api.deepgram.com/v1/listen"
    "?model=nova-3"
    "&language=en-US"
    "&endpointing=300"
    "&vad_events=true"   # enables SpeechStarted / SpeechEnded events
    # No encoding param — Deepgram auto-detects WebM/Opus from the container
)


class DeepgramSTT:
    def __init__(self) -> None:
        if not settings.DEEPGRAM_STT:
            raise ValueError("DEEPGRAM_STT is missing")
        self.api_key = settings.DEEPGRAM_STT

    async def create_connection(
        self,
        on_transcript_callback,
        on_interim_callback=None,
        on_speech_started_callback=None,
    ):
        """
        Open a native async WebSocket to Deepgram.

        Args:
            on_transcript_callback:      async fn(text: str) — called on final transcript.
            on_interim_callback:         async fn() — called on partial transcript (fallback barge-in).
            on_speech_started_callback:  async fn() — called on VAD SpeechStarted (primary barge-in).

        Returns:
            (send_fn, close_fn) — coroutines to forward audio and close the connection.
        """
        headers = {"Authorization": f"Token {self.api_key}"}
        dg_ws = await websockets.connect(DEEPGRAM_WSS, additional_headers=headers)
        logger.info("[STT] Connected to Deepgram.")

        async def listen_loop() -> None:
            try:
                async for raw_msg in dg_ws:
                    try:
                        msg = json.loads(raw_msg)
                        msg_type = msg.get("type", "")

                        # ── VAD SpeechStarted — earliest barge-in signal ──────
                        if msg_type == "SpeechStarted":
                            logger.debug("[STT] SpeechStarted event received.")
                            if on_speech_started_callback:
                                await on_speech_started_callback()
                            continue

                        # ── Transcript (interim or final) ─────────────────────
                        if msg_type in ("Results", ""):
                            is_final = msg.get("is_final", False)
                            alts = (msg.get("channel") or {}).get("alternatives", [])
                            transcript = alts[0].get("transcript", "").strip() if alts else ""

                            if not transcript:
                                continue

                            if is_final:
                                logger.info("[STT] Final transcript: %r", transcript)
                                await on_transcript_callback(transcript)
                            else:
                                # Interim — fallback barge-in if vad_events not firing
                                logger.debug("[STT] Interim: %r", transcript)
                                if on_interim_callback:
                                    await on_interim_callback()

                    except Exception as exc:
                        logger.error("[STT] Parse error: %s", exc)

            except websockets.exceptions.ConnectionClosedError as exc:
                logger.info("[STT] Connection closed: %s", exc)
            except asyncio.CancelledError:
                pass

        asyncio.create_task(listen_loop())

        async def send(data: bytes) -> None:
            try:
                await dg_ws.send(data)
            except Exception as exc:
                logger.warning("[STT] Send error: %s", exc)

        async def close() -> None:
            try:
                await dg_ws.close()
                logger.info("[STT] Connection closed cleanly.")
            except Exception:
                pass

        return send, close
