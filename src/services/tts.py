import asyncio
import httpx
from core.config import settings


class DeepgramTTS:
    """
    Handles TTS by sending text to Deepgram and streaming raw PCM audio
    over the WebSocket to the browser.

    Barge-in support
    ────────────────
    Every call to stream_sentence() accepts an optional asyncio.Event
    (stop_event). The method checks the event before sending EACH audio
    chunk. When the event is set (because the user started speaking),
    the method aborts immediately — no more audio bytes are pushed to
    the WebSocket. This gives sub-chunk (~4 KB) stop latency.
    """

    def __init__(self) -> None:
        if not settings.DEEPGRAM_TTS:
            raise ValueError("DEEPGRAM_TTS is missing")
        self.api_key = settings.DEEPGRAM_TTS
        self.url = "https://api.deepgram.com/v1/speak"
        self.params = {
            "model": "aura-2-thalia-en",
            "encoding": "linear16",
            "sample_rate": "16000",
        }
        self.headers = {
            "Authorization": f"Token {self.api_key}",
            "Content-Type": "application/json",
        }

    async def stream_sentence(
        self,
        sentence: str,
        websocket,
        stop_event: asyncio.Event | None = None,
    ) -> bool:
        """
        Synthesise *sentence* via Deepgram and stream raw PCM audio to the client.

        Args:
            sentence:   Text to speak.
            websocket:  FastAPI/Starlette WebSocket to write audio bytes to.
            stop_event: asyncio.Event that, when set, immediately halts audio
                        delivery (barge-in / interrupt). If None the call runs
                        to completion.

        Returns:
            True  — audio delivered completely.
            False — delivery was aborted (stop_event fired) or WebSocket closed.
        """
        if not sentence.strip():
            return True

        data = {"text": sentence}

        try:
            async with httpx.AsyncClient() as client:
                async with client.stream(
                    "POST",
                    self.url,
                    params=self.params,
                    headers=self.headers,
                    json=data,
                ) as response:
                    if response.status_code != 200:
                        print(f"[TTS] Deepgram error {response.status_code}")
                        return False

                    async for chunk in response.aiter_bytes(chunk_size=4096):
                        # ── Barge-in check ────────────────────────────────
                        # If the user started speaking, abort immediately.
                        if stop_event and stop_event.is_set():
                            return False

                        try:
                            await websocket.send_bytes(chunk)
                        except Exception:
                            # WebSocket closed by client
                            return False

            return True

        except Exception as exc:
            print(f"[TTS] Unexpected error: {exc}")
            return False
