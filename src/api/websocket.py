"""
websocket.py — Voice AI WebSocket endpoint.

Full barge-in pipeline
──────────────────────

  ┌──────────────────────────────────────────────────────────────────────┐
  │ Browser audio → server → Deepgram STT                               │
  └────────────────────┬─────────────────────────────────────────────────┘
                       │
          ┌────────────┴────────────────────────────────┐
          │                                             │
   SpeechStarted (VAD)                        Final transcript
   ~50–150 ms after voice onset               (utterance complete)
          │                                             │
          ▼                                             ▼
   on_speech_started()                       on_stt_message(text)
   ──────────────────                        ──────────────────────
   • session.interrupt()                     • await reset_for_new_turn()
     – tts_stop_event.set()  →abortstTS       – awaits cancelled task
     – agent_task.cancel()   →kills LLM       – clears tts_stop_event
     – _generation += 1                     • capture current generation
   • send {"type":"interrupt"}              • add user message
     to browser (mutes player)             • asyncio.create_task(run_agent)
                                           • only commits if gen matches

Race-condition protection
─────────────────────────
  _generation is incremented on every interrupt(). Each agent task
  captures the generation at launch. If a task finishes late (e.g.
  slow LLM) it checks `gen == session._generation`; if not, it
  discards its result and returns without touching session.messages.
  This prevents a stale response from overwriting a new one.

  reset_for_new_turn() awaits the old task (2 s timeout) before
  clearing tts_stop_event, ensuring no two tasks write session.messages
  concurrently.
"""

import asyncio
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from core.session import session_manager
from services.stt import DeepgramSTT
from services.tts import DeepgramTTS
from agent.workflow import build_workflow
from langchain_core.messages import HumanMessage  # noqa: F401 kept for potential future use

logger = logging.getLogger(__name__)
router = APIRouter()
agent_app = build_workflow()


@router.websocket("/ws/chat")
async def websocket_chat(websocket: WebSocket):
    await websocket.accept()
    session = session_manager.create_session()
    logger.info("[WS] Session %s connected.", session.session_id)

    stt = DeepgramSTT()
    tts = DeepgramTTS()

    dg_send = None
    dg_close = None

    # ── Barge-in: VAD speech onset (earliest signal) ──────────────────────

    async def on_speech_started() -> None:
        """
        Called by the STT service the instant Deepgram's VAD detects voice.
        This fires ~50–150 ms after the user starts speaking — well before
        any words are recognised.

        Actions:
          1. session.interrupt() — stops TTS chunk delivery and cancels agent task.
          2. Send {"type":"interrupt"} to browser — client mutes its audio player.
        """
        if not session.is_interrupted:
            logger.info("[WS][%s] SpeechStarted — interrupting.", session.session_id)
            session.interrupt()
            try:
                await websocket.send_json({"type": "interrupt"})
            except Exception:
                pass  # WebSocket may already be closing

    # ── Fallback barge-in: interim transcript (if vad_events unavailable) ──

    async def on_interim_message() -> None:
        """
        Fallback interrupt trigger: fires when Deepgram returns a non-empty
        interim transcript (requires at least one recognised word, so ~200–500 ms
        slower than SpeechStarted).  Guards with is_interrupted so it does
        not re-trigger if on_speech_started already fired.
        """
        if not session.is_interrupted:
            logger.info("[WS][%s] Interim transcript — interrupting (fallback).", session.session_id)
            session.interrupt()
            try:
                await websocket.send_json({"type": "interrupt"})
            except Exception:
                pass

    # ── Final transcript: run the full agent pipeline ─────────────────────

    async def on_stt_message(transcript: str) -> None:
        """
        Called when Deepgram returns a complete (final) transcript.

        Steps:
          1. Await reset_for_new_turn() — ensures the cancelled task is truly
             done before we clear the stop event (prevents race on messages).
          2. Capture current _generation so we can detect staleness later.
          3. Add the user turn to conversation history.
          4. Create a new asyncio.Task for the agent pipeline and store it on
             session.agent_task so a future barge-in can cancel it.
        """
        logger.info("[WS][%s] Final transcript: %r", session.session_id, transcript)

        # ── Wait for any previous pipeline to fully stop ───────────────────
        await session.reset_for_new_turn()

        # Capture generation AFTER reset so we have the latest value
        my_generation = session._generation

        session.add_user_message(transcript)

        # Build agent state snapshot (messages list shared by reference is fine
        # because only one task runs at a time after reset_for_new_turn).
        state = {
            "messages": session.messages,
            "websocket": websocket,
            "tts": tts,
            "session": session,
        }

        async def run_agent() -> None:
            try:
                result_state = await agent_app.ainvoke(state)

                # ── Staleness guard ────────────────────────────────────────
                # If another barge-in happened while we were running, our
                # generation number no longer matches — discard the result.
                if my_generation != session._generation:
                    logger.info(
                        "[WS][%s] Discarding stale agent result (gen %d < current %d).",
                        session.session_id,
                        my_generation,
                        session._generation,
                    )
                    return

                session.messages = result_state["messages"]
                session._prune_messages()
                logger.info("[WS][%s] Agent turn complete (gen %d).", session.session_id, my_generation)

            except asyncio.CancelledError:
                logger.info(
                    "[WS][%s] Agent task cancelled (barge-in, gen %d).",
                    session.session_id,
                    my_generation,
                )
                # Do not re-raise — task ends cleanly
            except Exception as exc:
                logger.error(
                    "[WS][%s] Agent pipeline error: %s",
                    session.session_id,
                    exc,
                    exc_info=True,
                )

        # Store the task so on_speech_started / on_interim_message can cancel it
        session.agent_task = asyncio.create_task(run_agent())

    # ── Startup ───────────────────────────────────────────────────────────

    try:
        # 1. Greet the user immediately (interruptible)
        greeting = "Welcome to Genesis! How can I help you find your perfect luxury car today?"
        await tts.stream_sentence(
            greeting,
            websocket,
            stop_event=session.tts_stop_event,
        )
        session.add_ai_message(greeting)

        # 2. Open Deepgram STT connection with all three callbacks
        dg_send, dg_close = await stt.create_connection(
            on_transcript_callback=on_stt_message,
            on_interim_callback=on_interim_message,
            on_speech_started_callback=on_speech_started,
        )

        # 3. Continuously forward browser audio to Deepgram
        while session.is_active:
            data = await websocket.receive_bytes()
            await dg_send(data)

    except WebSocketDisconnect:
        logger.info("[WS] Session %s disconnected.", session.session_id)
    except Exception as exc:
        logger.error("[WS] Session %s error: %s", session.session_id, exc, exc_info=True)
    finally:
        session.is_active = False

        # Cancel any in-flight agent task on disconnect
        if session.agent_task and not session.agent_task.done():
            session.agent_task.cancel()
            try:
                await session.agent_task
            except (asyncio.CancelledError, Exception):
                pass

        if dg_close:
            await dg_close()

        if websocket.application_state == WebSocketState.CONNECTED:
            try:
                await websocket.close()
            except Exception:
                pass

        session_manager.remove_session(session.session_id)
        logger.info("[WS] Session %s cleaned up.", session.session_id)
