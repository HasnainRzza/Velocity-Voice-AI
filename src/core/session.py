"""
session.py — Per-connection conversation state.

Barge-in design
───────────────
Every agent pipeline run is wrapped in an asyncio.Task stored as
`agent_task`. A `_generation` counter increments on every new user turn
so any task that completes late can detect it is stale and discard its
result.

                 user speaks
                     │
         ┌───────────▼────────────────┐
         │  session.interrupt()        │ ← fires on SpeechStarted (VAD)
         │  • tts_stop_event.set()    │   or on interim transcript
         │  • agent_task.cancel()     │
         │  • _generation += 1        │
         └────────────────────────────┘
                     │
         ┌───────────▼────────────────┐
         │  await session             │ ← fires on final transcript
         │    .reset_for_new_turn()   │
         │  • await task (2 s max)    │
         │  • tts_stop_event.clear()  │
         │  • agent_task = None       │
         └────────────────────────────┘
                     │
         ┌───────────▼────────────────┐
         │  asyncio.create_task(      │
         │    run_agent(gen=N)        │
         │  )                         │
         │  Only commits messages     │
         │  if gen == _generation     │
         └────────────────────────────┘
"""

import asyncio
import logging
import uuid
from typing import Dict

from langchain_core.messages import SystemMessage, HumanMessage, AIMessage

from agent.llm import SYSTEM_PROMPT

logger = logging.getLogger(__name__)

# Max time to wait for a cancelled agent task to actually stop before
# declaring it dead and clearing state anyway.
_TASK_CANCEL_TIMEOUT_SEC = 2.0


class Session:
    """
    Holds all state for a single WebSocket conversation.

    Thread/task safety
    ──────────────────
    All mutations happen on the asyncio event loop thread — there is no
    cross-thread access, so no threading locks are needed. asyncio.Event
    objects are used for signalling between coroutines on the same loop.
    """

    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self.messages: list = [SystemMessage(content=SYSTEM_PROMPT)]
        self.max_turns = 10
        self.is_active = True

        # ── Barge-in state ─────────────────────────────────────────────────
        # Passed into every TTS stream_sentence call. Set → stop audio immediately.
        self.tts_stop_event: asyncio.Event = asyncio.Event()

        # The currently running agent pipeline task.
        self.agent_task: asyncio.Task | None = None

        # Monotonically increasing counter. Each new user turn increments this.
        # A task captures `gen = session._generation` at launch time; when it
        # finishes it only commits if `gen == session._generation`.
        self._generation: int = 0

    # ── Convenience property (backward compat) ────────────────────────────

    @property
    def is_interrupted(self) -> bool:
        return self.tts_stop_event.is_set()

    @is_interrupted.setter
    def is_interrupted(self, value: bool) -> None:
        if value:
            self.tts_stop_event.set()
        else:
            self.tts_stop_event.clear()

    # ── Barge-in API ──────────────────────────────────────────────────────

    def interrupt(self) -> None:
        """
        Immediately signal barge-in (safe to call multiple times).

        Actions (all synchronous, no await needed):
          1. Increment _generation — any in-flight task becomes stale.
          2. Set tts_stop_event — TTS aborts on next audio chunk.
          3. Cancel agent_task — LLM/tool coroutines receive CancelledError.
        """
        self._generation += 1
        self.tts_stop_event.set()
        if self.agent_task and not self.agent_task.done():
            self.agent_task.cancel()
            logger.info(
                "[Session %s] Interrupted — generation now %d, agent task cancelled.",
                self.session_id,
                self._generation,
            )

    async def reset_for_new_turn(self) -> None:
        """
        Prepare clean state before starting a new agent turn.

        Awaits the cancelled task (up to _TASK_CANCEL_TIMEOUT_SEC) so it
        cannot race with the incoming task over session.messages. Clears
        the TTS stop event so the new TTS response can play freely.
        """
        if self.agent_task and not self.agent_task.done():
            try:
                await asyncio.wait_for(
                    asyncio.shield(self.agent_task),
                    timeout=_TASK_CANCEL_TIMEOUT_SEC,
                )
            except (asyncio.CancelledError, asyncio.TimeoutError):
                # Task cancelled or took too long — fine, move on.
                pass

        self.tts_stop_event.clear()
        self.agent_task = None
        logger.debug("[Session %s] Reset for new turn (gen=%d).", self.session_id, self._generation)

    # ── Message management ────────────────────────────────────────────────

    def add_user_message(self, content: str) -> None:
        self.messages.append(HumanMessage(content=content))
        self._prune_messages()

    def add_ai_message(self, content: str) -> None:
        self.messages.append(AIMessage(content=content))
        self._prune_messages()

    def _prune_messages(self) -> None:
        """Keep only the system prompt and the last max_turns turn-pairs."""
        system_msgs = [m for m in self.messages if isinstance(m, SystemMessage)]
        other_msgs = [m for m in self.messages if not isinstance(m, SystemMessage)]
        max_messages = self.max_turns * 2
        if len(other_msgs) > max_messages:
            other_msgs = other_msgs[-max_messages:]
        self.messages = system_msgs + other_msgs


class SessionManager:
    def __init__(self) -> None:
        self.sessions: Dict[str, Session] = {}

    def create_session(self) -> Session:
        session_id = str(uuid.uuid4())
        session = Session(session_id)
        self.sessions[session_id] = session
        logger.info("[SessionManager] Session %s created.", session_id)
        return session

    def get_session(self, session_id: str) -> Session:
        return self.sessions.get(session_id)

    def remove_session(self, session_id: str) -> None:
        if session_id in self.sessions:
            del self.sessions[session_id]
            logger.info("[SessionManager] Session %s removed.", session_id)


session_manager = SessionManager()
