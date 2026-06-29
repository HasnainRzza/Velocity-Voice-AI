
from __future__ import annotations

import asyncio
import atexit
import logging
import logging.handlers
import sys
import time
from pathlib import Path
from typing import List, Optional

# ── Configuration ─────────────────────────────────────────────────────────────

# Number of seconds between forced flushes even if batch is not full.
BATCH_INTERVAL_SECONDS: int = 60

# Flush immediately when the queue reaches this many records.
MAX_BATCH_RECORDS: int = 500

# Seconds to wait before restarting a crashed flush worker.
RECOVERY_DELAY_SECONDS: int = 5

# Maximum number of automatic recovery attempts before giving up.
MAX_RECOVERY_ATTEMPTS: int = 10

# Resolved once at import time; points to voice-ai/logs/ regardless of CWD.
_LOGS_DIR: Path = Path(__file__).resolve().parents[2] / "logs"


_log_queue: asyncio.Queue[logging.LogRecord] = asyncio.Queue(maxsize=10_000)
_flush_task: Optional[asyncio.Task] = None
_recovery_attempts: int = 0
_shutdown_event: asyncio.Event = asyncio.Event()

# ── Queue-based handler (non-blocking) ────────────────────────────────────────

class AsyncQueueHandler(logging.Handler):
    """
    Puts log records onto the async queue without blocking the caller.
    If the queue is full the record is silently dropped (preferred over
    blocking or crashing the application).
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            _log_queue.put_nowait(record)
        except asyncio.QueueFull:
            # Queue is full – drop rather than block.
            pass
        except Exception:
            self.handleError(record)


# ── Flush worker ──────────────────────────────────────────────────────────────

async def _flush_worker(file_handler: logging.FileHandler) -> None:
    """
    Drains _log_queue and writes records to disk in batches.
    Runs as a long-lived background asyncio task.
    """
    last_flush = time.monotonic()
    batch: List[logging.LogRecord] = []

    while not _shutdown_event.is_set():
        # Wait for up to 1 second for a new record so we can check the
        # shutdown event and batch-interval deadline regularly.
        try:
            record = await asyncio.wait_for(_log_queue.get(), timeout=1.0)
            batch.append(record)
        except asyncio.TimeoutError:
            pass  # no new record; fall through to check flush conditions

        now = time.monotonic()
        should_flush = (
            len(batch) >= MAX_BATCH_RECORDS
            or (batch and (now - last_flush) >= BATCH_INTERVAL_SECONDS)
        )

        if should_flush:
            _write_batch(file_handler, batch)
            batch.clear()
            last_flush = now

    # Drain any remaining records on shutdown.
    while not _log_queue.empty():
        try:
            batch.append(_log_queue.get_nowait())
        except asyncio.QueueEmpty:
            break
    if batch:
        _write_batch(file_handler, batch)


def _write_batch(file_handler: logging.FileHandler, batch: List[logging.LogRecord]) -> None:
    """Synchronously emit a batch of records through the file handler."""
    for record in batch:
        try:
            file_handler.emit(record)
        except Exception:
            # Never let a logging error propagate into the application.
            pass
    file_handler.flush()


# ── Recovery mechanism ────────────────────────────────────────────────────────

async def _watch_and_recover(file_handler: logging.FileHandler) -> None:
    """
    Monitors the flush worker task and restarts it if it crashes.
    """
    global _flush_task, _recovery_attempts

    while not _shutdown_event.is_set():
        await asyncio.sleep(RECOVERY_DELAY_SECONDS)

        if _flush_task is None or _flush_task.done():
            exc = _flush_task.exception() if (_flush_task and not _flush_task.cancelled()) else None
            if exc:
                sys.stderr.write(
                    f"[logger] Flush worker crashed ({exc!r}). "
                    f"Attempt {_recovery_attempts + 1}/{MAX_RECOVERY_ATTEMPTS} to restart.\n"
                )

            if _recovery_attempts >= MAX_RECOVERY_ATTEMPTS:
                sys.stderr.write(
                    "[logger] Max recovery attempts reached. Logging to disk is disabled.\n"
                )
                return

            _recovery_attempts += 1
            _flush_task = asyncio.create_task(
                _flush_worker(file_handler), name="log_flush_worker"
            )



def setup_logging(log_file_name: str = "voice_ai.log", level: int = logging.INFO) -> None:
    """
    Configure the root logger for async file-only logging.

    Call this ONCE at application startup, before any other module logs
    anything. It is safe to call from synchronous code (e.g. module-level).
    The background asyncio tasks are started separately via
    ``start_background_logger()`` once an event loop is running.
    """
    _LOGS_DIR.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(level)

    # Remove every existing handler (including any StreamHandler that would
    # print to the terminal).
    root.handlers.clear()

    # Install our non-blocking queue handler.
    root.addHandler(AsyncQueueHandler())

    # Suppress overly verbose third-party loggers.
    for noisy in ("uvicorn.access", "httpx", "httpcore", "chromadb"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # Register a best-effort synchronous flush on interpreter exit so the
    # last records are not silently lost if the loop is gone.
    _log_file = _LOGS_DIR / log_file_name
    _file_handler = _make_file_handler(_log_file)
    atexit.register(_sync_flush_on_exit, _file_handler)

    # Store handler so start_background_logger can access it.
    setup_logging._file_handler = _file_handler  # type: ignore[attr-defined]


def _make_file_handler(log_file: Path) -> logging.FileHandler:
    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    return handler


async def start_background_logger() -> None:
    """
    Start the async flush worker and the recovery watcher.

    Must be called from inside a running asyncio event loop, e.g. from a
    FastAPI lifespan startup handler.
    """
    global _flush_task, _recovery_attempts

    _recovery_attempts = 0
    _shutdown_event.clear()

    file_handler: logging.FileHandler = setup_logging._file_handler  # type: ignore[attr-defined]

    _flush_task = asyncio.create_task(
        _flush_worker(file_handler), name="log_flush_worker"
    )
    asyncio.create_task(
        _watch_and_recover(file_handler), name="log_recovery_watcher"
    )


async def stop_background_logger() -> None:
    """
    Signal the flush worker to drain and stop.
    Call from the FastAPI lifespan shutdown handler.
    """
    _shutdown_event.set()
    if _flush_task and not _flush_task.done():
        try:
            await asyncio.wait_for(_flush_task, timeout=10.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass


def _sync_flush_on_exit(file_handler: logging.FileHandler) -> None:
    """Best-effort drain on interpreter exit (synchronous fallback)."""
    batch: List[logging.LogRecord] = []
    while not _log_queue.empty():
        try:
            batch.append(_log_queue.get_nowait())
        except Exception:
            break
    _write_batch(file_handler, batch)
