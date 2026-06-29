import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Bootstrap async logging FIRST — before any module calls logging.getLogger.
import logging
from core.logger import setup_logging, start_background_logger, stop_background_logger

setup_logging(log_file_name="voice_ai.log", level=logging.INFO)
logger = logging.getLogger(__name__)

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from core.health import perform_health_checks
from core.cache import warm_up_cache, close_cache
from api.websocket import router as websocket_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the background log-flush worker and recovery watcher.
    await start_background_logger()

    # Startup
    logger.info("Starting up Voice AI service...")

    # Block until health checks pass
    if not await perform_health_checks():
        logger.error("Critical services are down. Cannot start.")
        raise RuntimeError("Health checks failed.")

    # Warm up cache
    await warm_up_cache()

    logger.info("Service is ready to accept connections.")
    yield

    # Shutdown — flush remaining log records before closing.
    logger.info("Shutting down Voice AI service...")
    await close_cache()
    await stop_background_logger()


app = FastAPI(title="Voice AI Production Server", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(websocket_router)

# Serve the static HTML client for testing.
# index.html lives one level above src/ (i.e. in voice-ai/), so we resolve
# the parent directory relative to this file's absolute path rather than
# relying on the CWD at runtime (which varies depending on where uvicorn
# is invoked from).
_STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
app.mount("/", StaticFiles(directory=_STATIC_DIR, html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
