import asyncio
import logging
from typing import Dict, Any

from .config import settings
from .cache import redis_pool
from agent.chroma_service import check_chroma_service
from langchain_groq import ChatGroq

logger = logging.getLogger(__name__)

async def check_redis() -> bool:
    try:
        await redis_pool.ping()
        return True
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        return False

async def check_chroma() -> bool:
    try:
        # chroma check is synchronous, wrap it in a thread
        result = await asyncio.to_thread(check_chroma_service)
        if not result.get("ok"):
            logger.error(f"Chroma health check failed: {result.get('error')}")
            return False
        return True
    except Exception as e:
        logger.error(f"Chroma health check failed: {e}")
        return False

async def check_groq() -> bool:
    if not settings.GROQ_API_KEY:
        logger.error("Groq API key missing")
        return False
    try:
        llm = ChatGroq(model="llama-3.1-8b-instant", temperature=0, max_tokens=10)
        # Just a tiny generation to ensure it works
        await llm.ainvoke("hi")
        return True
    except Exception as e:
        logger.error(f"Groq health check failed: {e}")
        return False

async def check_deepgram() -> bool:
    if not settings.DEEPGRAM_STT or not settings.DEEPGRAM_TTS:
        logger.error("Deepgram API keys missing")
        return False
    return True # Basic check since Deepgram WebSocket is checked on connection

async def perform_health_checks() -> bool:
    """Run all health checks and return True if all pass. Retries with backoff."""
    retries = 3
    delay = 2
    
    for attempt in range(1, retries + 1):
        logger.info(f"Running health checks (Attempt {attempt}/{retries})...")
        
        results = await asyncio.gather(
            check_redis(),
            check_chroma(),
            check_groq(),
            check_deepgram()
        )
        
        if all(results):
            logger.info("All health checks passed!")
            return True
            
        logger.warning(f"Health checks failed. Retrying in {delay} seconds...")
        await asyncio.sleep(delay)
        delay *= 2
        
    logger.error("Health checks failed after maximum retries.")
    return False
