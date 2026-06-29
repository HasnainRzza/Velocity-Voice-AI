# Conversational Voice Agent

A real-time, fully conversational AI agent built for the Genesis CPO car dealership. The system accepts browser microphone audio over a WebSocket, transcribes speech in real time using Deepgram, runs a stateful reasoning pipeline powered by Groq and LangGraph, queries a hybrid Redis and ChromaDB inventory database, and streams synthesised speech audio back to the browser. Barge-in (speaking over the agent) is supported with sub-150 ms interrupt latency.

---
## Demo Video: https://drive.google.com/file/d/1eLPwuT-AS_akZoPOK_wDvGWW6HlP3TtX/view?usp=sharing
## Quick Start

### 1. Prerequisites

| Dependency | Purpose |
|---|---|
| Python 3.10+ | Runtime |
| [uv](https://github.com/astral-sh/uv) | Fast Python package installer |
| Docker | Running Redis locally |
| ChromaDB server | Vector database (can be cloud-hosted) |

### 2. Start Dependencies

Redis must be running before the application starts:
```bash
docker run -d -p 6379:6379 redis
```

ChromaDB must be accessible at the credentials set in your `.env` file.

### 3. Install Python Packages

```bash
uv pip install -r requirements.txt
```

### 4. Configure Environment

Create a `.env` file in the `voice-ai/` root directory:
```env
DEEPGRAM_STT=your_deepgram_api_key_here
DEEPGRAM_TTS=your_deepgram_api_key_here
GROQ_API_KEY=your_groq_api_key_here
CHROMADB_API_KEY=your_chromadb_api_key
CHROMA_DB_TENANT=your_tenant_id
CHROMA_DB_DATABASE=your_database_name
REDIS_URL=redis://localhost:6379/0
```

### 5. Run the Server

```bash
uv run uvicorn main:app --port 8000
```

from the `voice-ai/src/` directory, or:

```bash
uv run python src/main.py
```

Once started, open `http://localhost:8000` in your browser, allow microphone access, and begin speaking.

---

## System Architecture

The application is a single-process FastAPI server. All I/O is asynchronous. The high-level data flow is as follows:

```
Browser Microphone Audio (WebM/Opus)
        |
        v
  WebSocket /ws/chat  (api/websocket.py)
        |
        +-------> Deepgram STT WebSocket (services/stt.py)
        |               |
        |         SpeechStarted (VAD)      Final Transcript
        |               |                        |
        |         interrupt()             run_agent()
        |               |                        |
        |         TTS abort +            LangGraph Workflow
        |         agent cancel           (agent/workflow.py)
        |                                        |
        |                              intent_router
        |                           /       |       \
        |                       booking  farewell  generate_node
        |                                            |
        |                                    search_inventory?
        |                                            |
        |                                    retrieve_node
        |                                    (Redis + ChromaDB)
        |                                            |
        |                                    generate_node (LLM reply)
        |                                            |
        v                                            v
  Browser Audio Player  <----  Deepgram TTS HTTP stream (services/tts.py)
```

---

## Module Reference

### Entry Point

#### `src/main.py`

The FastAPI application entry point. Responsibilities:
- Calls `setup_logging()` before any other module is imported, installing the async file logger and suppressing all terminal output.
- Defines the `lifespan` context manager, which on startup starts the background log flush worker, runs health checks, and warms the Redis cache. On shutdown it closes the Redis pool and flushes remaining log records.
- Mounts the WebSocket API router and serves `index.html` as a static file for the browser-based test client.

---

### `src/api/`

#### `websocket.py`

The central orchestrator for each voice session. Every browser connection gets its own `Session` object and independent STT and TTS instances.

Key responsibilities:
- Accepts WebSocket connections at `/ws/chat` and spawns Deepgram STT immediately.
- Registers three STT callbacks:
  - `on_speech_started`: fires on Deepgram VAD detection (50-150 ms after voice onset). Calls `session.interrupt()`, which sets the TTS stop event and cancels the in-flight agent task. Sends `{"type": "interrupt"}` to the browser so the audio player is muted immediately.
  - `on_stt_message`: fires on a final transcript. Calls `session.reset_for_new_turn()`, adds the user message to history, and launches a new `run_agent` task.
  - `on_interim_transcript`: fallback barge-in if VAD events are unavailable.
- `run_agent` builds the LangGraph state and invokes the compiled workflow. A generation counter is captured at launch; the task checks this counter before committing any AI message to the session history, preventing stale LLM responses from overwriting a newer turn.
- Handles `WebSocketDisconnect` by cancelling all tasks, closing the Deepgram connection, and removing the session.

---

### `src/agent/`

#### `workflow.py`

Builds and compiles the LangGraph `StateGraph`. The graph defines the complete decision logic for a single agent turn:

```
START
  |
  +-- intent_router (conditional) --+
  |                                  |
  v                                  v                   v
booking                          farewell           generate_node
  |                                  |                   |
  v                                  v      should_retrieve (conditional)
 END                                END       |              |
                                         retrieve_node    END
                                              |
                                         generate_node
                                              |
                                            END
```

#### `nodes.py`

Defines all LangGraph nodes and the shared `AgentState` TypedDict.

- **`intent_router`**: Inspects the last user message using regex patterns. Routes to `booking`, `farewell`, or `general` (generate) without calling the LLM, making these paths instantaneous.
- **`booking_node`**: Returns a fixed booking instructions response and streams it to TTS without an LLM call.
- **`farewell_node`**: Returns a farewell message, streams it to TTS, then closes the WebSocket cleanly with code 1000.
- **`generate_node`**: Invokes the Groq LLM with the full conversation history. If the LLM responds with a tool call (i.e., it needs inventory data), the response is appended to messages and the node returns without sending TTS, allowing the graph to route to `retrieve_node`. If the response is plain text, it is streamed directly to TTS.
- **`retrieve_node`**: Executes the `search_inventory` tool call. Sends a brief "Let me check our inventory..." phrase to TTS immediately so the user gets audio feedback during retrieval. Appends the tool result as a `ToolMessage` and returns to `generate_node` for the final LLM response.
- **`search_inventory`**: A LangChain `@tool` that calls `SimpleRetriever.retrieve()`. It handles the case where a named model is not in stock by returning alternative suggestions rather than an empty result.
- **`should_retrieve`**: An edge condition function. Checks if the last message in state contains tool calls; if so, routes to `retrieve_node`, otherwise routes to `END`.

#### `llm.py`

Configures the Groq LLM client (`langchain-groq`) and defines the system prompt. The system prompt instructs the agent to role-play as a Genesis car salesperson, use specific tool call patterns (e.g. always providing a meaningful query string), and handle STT transcription quirks like phonetic model names ("g ninety" for G90).

#### `retriever.py`

`SimpleRetriever` implements a two-level hybrid retrieval strategy:

1. **Redis fast-path**: If the user asks for a specific car name or price filter with no descriptive query, the retriever searches Redis keys directly using a full scan of `car:*` keys. This avoids a ChromaDB round-trip entirely for the most common queries.
2. **ChromaDB semantic + Redis enrichment**: For descriptive queries, performs a vector similarity search in ChromaDB, then enriches each result by fetching the matching `car:{id}` key from Redis (which contains live pricing and availability from the Cache Sync Service). Results sourced via this path carry `"source": "redis+chroma"` in their metadata.

The retriever also handles soft-deleted cars (`available == null`) by filtering them from results, and gracefully falls back to ChromaDB-only results if Redis is unavailable.

#### `chroma_service.py`

A thin wrapper around the `chromadb` client. Initialises the cloud ChromaDB connection using credentials from `settings` and provides `get_collection()` and `check_chroma_service()` helpers.

---

### `src/services/`

#### `stt.py`

`DeepgramSTT` opens a native `websockets` WebSocket directly to the Deepgram Streaming API (model: `nova-3`, language: `en-US`, `vad_events=true`). It bypasses the synchronous Deepgram Python SDK to avoid threading deadlocks in an async context.

A background `listen_loop` coroutine continuously reads messages from the Deepgram connection and dispatches them:
- `SpeechStarted` messages (VAD events): calls `on_speech_started_callback`. This is the primary barge-in trigger, firing 50-150 ms after voice onset.
- `is_final=False` (interim transcripts): calls `on_interim_callback` as a secondary barge-in fallback.
- `is_final=True` (final transcripts): calls `on_transcript_callback` with the completed utterance text.

The module exposes a `send` coroutine (to forward audio bytes from the browser to Deepgram) and a `close` coroutine for clean shutdown.

#### `tts.py`

`DeepgramTTS` sends text to Deepgram's HTTP TTS API (model: `aura-2-thalia-en`, `linear16` PCM at 16 kHz) and streams the raw audio bytes back to the browser WebSocket in 4 KB chunks.

Barge-in is implemented by checking `stop_event.is_set()` before sending each chunk. When the event is set (triggered by `session.interrupt()`), the method returns `False` immediately, stopping audio delivery mid-stream without waiting for the current HTTP response to finish.

---

### `src/core/`

#### `config.py`

Loads `voice-ai/.env` using `python-dotenv` and exposes all configuration values through a `Settings` class singleton (`settings`). All other modules import from this singleton rather than calling `os.getenv` directly.

#### `cache.py`

Manages the Redis connection pool (`redis.asyncio`) and the startup cache warm-up.

On startup, `warm_up_cache()` queries ChromaDB for three common vehicle sets (budget vehicles under 250,000 SAR, GV80 models, and G90 models), deduplicates results, and writes them all to Redis as `car:{id}` JSON keys with a 24-hour TTL. This is the only point in the entire Voice AI service where Redis is written to; all subsequent updates are handled by the separate ingestion Cache Sync Service.

After startup, the Voice AI treats Redis as strictly read-only.

#### `session.py`

`Session` holds all state for one WebSocket connection:
- `messages`: the conversation history as a list of LangChain message objects (system prompt + rolling window of the last 10 turn-pairs).
- `tts_stop_event`: an `asyncio.Event` shared with the TTS service. Setting it aborts audio delivery.
- `agent_task`: a reference to the currently running `run_agent` asyncio task.
- `_generation`: a monotonically incrementing counter used for race-condition protection. Incremented on every `interrupt()`. An agent task that finishes late will compare its captured generation against the current one before writing to `messages`, and will discard its result if they differ.

`SessionManager` is a process-level singleton that maps session IDs to `Session` objects, creating and removing them as connections open and close.

#### `health.py`

Performs startup health checks against both Redis and ChromaDB before accepting connections. Retries up to three times with a short delay between attempts. If checks do not pass, the FastAPI lifespan raises `RuntimeError` and the server does not start.

#### `logger.py`

A fully async, non-blocking logging system. All `logging.*` calls enqueue `LogRecord` objects into an `asyncio.Queue`. A background `_flush_worker` coroutine drains the queue and writes records to `voice-ai/logs/voice_ai.log` in batches (every 60 seconds or when 500 records accumulate, whichever comes first). The file handler uses UTF-8 encoding to handle special characters.

A `_watch_and_recover` coroutine runs alongside the flush worker. If the flush worker crashes, the watcher detects the failure, logs the exception to stderr, and restarts the worker automatically (up to 10 attempts).

No `StreamHandler` is installed, so nothing is printed to the terminal during normal operation.

---

## Logs

Logs are written exclusively to the `voice-ai/logs/` directory. The file is created automatically on first startup. To monitor the running application:

```bash
# PowerShell
Get-Content .\logs\voice_ai.log -Wait

# Unix
tail -f logs/voice_ai.log
```

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Native `websockets` for STT instead of the Deepgram SDK | The SDK uses synchronous threading internally, which causes deadlocks when mixed with `asyncio`. |
| VAD `SpeechStarted` events for barge-in | Fires 50-150 ms after voice onset, before any words are recognised, giving the lowest possible interrupt latency. |
| Generation counter for agent tasks | Prevents a slow LLM response from overwriting a response that was already committed for a newer user turn. |
| Redis warm-up on startup | Pre-populates the most common vehicle queries so the first user requests are served from Redis without a ChromaDB round-trip. |
| Voice AI never writes Redis post-startup | All live cache updates are delegated to the Cache Sync Service (ingestion pipeline), maintaining a clean separation of concerns. |
| Async batch logging | Log writes are decoupled from the hot path. The application never blocks on file I/O regardless of log volume. |
