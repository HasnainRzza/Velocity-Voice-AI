# Conversational Voice Agent

A fully conversational AI agent utilizing Deepgram for fast Speech-to-Text (STT) and Text-to-Speech (TTS), Groq for high-speed LLM inference, and ChromaDB for RAG-based inventory hybrid search.

## Features
- **Continuous Listening**: Hands-free interactions.
- **Barge-in / Interruptions**: Speak over the agent and it will stop instantly.
- **RAG Integration**: Leverages ChromaDB for metadata and semantic search over a car inventory.
- **Persuasive Persona**: Role-plays as an engaging Genesis car salesperson.

## Setup Instructions

### 1. Prerequisites
- [uv](https://github.com/astral-sh/uv) (Extremely fast Python package installer)
- Python 3.10+

### 2. Install Dependencies
Use `uv` to install the requirements:
```bash
uv pip install -r requirements.txt
```

### 3. Configuration
Create a `.env` file in the root directory (alongside this README) and add your API keys:
```env
DEEPGRAM_STT=your_deepgram_api_key_here
DEEPGRAM_TTS=your_deepgram_api_key_here
GROQ_API_KEY=your_groq_api_key_here
```

### 4. Run the Agent
Run the main orchestrator script:
```bash
uv run python src/main.py
```

Once started, just start speaking into your microphone. The agent will listen, retrieve context if necessary (e.g., car prices), and speak back to you!