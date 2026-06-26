import os
import httpx
import pyaudio
import asyncio
import threading
import subprocess
from dotenv import load_dotenv

load_dotenv()

async def speak_async(text: str, interrupt_event: threading.Event = None):
    """
    Synthesize text to speech using Deepgram in raw PCM format (16000Hz, 16-bit, mono) 
    and streams it directly to PyAudio chunk-by-chunk for ultra-low latency.
    """
    api_key = os.getenv("DEEPGRAM_TTS")
    if not api_key:
        print("⚠️ DEEPGRAM_TTS is not set. Falling back to local TTS.", flush=True)
        speak_locally(text)
        return

    url = "https://api.deepgram.com/v1/speak"
    params = {
        "model": "aura-2-thalia-en",
        "encoding": "linear16",
        "sample_rate": "16000"
    }
    headers = {
        "Authorization": f"Token {api_key}",
        "Content-Type": "application/json"
    }
    data = {"text": text}

    from agent.transcribe import global_pyaudio
    import pyaudio
    if global_pyaudio is None:
        speak_locally(text)
        return

    stream = global_pyaudio.open(format=pyaudio.paInt16, channels=1, rate=16000, output=True)

    try:
        async with httpx.AsyncClient() as client:
            async with client.stream("POST", url, params=params, headers=headers, json=data) as response:
                if response.status_code == 200:
                    async for chunk in response.aiter_bytes(chunk_size=4096):
                        # If user interrupts by speaking, immediately halt playback!
                        if interrupt_event and interrupt_event.is_set():
                            break
                        stream.write(chunk)
                else:
                    text_err = await response.aread()
                    print(f"⚠️ Deepgram PCM TTS failed: {text_err}", flush=True)
                    speak_locally(text)
    except Exception as e:
        print(f"⚠️ Deepgram TTS failed: {e}. Falling back to local.", flush=True)
        speak_locally(text)
    finally:
        stream.stop_stream()
        stream.close()

def speak(text: str, interrupt_event: threading.Event = None):
    """Synchronous wrapper to stream TTS."""
    asyncio.run(speak_async(text, interrupt_event))

def speak_locally(text: str):
    """
    Speak text locally using native Windows SpeechSynthesizer via PowerShell.
    """
    clean_text = text.replace('"', '""').replace("`", "``")
    ps_cmd = f'Add-Type -AssemblyName System.Speech; $synth = New-Object System.Speech.Synthesis.SpeechSynthesizer; $synth.Speak("{clean_text}")'
    try:
        subprocess.run(["powershell", "-Command", ps_cmd], capture_output=True, check=True)
    except Exception as e:
        print(f"⚠️ Local TTS failed: {e}", flush=True)

def shutdown():
    """Placeholder for graceful shutdowns if needed."""
    pass
