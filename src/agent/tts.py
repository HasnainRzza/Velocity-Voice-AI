import os
import threading
import sounddevice as sd
from dotenv import load_dotenv
from deepgram import DeepgramClient

load_dotenv()

API_KEY = os.getenv("DEEPGRAM_TTS")
deepgram = DeepgramClient(api_key=API_KEY) if API_KEY else None

def speak(text: str, interrupt_event: threading.Event = None) -> None:
    if not deepgram:
        print(f"TTS offline. Would say: {text}")
        return

    try:
        audio_stream = deepgram.speak.v1.audio.generate(
            text=text,
            model="aura-2-thalia-en",
            encoding="linear16",
            sample_rate=24000,
        )

        stream = sd.RawOutputStream(
            samplerate=24000,
            channels=1,
            dtype="int16",
        )
        
        # Buffer some audio to prevent stuttering
        buffer = []
        buffer_size = 0
        min_buffer_size = 24000 * 2 # 1 second of 16-bit audio (2 bytes per sample)
        
        for chunk in audio_stream:
            buffer.append(chunk)
            buffer_size += len(chunk)
            if buffer_size >= min_buffer_size:
                break
                
        stream.start()
        
        # Play the buffered audio first
        for chunk in buffer:
            if interrupt_event and interrupt_event.is_set():
                print("\n[TTS Interrupted by User]")
                stream.stop()
                stream.close()
                return
            stream.write(chunk)
            
        # Continue playing the rest of the stream
        for chunk in audio_stream:
            if interrupt_event and interrupt_event.is_set():
                print("\n[TTS Interrupted by User]")
                break
            stream.write(chunk)
            
        stream.stop()
        stream.close()
    except Exception as e:
        print(f"\n[TTS Error: {e}]")

if __name__ == "__main__":
    speak("Hello, this is a test.", threading.Event())