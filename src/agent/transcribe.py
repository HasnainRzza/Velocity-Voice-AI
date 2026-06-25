import os
import threading
import queue
from pathlib import Path

from dotenv import load_dotenv
from deepgram import DeepgramClient
from deepgram.core.events import EventType

try:
    import pyaudio
except ImportError:
    pyaudio = None

load_dotenv(Path(__file__).resolve().parent / ".env")

DEEPGRAM_STT = os.getenv("DEEPGRAM_STT")
RATE = 16000
CHUNK_SIZE = 1024


class Transcriber:
    def __init__(self, interrupt_event: threading.Event):
        if not DEEPGRAM_STT:
            raise RuntimeError("DEEPGRAM_STT is not set. Please add it to your .env file.")
        if pyaudio is None:
            raise RuntimeError("pyaudio is not installed.")

        self.interrupt_event = interrupt_event
        self.transcript_queue = queue.Queue()
        self.ignore_mode = False
        self.stop_event = threading.Event()
        
        self.deepgram = DeepgramClient(api_key=DEEPGRAM_STT)
        self.connection = None
        self.mic_thread = None
        # Deepgram python SDK passes dicts or objects, so we need to safely extract
        
    def start(self):
        def listening_thread():
            try:
                with self.deepgram.listen.v1.connect(
                    model="nova-3",
                    language="en-US",
                    interim_results=True,
                    encoding="linear16",
                    sample_rate=RATE,
                ) as connection:
                    self.connection = connection
                    
                    def on_message(message, **kwargs) -> None:
                        # We receive message objects here.
                        try:
                            is_final = getattr(message, "is_final", False)
                            speech_final = getattr(message, "speech_final", False)
                            
                            channel = getattr(message, "channel", None)
                            if not channel:
                                return
                            alternatives = getattr(channel, "alternatives", [])
                            if not alternatives:
                                return
                            
                            transcript = getattr(alternatives[0], "transcript", "")
                            
                            if transcript and not self.ignore_mode:
                                # If we got any transcript (interim or final), user is speaking -> Interrupt TTS
                                self.interrupt_event.set()
                                
                                if is_final and transcript.strip():
                                    print(f"\n[Transcribed]: {transcript}")
                                    self.transcript_queue.put(transcript.strip())
                        except Exception as e:
                            print(f"Error parsing message: {e}")

                    self.connection.on(EventType.MESSAGE, on_message)
                    self.connection.on(EventType.ERROR, lambda error, **kwargs: print(f"STT Error: {error}"))
                    
                    self.connection.start_listening()
                    
                    # Keep context manager open
                    self.stop_event.wait()
            except Exception as e:
                print(f"Error starting listening: {e}")
                
        self.listen_thread = threading.Thread(target=listening_thread, daemon=True)
        self.listen_thread.start()
        
        import time
        time.sleep(1.0) # wait for connection to establish
        
        self.mic_thread = threading.Thread(target=self._microphone_thread, daemon=True)
        self.mic_thread.start()
        print("Microphone STT started...")

    def _microphone_thread(self):
        audio = pyaudio.PyAudio()
        stream = audio.open(
            format=pyaudio.paInt16,
            channels=1,
            rate=RATE,
            input=True,
            frames_per_buffer=CHUNK_SIZE,
        )
        try:
            while not self.stop_event.is_set():
                data = stream.read(CHUNK_SIZE, exception_on_overflow=False)
                if self.connection:
                    self.connection.send_media(data)
        except Exception as exc:
            print(f"Error in microphone thread: {exc}")
        finally:
            stream.stop_stream()
            stream.close()
            audio.terminate()
            
    def set_ignore_mode(self, ignore: bool):
        self.ignore_mode = ignore
        if ignore:
            # Clear pending transcripts
            while not self.transcript_queue.empty():
                try:
                    self.transcript_queue.get_nowait()
                except queue.Empty:
                    break

    def get_transcript(self, timeout=None) -> str:
        """Blocks until a transcript is available, then returns it."""
        try:
            return self.transcript_queue.get(timeout=timeout)
        except queue.Empty:
            return ""
            
    def stop(self):
        self.stop_event.set()
        if self.mic_thread:
            self.mic_thread.join(timeout=2.0)
        if self.connection and hasattr(self.connection, "finish"):
            self.connection.finish()

if __name__ == "__main__":
    import time
    ev = threading.Event()
    t = Transcriber(ev)
    t.start()
    try:
        while True:
            text = t.get_transcript(timeout=1.0)
            if text:
                print(f"Got final: {text}")
    except KeyboardInterrupt:
        t.stop()
