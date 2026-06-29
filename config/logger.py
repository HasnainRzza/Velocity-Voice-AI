import logging
import logging.handlers
from queue import SimpleQueue
from config.settings import PROJECT_ROOT
import atexit

LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

def setup_async_logging(log_file_name="ingestion.log", level=logging.INFO):
    """
    Sets up a root logger that writes logs to a file using an async QueueListener.
    This avoids blocking the main thread during heavy I/O and removes terminal output.
    """
    root_logger = logging.getLogger()
    
    # Remove all existing handlers (like terminal output)
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
        
    root_logger.setLevel(level)
    
    log_file = LOGS_DIR / log_file_name
    
    # Create the file handler
    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    file_handler.setFormatter(formatter)
    
    # Async Queue Setup
    log_queue = SimpleQueue()
    queue_handler = logging.handlers.QueueHandler(log_queue)
    
    # Queue listener writes from queue to file_handler on a background thread
    listener = logging.handlers.QueueListener(log_queue, file_handler, respect_handler_level=True)
    listener.start()
    
    # Ensure listener stops gracefully on exit
    atexit.register(listener.stop)
    
    # Add queue handler to root logger
    root_logger.addHandler(queue_handler)
