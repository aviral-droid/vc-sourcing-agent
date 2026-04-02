"""
Global Groq rate limiter — shared across all source modules.

Groq free tier: 30 req/min across all models.
We pace at 1 request every 2.5s (= 24/min) leaving headroom.
"""
import threading
import time

_lock = threading.Lock()
_last_call: float = 0.0
MIN_INTERVAL = 2.5  # seconds between Groq calls


def groq_wait() -> None:
    """Call this immediately before every Groq API request."""
    global _last_call
    with _lock:
        now = time.monotonic()
        gap = now - _last_call
        if gap < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - gap)
        _last_call = time.monotonic()
