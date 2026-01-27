"""Self-ping keepalive to prevent Render free tier from sleeping."""

import logging
import os
import threading

import httpx

logger = logging.getLogger(__name__)

PING_INTERVAL = 600  # 10 minutes (Render sleeps after 15 min idle)

_started = False
_lock = threading.Lock()


def _ping_loop(url):
    """Ping the health endpoint in a loop."""
    event = threading.Event()
    while not event.wait(PING_INTERVAL):
        try:
            response = httpx.get(url, timeout=10)
            logger.debug("Keepalive ping %s -> %s", url, response.status_code)
        except Exception:
            logger.warning("Keepalive ping failed for %s", url, exc_info=True)


def start():
    """Start the keepalive thread if running on Render."""
    global _started

    base_url = os.environ.get('RENDER_EXTERNAL_URL')
    if not base_url:
        return

    with _lock:
        if _started:
            return
        _started = True

    url = f"{base_url}/health/"
    thread = threading.Thread(target=_ping_loop, args=(url,), daemon=True)
    thread.start()
    logger.info("Keepalive started: pinging %s every %ds", url, PING_INTERVAL)
