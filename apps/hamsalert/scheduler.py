"""
Simple task scheduler using background threads.

Runs alongside the keepalive thread to execute periodic tasks like cleanup.
Uses the database to track last run times to prevent duplicate runs across restarts.
"""

import logging
import threading
from datetime import datetime, timedelta

from django.conf import settings
from django.core.cache import cache
from django.utils import timezone

logger = logging.getLogger(__name__)

CHECK_INTERVAL = 3600  # Check every hour

_started = False
_lock = threading.Lock()


def get_last_run(task_name):
    """Get the last run time for a task from cache/DB."""
    cache_key = f'scheduler_last_run_{task_name}'
    return cache.get(cache_key)


def set_last_run(task_name, run_time=None):
    """Record when a task was last run."""
    if run_time is None:
        run_time = timezone.now()
    cache_key = f'scheduler_last_run_{task_name}'
    # Store for 48 hours (well beyond the daily interval)
    cache.set(cache_key, run_time, 48 * 3600)
    logger.debug("Recorded last run for %s: %s", task_name, run_time)


def should_run_today(task_name, target_hour):
    """Check if a daily task should run now."""
    now = timezone.localtime()
    current_hour = now.hour

    # Only run at or after the target hour
    if current_hour < target_hour:
        return False

    last_run = get_last_run(task_name)
    if last_run is None:
        return True

    # Check if last run was before today's target time
    last_run_local = timezone.localtime(last_run)
    today_target = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)

    return last_run_local < today_target


def run_cleanup_weather_records():
    """Execute the weather records cleanup task."""
    from apps.hamsalert.cron import cleanup_weather_records

    task_name = 'cleanup_weather_records'
    target_hour = getattr(settings, 'SCHEDULER_CLEANUP_HOUR', 3)

    if not should_run_today(task_name, target_hour):
        return

    logger.info("Scheduler: starting %s", task_name)
    try:
        deleted = cleanup_weather_records()
        set_last_run(task_name)
        logger.info("Scheduler: completed %s (deleted %d records)", task_name, deleted)
    except Exception:
        logger.exception("Scheduler: %s failed", task_name)


def _scheduler_loop():
    """Main scheduler loop - checks periodically if tasks need to run."""
    event = threading.Event()

    # Initial delay to let Django fully start
    event.wait(60)

    while not event.wait(CHECK_INTERVAL):
        try:
            run_cleanup_weather_records()
        except Exception:
            logger.exception("Scheduler loop error")


def start():
    """Start the scheduler thread."""
    global _started

    with _lock:
        if _started:
            return
        _started = True

    thread = threading.Thread(target=_scheduler_loop, daemon=True)
    thread.start()
    logger.info("Scheduler started: checking tasks every %ds", CHECK_INTERVAL)
