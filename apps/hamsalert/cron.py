"""
Scheduled tasks for the hamsalert app.

These functions are called by the scheduler module (apps/hamsalert/scheduler.py)
which runs in a background thread. Tasks can also be run manually via
management commands (e.g., python manage.py cleanup_weather_records).
"""

from datetime import timedelta

from django.conf import settings
from django.utils import timezone


def cleanup_weather_records():
    """
    Delete old WeatherRecord entries.

    Runs daily and deletes records older than WEATHER_DB_CLEANUP_DAYS (default 30).
    """
    from apps.hamsalert.models import WeatherRecord

    days = getattr(settings, 'WEATHER_DB_CLEANUP_DAYS', 30)
    cutoff = timezone.now() - timedelta(days=days)

    queryset = WeatherRecord.objects.filter(fetched_at__lt=cutoff)
    deleted, _ = queryset.delete()

    # Log the result
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f'Cleanup: deleted {deleted} weather records older than {days} days')

    return deleted
