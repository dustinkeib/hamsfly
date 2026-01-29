"""
Background Weather Poller

Polls weather APIs on schedule and updates the database.
Views read from DB only - they never call APIs directly.

Poll intervals (TTLs):
- METAR: 30 min (day 0 only)
- TAF: 1 hour (days 0-1)
- NWS: 2 hours (days 2-7)
- OpenMeteo: 4 hours (days 0-15)
- Hourly: 4 hours (days 0-15)
"""

import logging
import threading
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from django.conf import settings

logger = logging.getLogger(__name__)

_started = False
_lock = threading.Lock()

# Poll intervals in seconds
METAR_INTERVAL = 1800      # 30 min
TAF_INTERVAL = 3600        # 1 hour
NWS_INTERVAL = 7200        # 2 hours
OPENMETEO_INTERVAL = 14400 # 4 hours

# Delay between API calls to avoid rate limiting (seconds)
API_CALL_DELAY = 2

# Backoff time when rate limited (seconds) - 10 minutes to let Open-Meteo cool down
RATE_LIMIT_BACKOFF = 600


class WeatherPoller:
    """Background service that polls weather APIs on schedule."""

    def __init__(self):
        from .services import WeatherService
        self.service = WeatherService()
        self.local_timezone = ZoneInfo(
            getattr(settings, 'WEATHER_LOCAL_TIMEZONE', 'America/Los_Angeles')
        )
        self.last_poll = {
            'metar': None,
            'taf': None,
            'nws': None,
            'openmeteo': None,
        }
        self._rate_limited_until = 0  # timestamp when rate limit expires

    def run(self):
        """Main loop - runs forever, polling on TTL intervals."""
        # Initial delay to let Django fully start
        time.sleep(10)

        # Run initial poll for all sources immediately
        logger.info("WeatherPoller: Starting initial poll of all sources")
        self._poll_all_sources()

        # Then switch to TTL-based scheduling
        while True:
            if not self._is_rate_limited():
                self._poll_if_due('metar', METAR_INTERVAL)
                self._poll_if_due('taf', TAF_INTERVAL)
                self._poll_if_due('nws', NWS_INTERVAL)
                self._poll_if_due('openmeteo', OPENMETEO_INTERVAL)
            time.sleep(60)  # Check every minute

    def _poll_all_sources(self):
        """Poll all sources immediately (used on startup), skipping if fresh data exists."""
        local_today = datetime.now(self.local_timezone).date()
        for source in ['metar', 'taf', 'nws', 'openmeteo']:
            if self._is_rate_limited():
                logger.info("WeatherPoller: Rate limited, stopping initial poll")
                break
            # Skip if we already have fresh data (avoids hitting rate limits on restart)
            if self._has_fresh_data(source, local_today):
                self.last_poll[source] = time.time()
                continue
            self._poll_source(source)
            # Only mark as polled if we didn't get rate limited during the poll
            if not self._is_rate_limited():
                self.last_poll[source] = time.time()

    def _poll_if_due(self, source: str, interval: int):
        """Poll if interval elapsed since last poll."""
        now = time.time()
        if self.last_poll[source] is None or (now - self.last_poll[source]) >= interval:
            self._poll_source(source)
            # Only mark as polled if we didn't get rate limited during the poll
            if not self._is_rate_limited():
                self.last_poll[source] = now

    def _is_rate_limited(self) -> bool:
        """Check if we're currently in a rate limit backoff period."""
        return time.time() < self._rate_limited_until

    def _set_rate_limited(self):
        """Set rate limit backoff."""
        self._rate_limited_until = time.time() + RATE_LIMIT_BACKOFF
        logger.warning(f"WeatherPoller: Rate limited, backing off for {RATE_LIMIT_BACKOFF}s")

    def _has_fresh_data(self, source: str, local_today: date) -> bool:
        """Check if DB already has fresh data for this source."""
        from apps.hamsalert.models import WeatherRecord
        from django.utils import timezone

        # Map source to weather_type and TTL
        ttl_map = {
            'metar': ('metar', METAR_INTERVAL),
            'taf': ('taf', TAF_INTERVAL),
            'nws': ('nws', NWS_INTERVAL),
            'openmeteo': ('openmeteo', OPENMETEO_INTERVAL),
        }
        weather_type, ttl = ttl_map.get(source, (source, 3600))

        # Check if we have a record within the TTL
        cutoff = timezone.now() - timedelta(seconds=ttl)

        # For openmeteo, check BOTH daily and hourly (need both to skip)
        if source == 'openmeteo':
            has_daily = WeatherRecord.objects.filter(
                weather_type='openmeteo',
                target_date=local_today,
                fetched_at__gte=cutoff,
            ).exists()
            has_hourly = WeatherRecord.objects.filter(
                weather_type='hourly',
                target_date=local_today,
                fetched_at__gte=cutoff,
            ).exists()
            if has_daily and has_hourly:
                logger.debug(f"WeatherPoller: Fresh openmeteo+hourly data exists, skipping poll")
                return True
            return False

        exists = WeatherRecord.objects.filter(
            weather_type=weather_type,
            target_date=local_today,
            fetched_at__gte=cutoff,
        ).exists()

        if exists:
            logger.debug(f"WeatherPoller: Fresh {source} data exists, skipping poll")
        return exists

    def _poll_source(self, source: str):
        """Fetch and store data for all applicable days."""
        local_today = datetime.now(self.local_timezone).date()

        try:
            if source == 'metar':
                self._poll_metar(local_today)
            elif source == 'taf':
                self._poll_taf(local_today)
            elif source == 'nws':
                self._poll_nws(local_today)
            elif source == 'openmeteo':
                self._poll_openmeteo(local_today)
        except Exception as e:
            logger.exception(f"WeatherPoller: Error polling {source}: {e}")

    def _poll_metar(self, local_today: date):
        """Poll METAR for day 0."""
        logger.info("WeatherPoller: Polling METAR")
        try:
            data = self.service._fetch_metar_from_api(self.service.default_station)
            if data:
                self.service._save_to_db(
                    'metar',
                    local_today,
                    self.service._serialize_metar_data(data),
                    station=self.service.default_station,
                )
                logger.info("WeatherPoller: METAR updated")
        except Exception as e:
            logger.warning(f"WeatherPoller: METAR poll failed: {e}")

    def _poll_taf(self, local_today: date):
        """Poll TAF for days 0-1."""
        logger.info("WeatherPoller: Polling TAF")
        for days_out in range(2):  # Days 0 and 1
            target_date = local_today + timedelta(days=days_out)
            try:
                data = self.service._fetch_taf_from_api(
                    self.service.default_station, target_date
                )
                if data:
                    self.service._save_to_db(
                        'taf',
                        target_date,
                        self.service._serialize_taf_data(data),
                        station=self.service.default_station,
                    )
                    logger.debug(f"WeatherPoller: TAF updated for {target_date}")
            except Exception as e:
                logger.warning(f"WeatherPoller: TAF poll failed for {target_date}: {e}")
            time.sleep(API_CALL_DELAY)
        logger.info("WeatherPoller: TAF updated")

    def _poll_nws(self, local_today: date):
        """Poll NWS for days 2-7."""
        logger.info("WeatherPoller: Polling NWS")
        lat, lon = self.service.nws_location
        for days_out in range(2, 8):  # Days 2-7
            target_date = local_today + timedelta(days=days_out)
            try:
                data = self.service._fetch_nws_forecast(target_date)
                if data:
                    self.service._save_to_db(
                        'nws',
                        target_date,
                        self.service._serialize_nws_data(data),
                        lat=lat,
                        lon=lon,
                    )
                    logger.debug(f"WeatherPoller: NWS updated for {target_date}")
            except Exception as e:
                logger.warning(f"WeatherPoller: NWS poll failed for {target_date}: {e}")
            time.sleep(API_CALL_DELAY)
        logger.info("WeatherPoller: NWS updated")

    def _poll_openmeteo(self, local_today: date):
        """Poll OpenMeteo for days 0-15, including hourly data (2 API calls total)."""
        logger.info("WeatherPoller: Polling OpenMeteo (batch)")
        lat, lon = self.service.nws_location

        # Daily forecast for days 0-15 (ONE API call)
        try:
            results = self.service.fetch_openmeteo_batch(local_today)
            for target_date, data in results:
                self.service._save_to_db(
                    'openmeteo',
                    target_date,
                    self.service._serialize_openmeteo_data(data),
                    lat=lat,
                    lon=lon,
                )
            logger.info(f"WeatherPoller: OpenMeteo daily updated ({len(results)} days)")
        except Exception as e:
            error_str = str(e).lower()
            if 'rate limit' in error_str or '429' in error_str:
                self._set_rate_limited()
                return
            logger.warning(f"WeatherPoller: OpenMeteo daily poll failed: {e}")

        time.sleep(API_CALL_DELAY)

        # Hourly forecast for days 0-15 (ONE API call)
        try:
            results = self.service.fetch_hourly_batch(local_today, days=16)
            for target_date, data in results:
                self.service._save_to_db(
                    'hourly',
                    target_date,
                    self.service._serialize_hourly_data(data),
                    lat=lat,
                    lon=lon,
                )
            logger.info(f"WeatherPoller: OpenMeteo hourly updated ({len(results)} days)")
        except Exception as e:
            error_str = str(e).lower()
            if 'rate limit' in error_str or '429' in error_str:
                self._set_rate_limited()
                return
            logger.warning(f"WeatherPoller: Hourly poll failed: {e}")

        logger.info("WeatherPoller: OpenMeteo updated")


def _poller_loop():
    """Entry point for poller thread."""
    poller = WeatherPoller()
    poller.run()


def start():
    """Start the weather poller thread."""
    global _started

    with _lock:
        if _started:
            return
        _started = True

    thread = threading.Thread(target=_poller_loop, daemon=True)
    thread.start()
    logger.info("WeatherPoller started")
