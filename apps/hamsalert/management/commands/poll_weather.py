"""
Management command to manually poll weather sources.

Usage:
    python manage.py poll_weather                    # Poll all sources
    python manage.py poll_weather --source metar    # Poll specific source
    python manage.py poll_weather --source openmeteo --source hourly  # Multiple sources
"""

import time
from datetime import datetime, timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from zoneinfo import ZoneInfo

VALID_SOURCES = ['all', 'metar', 'taf', 'nws', 'openmeteo', 'hourly', 'historical']


class Command(BaseCommand):
    help = 'Poll weather APIs manually'

    def add_arguments(self, parser):
        parser.add_argument(
            '--source',
            action='append',
            choices=VALID_SOURCES,
            help='Source(s) to poll. Can be specified multiple times. Default: all',
        )
        parser.add_argument(
            '--historical-days',
            type=int,
            default=7,
            help='Number of past days to fetch for historical data (default: 7)',
        )

    def handle(self, *args, **options):
        from apps.hamsalert.services import WeatherService

        sources = options['source'] or ['all']
        historical_days = options['historical_days']

        if 'all' in sources:
            sources = ['metar', 'taf', 'nws', 'openmeteo', 'hourly']

        service = WeatherService()
        local_tz = ZoneInfo(getattr(settings, 'WEATHER_LOCAL_TIMEZONE', 'America/Los_Angeles'))
        local_today = datetime.now(local_tz).date()
        lat, lon = service.nws_location

        for source in sources:
            self.stdout.write(f'Polling {source}...')
            try:
                if source == 'metar':
                    self._poll_metar(service, local_today)
                elif source == 'taf':
                    self._poll_taf(service, local_today)
                elif source == 'nws':
                    self._poll_nws(service, local_today, lat, lon)
                elif source == 'openmeteo':
                    self._poll_openmeteo(service, local_today, lat, lon)
                elif source == 'hourly':
                    self._poll_hourly(service, local_today, lat, lon)
                elif source == 'historical':
                    self._poll_historical(service, local_today, lat, lon, historical_days)
                self.stdout.write(self.style.SUCCESS(f'  {source} done'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'  {source} failed: {e}'))

        self.stdout.write(self.style.SUCCESS('Poll complete'))

    def _poll_metar(self, service, local_today):
        """Poll METAR for day 0."""
        data = service._fetch_metar_from_api(service.default_station)
        if data:
            service._save_to_db(
                'metar',
                local_today,
                service._serialize_metar_data(data),
                station=service.default_station,
            )
            self.stdout.write(f'    METAR saved for {local_today}')

    def _poll_taf(self, service, local_today):
        """Poll TAF for days 0-1."""
        for days_out in range(2):
            target_date = local_today + timedelta(days=days_out)
            data = service._fetch_taf_from_api(service.default_station, target_date)
            if data:
                service._save_to_db(
                    'taf',
                    target_date,
                    service._serialize_taf_data(data),
                    station=service.default_station,
                )
                self.stdout.write(f'    TAF saved for {target_date}')
            time.sleep(1)

    def _poll_nws(self, service, local_today, lat, lon):
        """Poll NWS for days 2-7."""
        for days_out in range(2, 8):
            target_date = local_today + timedelta(days=days_out)
            data = service._fetch_nws_forecast(target_date)
            if data:
                service._save_to_db(
                    'nws',
                    target_date,
                    service._serialize_nws_data(data),
                    lat=lat,
                    lon=lon,
                )
                self.stdout.write(f'    NWS saved for {target_date}')
            time.sleep(1)

    def _poll_openmeteo(self, service, local_today, lat, lon):
        """Poll OpenMeteo daily for days 0-15 (batch)."""
        results = service.fetch_openmeteo_batch(local_today)
        for target_date, data in results:
            service._save_to_db(
                'openmeteo',
                target_date,
                service._serialize_openmeteo_data(data),
                lat=lat,
                lon=lon,
            )
        self.stdout.write(f'    OpenMeteo daily saved ({len(results)} days)')

    def _poll_hourly(self, service, local_today, lat, lon):
        """Poll OpenMeteo hourly for days 0-15 (batch)."""
        results = service.fetch_hourly_batch(local_today, days=16)
        for target_date, data in results:
            service._save_to_db(
                'hourly',
                target_date,
                service._serialize_hourly_data(data),
                lat=lat,
                lon=lon,
            )
        self.stdout.write(f'    Hourly saved ({len(results)} days)')

    def _poll_historical(self, service, local_today, lat, lon, days):
        """Poll historical weather for past N days."""
        for days_ago in range(1, days + 1):
            target_date = local_today - timedelta(days=days_ago)
            try:
                data = service._fetch_historical_weather(target_date)
                if data:
                    service._save_to_db(
                        'historical',
                        target_date,
                        service._serialize_historical_data(data),
                        lat=lat,
                        lon=lon,
                    )
                    self.stdout.write(f'    Historical saved for {target_date}')
            except Exception as e:
                self.stdout.write(self.style.WARNING(f'    Historical failed for {target_date}: {e}'))
            time.sleep(1)
