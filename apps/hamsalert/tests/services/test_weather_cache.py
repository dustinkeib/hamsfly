"""Tests for weather service database caching."""

from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

from django.test import TestCase, override_settings
from django.utils import timezone

from apps.hamsalert.models import WeatherRecord
from apps.hamsalert.services.weather import (
    CloudLayer,
    HourlyForecastData,
    HourlyForecastEntry,
    HistoricalWeatherData,
    NwsForecastData,
    NwsForecastPeriod,
    OpenMeteoForecastData,
    RateLimitError,
    TafForecastData,
    TafForecastPeriod,
    WeatherData,
    WeatherService,
    WeatherServiceError,
    WeatherSource,
    WindData,
)


class WeatherServiceDBCacheTests(TestCase):
    """Tests for database-level caching in WeatherService."""

    def setUp(self):
        WeatherRecord.objects.all().delete()
        self.service = WeatherService()
        self.test_date = date.today() + timedelta(days=10)
        self.lat, self.lon = self.service.nws_location

    def test_get_from_db_returns_none_when_empty(self):
        result = self.service._get_from_db('openmeteo', self.test_date, lat=self.lat, lon=self.lon)
        self.assertIsNone(result)

    def test_save_and_get_from_db(self):
        test_data = {'test': 'value', 'wind': {'speed': 10}}
        self.service._save_to_db(
            'openmeteo',
            self.test_date,
            test_data,
            lat=self.lat,
            lon=self.lon,
        )

        result = self.service._get_from_db('openmeteo', self.test_date, lat=self.lat, lon=self.lon)
        self.assertEqual(result, test_data)

    def test_get_from_db_respects_max_age(self):
        test_data = {'test': 'old_value'}
        self.service._save_to_db('openmeteo', self.test_date, test_data, lat=self.lat, lon=self.lon)

        # Manually age the record
        WeatherRecord.objects.filter(weather_type='openmeteo').update(
            fetched_at=timezone.now() - timedelta(hours=5)
        )

        # Should not find with max_age of 1 hour
        result = self.service._get_from_db(
            'openmeteo', self.test_date, lat=self.lat, lon=self.lon, max_age_seconds=3600
        )
        self.assertIsNone(result)

        # Should find with no max_age
        result = self.service._get_from_db('openmeteo', self.test_date, lat=self.lat, lon=self.lon)
        self.assertEqual(result, test_data)

    def test_get_from_db_by_station(self):
        test_data = {'station': 'KACV', 'raw': 'METAR data'}
        self.service._save_to_db('metar', self.test_date, test_data, station='KACV')

        result = self.service._get_from_db('metar', self.test_date, station='KACV')
        self.assertEqual(result, test_data)

        # Different station should not match
        result = self.service._get_from_db('metar', self.test_date, station='KJFK')
        self.assertIsNone(result)

    def test_get_from_db_returns_most_recent(self):
        old_data = {'version': 'old'}
        new_data = {'version': 'new'}

        self.service._save_to_db('openmeteo', self.test_date, old_data, lat=self.lat, lon=self.lon)

        # Age the first record
        WeatherRecord.objects.filter(weather_type='openmeteo').update(
            fetched_at=timezone.now() - timedelta(hours=1)
        )

        self.service._save_to_db('openmeteo', self.test_date, new_data, lat=self.lat, lon=self.lon)

        result = self.service._get_from_db('openmeteo', self.test_date, lat=self.lat, lon=self.lon)
        self.assertEqual(result, new_data)

    def test_save_to_db_records_api_response_time(self):
        self.service._save_to_db(
            'openmeteo',
            self.test_date,
            {'test': 'data'},
            lat=self.lat,
            lon=self.lon,
            api_response_time_ms=150,
        )

        record = WeatherRecord.objects.get(weather_type='openmeteo')
        self.assertEqual(record.api_response_time_ms, 150)


class OpenMeteoSerializationTests(TestCase):
    """Tests for OpenMeteo data serialization/deserialization."""

    def setUp(self):
        self.service = WeatherService()
        self.test_date = date.today() + timedelta(days=10)

    def test_serialize_openmeteo_data(self):
        data = OpenMeteoForecastData(
            location=(40.9781, -124.1086),
            target_date=self.test_date,
            wind=WindData(direction=270, speed=10, gust=15, direction_repr='270'),
            temperature_high=22,
            temperature_low=14,
            precipitation_probability=30,
        )

        serialized = self.service._serialize_openmeteo_data(data)

        self.assertEqual(serialized['location'], [40.9781, -124.1086])
        self.assertEqual(serialized['target_date'], self.test_date.isoformat())
        self.assertEqual(serialized['wind']['direction'], 270)
        self.assertEqual(serialized['wind']['speed'], 10)
        self.assertEqual(serialized['wind']['gust'], 15)
        self.assertEqual(serialized['temperature_high'], 22)
        self.assertEqual(serialized['temperature_low'], 14)
        self.assertEqual(serialized['precipitation_probability'], 30)

    def test_deserialize_openmeteo_data(self):
        serialized = {
            'location': [40.9781, -124.1086],
            'target_date': self.test_date.isoformat(),
            'wind': {
                'direction': 270,
                'speed': 10,
                'gust': 15,
                'direction_repr': '270',
            },
            'temperature_high': 22,
            'temperature_low': 14,
            'precipitation_probability': 30,
        }

        data = self.service._deserialize_openmeteo_data(serialized)

        self.assertEqual(data.location, (40.9781, -124.1086))
        self.assertEqual(data.target_date, self.test_date)
        self.assertEqual(data.wind.direction, 270)
        self.assertEqual(data.wind.speed, 10)
        self.assertEqual(data.wind.gust, 15)
        self.assertEqual(data.temperature_high, 22)
        self.assertEqual(data.temperature_low, 14)
        self.assertEqual(data.precipitation_probability, 30)
        self.assertTrue(data.from_cache)

    def test_roundtrip_serialization(self):
        original = OpenMeteoForecastData(
            location=(40.9781, -124.1086),
            target_date=self.test_date,
            wind=WindData(direction=180, speed=5, gust=None, direction_repr='180'),
            temperature_high=25,
            temperature_low=18,
            precipitation_probability=10,
        )

        serialized = self.service._serialize_openmeteo_data(original)
        restored = self.service._deserialize_openmeteo_data(serialized)

        self.assertEqual(restored.location, original.location)
        self.assertEqual(restored.target_date, original.target_date)
        self.assertEqual(restored.wind.direction, original.wind.direction)
        self.assertEqual(restored.wind.speed, original.wind.speed)
        self.assertEqual(restored.wind.gust, original.wind.gust)
        self.assertEqual(restored.temperature_high, original.temperature_high)
        self.assertEqual(restored.temperature_low, original.temperature_low)
        self.assertEqual(restored.precipitation_probability, original.precipitation_probability)


class HourlySerializationTests(TestCase):
    """Tests for Hourly forecast data serialization/deserialization."""

    def setUp(self):
        self.service = WeatherService()
        self.test_date = date.today()

    def test_serialize_hourly_data(self):
        from datetime import datetime
        hour_time = datetime(2024, 1, 15, 10, 0, 0)

        data = HourlyForecastData(
            location=(40.9781, -124.1086),
            target_date=self.test_date,
            hours=[
                HourlyForecastEntry(
                    time=hour_time,
                    temperature_c=18.5,
                    wind_speed_kmh=20.0,
                    wind_direction=270,
                    wind_gusts_kmh=30.0,
                    precipitation_probability=20,
                    weather_code=2,
                ),
            ],
        )

        serialized = self.service._serialize_hourly_data(data)

        self.assertEqual(serialized['location'], [40.9781, -124.1086])
        self.assertEqual(len(serialized['hours']), 1)
        self.assertEqual(serialized['hours'][0]['temperature_c'], 18.5)
        self.assertEqual(serialized['hours'][0]['wind_speed_kmh'], 20.0)

    def test_deserialize_hourly_data(self):
        serialized = {
            'location': [40.9781, -124.1086],
            'target_date': self.test_date.isoformat(),
            'hours': [
                {
                    'time': '2024-01-15T10:00:00',
                    'temperature_c': 18.5,
                    'wind_speed_kmh': 20.0,
                    'wind_direction': 270,
                    'wind_gusts_kmh': 30.0,
                    'precipitation_probability': 20,
                    'weather_code': 2,
                },
            ],
        }

        data = self.service._deserialize_hourly_data(serialized)

        self.assertEqual(data.location, (40.9781, -124.1086))
        self.assertEqual(len(data.hours), 1)
        self.assertEqual(data.hours[0].temperature_c, 18.5)
        self.assertEqual(data.hours[0].wind_speed_kmh, 20.0)
        self.assertTrue(data.from_cache)


class HistoricalSerializationTests(TestCase):
    """Tests for Historical weather data serialization/deserialization."""

    def setUp(self):
        self.service = WeatherService()
        self.test_date = date.today() - timedelta(days=30)

    def test_serialize_historical_data(self):
        data = HistoricalWeatherData(
            location=(40.9781, -124.1086),
            target_date=self.test_date,
            wind=WindData(direction=180, speed=8, gust=12, direction_repr='180'),
            temperature_high=20,
            temperature_low=12,
            precipitation_sum=5.5,
        )

        serialized = self.service._serialize_historical_data(data)

        self.assertEqual(serialized['location'], [40.9781, -124.1086])
        self.assertEqual(serialized['wind']['direction'], 180)
        self.assertEqual(serialized['precipitation_sum'], 5.5)

    def test_deserialize_historical_data(self):
        serialized = {
            'location': [40.9781, -124.1086],
            'target_date': self.test_date.isoformat(),
            'wind': {
                'direction': 180,
                'speed': 8,
                'gust': 12,
                'direction_repr': '180',
            },
            'temperature_high': 20,
            'temperature_low': 12,
            'precipitation_sum': 5.5,
        }

        data = self.service._deserialize_historical_data(serialized)

        self.assertEqual(data.location, (40.9781, -124.1086))
        self.assertEqual(data.wind.speed, 8)
        self.assertEqual(data.precipitation_sum, 5.5)
        self.assertTrue(data.from_cache)


class DBCacheIntegrationTests(TestCase):
    """Integration tests for the DB caching strategy."""

    def setUp(self):
        WeatherRecord.objects.all().delete()
        self.service = WeatherService()
        self.test_date = date.today() + timedelta(days=10)
        self.lat, self.lon = self.service.nws_location

    @patch.object(WeatherService, '_fetch_visualcrossing_daily')
    def test_cache_miss_fetches_from_api_and_stores(self, mock_fetch):
        """On cache miss, should fetch from API and store in DB."""
        mock_data = OpenMeteoForecastData(
            location=(self.lat, self.lon),
            target_date=self.test_date,
            wind=WindData(direction=270, speed=10, gust=None, direction_repr='270'),
            temperature_high=20,
            temperature_low=12,
            precipitation_probability=15,
        )
        mock_fetch.return_value = mock_data

        result = self.service._get_openmeteo_forecast(self.test_date)

        mock_fetch.assert_called_once()
        self.assertEqual(result.wind.speed, 10)

        # Verify stored in DB
        self.assertEqual(WeatherRecord.objects.filter(weather_type='openmeteo').count(), 1)

    @patch.object(WeatherService, '_fetch_visualcrossing_daily')
    def test_db_cache_hit_skips_api(self, mock_fetch):
        """On DB cache hit, should not call API."""
        # Store in DB
        db_data = {
            'location': [self.lat, self.lon],
            'target_date': self.test_date.isoformat(),
            'wind': {'direction': 90, 'speed': 8, 'gust': None, 'direction_repr': '090'},
            'temperature_high': 22,
            'temperature_low': 14,
            'precipitation_probability': 10,
        }
        self.service._save_to_db('openmeteo', self.test_date, db_data, lat=self.lat, lon=self.lon)

        result = self.service._get_openmeteo_forecast(self.test_date)

        mock_fetch.assert_not_called()
        self.assertEqual(result.wind.speed, 8)
        self.assertTrue(result.from_cache)

    @patch.object(WeatherService, '_fetch_visualcrossing_daily')
    def test_stale_db_fallback_on_api_error(self, mock_fetch):
        """On API error, should fall back to stale DB data."""
        # Store stale data in DB
        stale_data = {
            'location': [self.lat, self.lon],
            'target_date': self.test_date.isoformat(),
            'wind': {'direction': 45, 'speed': 12, 'gust': None, 'direction_repr': '045'},
            'temperature_high': 25,
            'temperature_low': 16,
            'precipitation_probability': 20,
        }
        self.service._save_to_db('openmeteo', self.test_date, stale_data, lat=self.lat, lon=self.lon)

        # Age the record beyond TTL
        WeatherRecord.objects.filter(weather_type='openmeteo').update(
            fetched_at=timezone.now() - timedelta(hours=10)
        )

        # Simulate API failure
        mock_fetch.side_effect = WeatherServiceError("API unavailable")

        result = self.service._get_openmeteo_forecast(self.test_date)

        self.assertEqual(result.wind.speed, 12)
        self.assertTrue(result.from_cache)

    @patch.object(WeatherService, '_fetch_visualcrossing_daily')
    def test_api_error_with_no_stale_data_raises(self, mock_fetch):
        """On API error with no stale data, should raise exception."""
        mock_fetch.side_effect = WeatherServiceError("API unavailable")

        with self.assertRaises(WeatherServiceError):
            self.service._get_openmeteo_forecast(self.test_date)

    @patch.object(WeatherService, '_fetch_visualcrossing_daily')
    def test_rate_limit_error_triggers_stale_fallback(self, mock_fetch):
        """RateLimitError causes _get_openmeteo_forecast to return stale DB data."""
        # Store stale data in DB
        stale_data = {
            'location': [self.lat, self.lon],
            'target_date': self.test_date.isoformat(),
            'wind': {'direction': 270, 'speed': 15, 'gust': 20, 'direction_repr': '270'},
            'temperature_high': 22,
            'temperature_low': 14,
            'precipitation_probability': 25,
        }
        self.service._save_to_db('openmeteo', self.test_date, stale_data, lat=self.lat, lon=self.lon)

        # Age the record beyond TTL
        WeatherRecord.objects.filter(weather_type='openmeteo').update(
            fetched_at=timezone.now() - timedelta(hours=10)
        )

        # Simulate rate limit hit
        mock_fetch.side_effect = RateLimitError("Rate limit threshold reached")

        result = self.service._get_openmeteo_forecast(self.test_date)

        # Should fall back to stale data
        self.assertEqual(result.wind.speed, 15)
        self.assertEqual(result.wind.gust, 20)
        self.assertTrue(result.from_cache)


class WeatherRecordModelTests(TestCase):
    """Tests for the WeatherRecord model."""

    def test_create_with_station(self):
        record = WeatherRecord.objects.create(
            weather_type=WeatherRecord.WeatherType.METAR,
            target_date=date.today(),
            station='KACV',
            data={'raw': 'METAR data'},
            fetched_at=timezone.now(),
        )

        self.assertEqual(record.station, 'KACV')
        self.assertIsNone(record.latitude)
        self.assertIsNone(record.longitude)
        self.assertEqual(str(record), f'METAR - {date.today()} @ KACV')

    def test_create_with_coordinates(self):
        record = WeatherRecord.objects.create(
            weather_type=WeatherRecord.WeatherType.OPENMETEO,
            target_date=date.today(),
            latitude=Decimal('40.978100'),
            longitude=Decimal('-124.108600'),
            data={'test': 'data'},
            fetched_at=timezone.now(),
        )

        self.assertEqual(record.latitude, Decimal('40.978100'))
        self.assertEqual(record.longitude, Decimal('-124.108600'))
        self.assertEqual(record.station, '')

    def test_weather_type_choices(self):
        for i, weather_type in enumerate(WeatherRecord.WeatherType):
            record = WeatherRecord.objects.create(
                weather_type=weather_type,
                target_date=date.today() + timedelta(days=i),  # Different dates to avoid unique constraint
                station='TEST',
                data={},
                fetched_at=timezone.now(),
            )
            self.assertEqual(record.weather_type, weather_type)

    def test_ordering_by_fetched_at_descending(self):
        old_record = WeatherRecord.objects.create(
            weather_type=WeatherRecord.WeatherType.METAR,
            target_date=date.today(),
            station='KACV',
            data={'order': 'old'},
            fetched_at=timezone.now() - timedelta(hours=1),
        )

        new_record = WeatherRecord.objects.create(
            weather_type=WeatherRecord.WeatherType.METAR,
            target_date=date.today() + timedelta(days=1),  # Different date to avoid unique constraint
            station='KACV',
            data={'order': 'new'},
            fetched_at=timezone.now(),
        )

        records = list(WeatherRecord.objects.all())
        self.assertEqual(records[0].pk, new_record.pk)
        self.assertEqual(records[1].pk, old_record.pk)


class MetarSerializationTests(TestCase):
    """Tests for METAR data serialization/deserialization."""

    def setUp(self):
        self.service = WeatherService()
        self.test_date = date.today()

    def test_serialize_metar_data(self):
        obs_time = datetime(2024, 1, 15, 10, 0, 0)
        data = WeatherData(
            station='KACV',
            raw_metar='KACV 151000Z 27010G15KT 10SM FEW050 BKN100 15/10 A3012',
            observation_time=obs_time,
            wind=WindData(direction=270, speed=10, gust=15, direction_repr='270'),
            visibility=10.0,
            visibility_repr='10',
            clouds=[
                CloudLayer(coverage='FEW', altitude=50),
                CloudLayer(coverage='BKN', altitude=100),
            ],
            temperature=15,
            dewpoint=10,
            flight_rules='VFR',
        )

        serialized = self.service._serialize_metar_data(data)

        self.assertEqual(serialized['station'], 'KACV')
        self.assertEqual(serialized['wind']['speed'], 10)
        self.assertEqual(serialized['wind']['gust'], 15)
        self.assertEqual(len(serialized['clouds']), 2)
        self.assertEqual(serialized['clouds'][0]['coverage'], 'FEW')
        self.assertEqual(serialized['temperature'], 15)

    def test_deserialize_metar_data(self):
        serialized = {
            'station': 'KACV',
            'raw_metar': 'KACV 151000Z 27010KT 10SM CLR 20/12 A3012',
            'observation_time': '2024-01-15T10:00:00',
            'wind': {
                'direction': 270,
                'speed': 10,
                'gust': None,
                'direction_repr': '270',
            },
            'visibility': 10.0,
            'visibility_repr': '10',
            'clouds': [{'coverage': 'CLR', 'altitude': None}],
            'temperature': 20,
            'dewpoint': 12,
            'flight_rules': 'VFR',
        }

        data = self.service._deserialize_metar_data(serialized)

        self.assertEqual(data.station, 'KACV')
        self.assertEqual(data.wind.speed, 10)
        self.assertEqual(data.temperature, 20)
        self.assertTrue(data.from_cache)
        self.assertEqual(data.source, WeatherSource.METAR)

    def test_roundtrip_metar_serialization(self):
        obs_time = datetime(2024, 1, 15, 10, 0, 0)
        original = WeatherData(
            station='KACV',
            raw_metar='KACV 151000Z 27015G20KT 10SM SCT040 BKN080 18/12 A3010',
            observation_time=obs_time,
            wind=WindData(direction=270, speed=15, gust=20, direction_repr='270'),
            visibility=10.0,
            visibility_repr='10',
            clouds=[CloudLayer(coverage='SCT', altitude=40)],
            temperature=18,
            dewpoint=12,
            flight_rules='VFR',
        )

        serialized = self.service._serialize_metar_data(original)
        restored = self.service._deserialize_metar_data(serialized)

        self.assertEqual(restored.station, original.station)
        self.assertEqual(restored.wind.speed, original.wind.speed)
        self.assertEqual(restored.wind.gust, original.wind.gust)
        self.assertEqual(restored.temperature, original.temperature)


class TafSerializationTests(TestCase):
    """Tests for TAF data serialization/deserialization."""

    def setUp(self):
        self.service = WeatherService()
        self.test_date = date.today() + timedelta(days=1)

    def test_serialize_taf_data(self):
        issue_time = datetime(2024, 1, 15, 10, 0, 0)
        period_start = datetime(2024, 1, 15, 12, 0, 0)
        period_end = datetime(2024, 1, 16, 12, 0, 0)

        period = TafForecastPeriod(
            start_time=period_start,
            end_time=period_end,
            wind=WindData(direction=270, speed=10, gust=15, direction_repr='270'),
            visibility=10.0,
            clouds=[CloudLayer(coverage='SCT', altitude=40)],
            flight_rules='VFR',
            raw_line='FM151200 27010G15KT P6SM SCT040',
        )

        data = TafForecastData(
            station='KACV',
            raw_taf='TAF KACV 151000Z ...',
            issue_time=issue_time,
            target_date=self.test_date,
            period=period,
            wind=WindData(direction=270, speed=10, gust=15, direction_repr='270'),
            visibility=10.0,
            clouds=[CloudLayer(coverage='SCT', altitude=40)],
            flight_rules='VFR',
        )

        serialized = self.service._serialize_taf_data(data)

        self.assertEqual(serialized['station'], 'KACV')
        self.assertEqual(serialized['period']['wind']['speed'], 10)
        self.assertEqual(serialized['flight_rules'], 'VFR')

    def test_deserialize_taf_data(self):
        serialized = {
            'station': 'KACV',
            'raw_taf': 'TAF KACV 151000Z ...',
            'issue_time': '2024-01-15T10:00:00',
            'target_date': self.test_date.isoformat(),
            'period': {
                'start_time': '2024-01-15T12:00:00',
                'end_time': '2024-01-16T12:00:00',
                'wind': {
                    'direction': 270,
                    'speed': 12,
                    'gust': None,
                    'direction_repr': '270',
                },
                'visibility': 10.0,
                'clouds': [{'coverage': 'BKN', 'altitude': 50}],
                'flight_rules': 'VFR',
                'raw_line': 'FM151200 27012KT P6SM BKN050',
            },
            'wind': {
                'direction': 270,
                'speed': 12,
                'gust': None,
                'direction_repr': '270',
            },
            'visibility': 10.0,
            'clouds': [{'coverage': 'BKN', 'altitude': 50}],
            'flight_rules': 'VFR',
        }

        data = self.service._deserialize_taf_data(serialized)

        self.assertEqual(data.station, 'KACV')
        self.assertEqual(data.wind.speed, 12)
        self.assertEqual(data.period.wind.speed, 12)
        self.assertTrue(data.from_cache)
        self.assertEqual(data.source, WeatherSource.TAF)


class NwsSerializationTests(TestCase):
    """Tests for NWS data serialization/deserialization."""

    def setUp(self):
        self.service = WeatherService()
        self.test_date = date.today() + timedelta(days=3)

    def test_serialize_nws_data(self):
        period_start = datetime(2024, 1, 18, 6, 0, 0)
        period_end = datetime(2024, 1, 18, 18, 0, 0)

        periods = [
            NwsForecastPeriod(
                name='Thursday',
                start_time=period_start,
                end_time=period_end,
                temperature=65,
                temperature_unit='F',
                is_daytime=True,
                wind_speed='10 to 15 mph',
                wind_direction='SW',
                short_forecast='Sunny',
                detailed_forecast='Sunny with a high near 65.',
                precipitation_probability=10,
            ),
        ]

        data = NwsForecastData(
            location=(40.9781, -124.1086),
            target_date=self.test_date,
            periods=periods,
            wind=WindData(direction=225, speed=13, gust=None, direction_repr='SW'),
            temperature_high=65,
            temperature_low=45,
            short_forecast='Sunny',
            precipitation_probability=10,
        )

        serialized = self.service._serialize_nws_data(data)

        self.assertEqual(serialized['location'], [40.9781, -124.1086])
        self.assertEqual(len(serialized['periods']), 1)
        self.assertEqual(serialized['periods'][0]['temperature'], 65)
        self.assertEqual(serialized['wind']['speed'], 13)

    def test_deserialize_nws_data(self):
        serialized = {
            'location': [40.9781, -124.1086],
            'target_date': self.test_date.isoformat(),
            'periods': [
                {
                    'name': 'Friday',
                    'start_time': '2024-01-19T06:00:00',
                    'end_time': '2024-01-19T18:00:00',
                    'temperature': 60,
                    'temperature_unit': 'F',
                    'is_daytime': True,
                    'wind_speed': '5 to 10 mph',
                    'wind_direction': 'NW',
                    'short_forecast': 'Mostly Sunny',
                    'detailed_forecast': 'Mostly sunny with a high near 60.',
                    'precipitation_probability': 5,
                },
            ],
            'wind': {
                'direction': 315,
                'speed': 9,
                'gust': None,
                'direction_repr': 'NW',
            },
            'temperature_high': 60,
            'temperature_low': 42,
            'short_forecast': 'Mostly Sunny',
            'precipitation_probability': 5,
        }

        data = self.service._deserialize_nws_data(serialized)

        self.assertEqual(data.location, (40.9781, -124.1086))
        self.assertEqual(data.wind.speed, 9)
        self.assertEqual(len(data.periods), 1)
        self.assertEqual(data.periods[0].temperature, 60)
        self.assertTrue(data.from_cache)
        self.assertEqual(data.source, WeatherSource.NWS)


class RateLimitDBCountTests(TestCase):
    """Tests for DB-based rate limit counting."""

    def setUp(self):
        WeatherRecord.objects.all().delete()
        self.service = WeatherService()

    def test_check_rate_limit_with_no_records(self):
        """Should return True when no records exist."""
        result = self.service._check_rate_limit()
        self.assertTrue(result)

    def test_check_rate_limit_counts_openmeteo_types(self):
        """Should count openmeteo, hourly, and historical records."""
        # Create records for Open-Meteo API types
        for i, weather_type in enumerate(['openmeteo', 'hourly', 'historical']):
            WeatherRecord.objects.create(
                weather_type=weather_type,
                target_date=date.today() + timedelta(days=i),  # Different dates to avoid unique constraint
                latitude=Decimal('40.9781'),
                longitude=Decimal('-124.1086'),
                data={'test': 'data'},
                fetched_at=timezone.now(),
            )

        # With 3 records, should still be under limit
        result = self.service._check_rate_limit()
        self.assertTrue(result)

    def test_check_rate_limit_ignores_non_openmeteo_types(self):
        """Should not count METAR, TAF, or NWS records."""
        # Create many non-Open-Meteo records
        for i in range(100):
            WeatherRecord.objects.create(
                weather_type='metar',
                target_date=date.today() + timedelta(days=i),  # Different dates to avoid unique constraint
                station='KACV',
                data={'test': f'data_{i}'},
                fetched_at=timezone.now(),
            )

        # Should still return True since these don't count
        result = self.service._check_rate_limit()
        self.assertTrue(result)
