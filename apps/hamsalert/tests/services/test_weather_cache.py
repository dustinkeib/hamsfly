"""Tests for weather service two-tier caching (in-memory + database)."""

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

from django.core.cache import cache
from django.test import TestCase, override_settings
from django.utils import timezone

from apps.hamsalert.models import WeatherRecord
from apps.hamsalert.services.weather import (
    HourlyForecastData,
    HourlyForecastEntry,
    HistoricalWeatherData,
    OpenMeteoForecastData,
    RateLimitError,
    WeatherService,
    WeatherServiceError,
    WindData,
)


class WeatherServiceDBCacheTests(TestCase):
    """Tests for database-level caching in WeatherService."""

    def setUp(self):
        cache.clear()
        WeatherRecord.objects.all().delete()
        self.service = WeatherService()
        self.test_date = date.today() + timedelta(days=10)
        self.lat, self.lon = self.service.nws_location

    def tearDown(self):
        cache.clear()

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


class TwoTierCacheIntegrationTests(TestCase):
    """Integration tests for the two-tier caching strategy."""

    def setUp(self):
        cache.clear()
        WeatherRecord.objects.all().delete()
        self.service = WeatherService()
        self.test_date = date.today() + timedelta(days=10)
        self.lat, self.lon = self.service.nws_location

    def tearDown(self):
        cache.clear()

    @patch.object(WeatherService, '_fetch_openmeteo_forecast')
    def test_cache_miss_fetches_from_api_and_stores(self, mock_fetch):
        """On cache miss, should fetch from API and store in both cache and DB."""
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

    @patch.object(WeatherService, '_fetch_openmeteo_forecast')
    def test_db_cache_hit_skips_api(self, mock_fetch):
        """On DB cache hit (memory miss), should not call API."""
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

    @patch.object(WeatherService, '_fetch_openmeteo_forecast')
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

    @patch.object(WeatherService, '_fetch_openmeteo_forecast')
    def test_api_error_with_no_stale_data_raises(self, mock_fetch):
        """On API error with no stale data, should raise exception."""
        mock_fetch.side_effect = WeatherServiceError("API unavailable")

        with self.assertRaises(WeatherServiceError):
            self.service._get_openmeteo_forecast(self.test_date)

    @patch.object(WeatherService, '_fetch_openmeteo_forecast')
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
        mock_fetch.side_effect = RateLimitError("Open-Meteo rate limit threshold reached")

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
        )

        self.assertEqual(record.latitude, Decimal('40.978100'))
        self.assertEqual(record.longitude, Decimal('-124.108600'))
        self.assertEqual(record.station, '')

    def test_weather_type_choices(self):
        for weather_type in WeatherRecord.WeatherType:
            record = WeatherRecord.objects.create(
                weather_type=weather_type,
                target_date=date.today(),
                station='TEST',
                data={},
            )
            self.assertEqual(record.weather_type, weather_type)

    def test_ordering_by_fetched_at_descending(self):
        old_record = WeatherRecord.objects.create(
            weather_type=WeatherRecord.WeatherType.METAR,
            target_date=date.today(),
            station='KACV',
            data={'order': 'old'},
        )
        WeatherRecord.objects.filter(pk=old_record.pk).update(
            fetched_at=timezone.now() - timedelta(hours=1)
        )

        new_record = WeatherRecord.objects.create(
            weather_type=WeatherRecord.WeatherType.METAR,
            target_date=date.today(),
            station='KACV',
            data={'order': 'new'},
        )

        records = list(WeatherRecord.objects.all())
        self.assertEqual(records[0].pk, new_record.pk)
        self.assertEqual(records[1].pk, old_record.pk)
