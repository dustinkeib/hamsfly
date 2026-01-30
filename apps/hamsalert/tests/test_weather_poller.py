"""Tests for the background weather poller."""

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

from django.test import TestCase, override_settings
from django.utils import timezone

from apps.hamsalert.models import WeatherRecord
from apps.hamsalert.weather_poller import WeatherPoller


class WeatherPollerTests(TestCase):
    """Tests for WeatherPoller class."""

    def setUp(self):
        WeatherRecord.objects.all().delete()

    def test_poll_metar_saves_to_db(self):
        """METAR poll should save data to DB via _save_to_db."""
        poller = WeatherPoller()
        mock_service = MagicMock()
        mock_service.default_station = 'KACV'
        mock_service._fetch_metar_from_api.return_value = MagicMock()
        mock_service._serialize_metar_data.return_value = {'test': 'data'}
        poller.service = mock_service

        poller._poll_metar(date.today())

        mock_service._fetch_metar_from_api.assert_called_once()
        mock_service._save_to_db.assert_called_once()

    def test_poll_taf_saves_for_two_days(self):
        """TAF poll should save data for days 0 and 1."""
        poller = WeatherPoller()
        mock_service = MagicMock()
        mock_service.default_station = 'KACV'
        mock_service._fetch_taf_from_api.return_value = MagicMock()
        mock_service._serialize_taf_data.return_value = {'test': 'data'}
        poller.service = mock_service

        poller._poll_taf(date.today())

        # Should be called twice (day 0 and day 1)
        self.assertEqual(mock_service._fetch_taf_from_api.call_count, 2)
        self.assertEqual(mock_service._save_to_db.call_count, 2)

    def test_poll_nws_saves_for_days_2_to_7(self):
        """NWS poll should save data for days 2-7."""
        poller = WeatherPoller()
        mock_service = MagicMock()
        mock_service.nws_location = (40.9781, -124.1086)
        mock_service._fetch_nws_forecast.return_value = MagicMock()
        mock_service._serialize_nws_data.return_value = {'test': 'data'}
        poller.service = mock_service

        poller._poll_nws(date.today())

        # Should be called 6 times (days 2, 3, 4, 5, 6, 7)
        self.assertEqual(mock_service._fetch_nws_forecast.call_count, 6)
        self.assertEqual(mock_service._save_to_db.call_count, 6)

    def test_poll_openmeteo_saves_daily_and_hourly(self):
        """OpenMeteo poll should use batch fetch for daily and hourly data."""
        poller = WeatherPoller()
        mock_service = MagicMock()
        mock_service.nws_location = (40.9781, -124.1086)

        # Batch methods return list of (date, data) tuples
        today = date.today()
        mock_daily_results = [
            (today + timedelta(days=i), MagicMock())
            for i in range(15)
        ]
        mock_hourly_results = [
            (today + timedelta(days=i), MagicMock())
            for i in range(15)
        ]
        mock_service.fetch_visualcrossing_batch.return_value = mock_daily_results
        mock_service.fetch_visualcrossing_hourly_batch.return_value = mock_hourly_results
        mock_service._serialize_openmeteo_data.return_value = {'test': 'data'}
        mock_service._serialize_hourly_data.return_value = {'test': 'hourly'}
        poller.service = mock_service

        poller._poll_openmeteo(today)

        # Batch methods should be called once each (not 15 times)
        mock_service.fetch_visualcrossing_batch.assert_called_once_with(today)
        mock_service.fetch_visualcrossing_hourly_batch.assert_called_once_with(today, days=15)
        # Save called twice per day (daily + hourly) = 30 total
        self.assertEqual(mock_service._save_to_db.call_count, 30)

    def test_poll_source_handles_api_error(self):
        """Poller should handle API errors gracefully."""
        poller = WeatherPoller()
        mock_service = MagicMock()
        mock_service.default_station = 'KACV'
        mock_service._fetch_metar_from_api.side_effect = Exception("API error")
        poller.service = mock_service

        # Should not raise an exception
        poller._poll_metar(date.today())

        # _save_to_db should not be called since API failed
        mock_service._save_to_db.assert_not_called()

    def test_poll_if_due_respects_interval(self):
        """_poll_if_due should only poll when interval has elapsed."""
        poller = WeatherPoller()
        poller._poll_source = MagicMock()

        # First poll should happen (last_poll is None)
        poller._poll_if_due('metar', 1800)
        poller._poll_source.assert_called_once_with('metar')

        # Second poll should not happen (interval not elapsed)
        poller._poll_source.reset_mock()
        poller._poll_if_due('metar', 1800)
        poller._poll_source.assert_not_called()

    def test_poll_all_sources_sets_last_poll_times(self):
        """_poll_all_sources should update last_poll for all sources."""
        poller = WeatherPoller()
        poller._poll_source = MagicMock()

        poller._poll_all_sources()

        # All sources should have been polled
        self.assertEqual(poller._poll_source.call_count, 5)
        # All last_poll times should be set
        for source in ['metar', 'taf', 'nws', 'openmeteo', 'historical']:
            self.assertIsNotNone(poller.last_poll[source])
