"""Tests for cron jobs and the cleanup management command."""

from datetime import timedelta
from io import StringIO

from django.core.cache import cache
from django.core.management import call_command
from django.test import TestCase, override_settings
from django.utils import timezone

from apps.hamsalert.cron import cleanup_weather_records
from apps.hamsalert.models import WeatherRecord


class CleanupWeatherRecordsTests(TestCase):
    """Tests for the cleanup_weather_records cron function."""

    def setUp(self):
        cache.clear()
        WeatherRecord.objects.all().delete()

    def tearDown(self):
        cache.clear()

    def create_weather_record(self, days_ago, weather_type=None):
        """Helper to create a WeatherRecord with a specific age."""
        weather_type = weather_type or WeatherRecord.WeatherType.METAR
        record = WeatherRecord.objects.create(
            weather_type=weather_type,
            target_date=timezone.now().date(),
            station='KACV',
            data={'test': 'data'},
        )
        old_time = timezone.now() - timedelta(days=days_ago)
        WeatherRecord.objects.filter(pk=record.pk).update(fetched_at=old_time)
        return record

    @override_settings(WEATHER_DB_CLEANUP_DAYS=30)
    def test_cleanup_deletes_old_records(self):
        old_record = self.create_weather_record(days_ago=31)
        recent_record = self.create_weather_record(days_ago=29)

        deleted = cleanup_weather_records()

        self.assertEqual(deleted, 1)
        self.assertFalse(WeatherRecord.objects.filter(pk=old_record.pk).exists())
        self.assertTrue(WeatherRecord.objects.filter(pk=recent_record.pk).exists())

    @override_settings(WEATHER_DB_CLEANUP_DAYS=30)
    def test_cleanup_returns_zero_when_no_old_records(self):
        self.create_weather_record(days_ago=5)
        self.create_weather_record(days_ago=10)

        deleted = cleanup_weather_records()

        self.assertEqual(deleted, 0)
        self.assertEqual(WeatherRecord.objects.count(), 2)

    @override_settings(WEATHER_DB_CLEANUP_DAYS=7)
    def test_cleanup_respects_custom_days_setting(self):
        record_8_days = self.create_weather_record(days_ago=8)
        record_6_days = self.create_weather_record(days_ago=6)

        deleted = cleanup_weather_records()

        self.assertEqual(deleted, 1)
        self.assertFalse(WeatherRecord.objects.filter(pk=record_8_days.pk).exists())
        self.assertTrue(WeatherRecord.objects.filter(pk=record_6_days.pk).exists())

    @override_settings(WEATHER_DB_CLEANUP_DAYS=30)
    def test_cleanup_deletes_multiple_old_records(self):
        for i in range(5):
            self.create_weather_record(days_ago=35 + i)
        for i in range(3):
            self.create_weather_record(days_ago=10 + i)

        deleted = cleanup_weather_records()

        self.assertEqual(deleted, 5)
        self.assertEqual(WeatherRecord.objects.count(), 3)

    @override_settings(WEATHER_DB_CLEANUP_DAYS=30)
    def test_cleanup_deletes_all_weather_types(self):
        for weather_type in WeatherRecord.WeatherType:
            self.create_weather_record(days_ago=35, weather_type=weather_type)

        deleted = cleanup_weather_records()

        self.assertEqual(deleted, len(WeatherRecord.WeatherType))
        self.assertEqual(WeatherRecord.objects.count(), 0)


class CleanupManagementCommandTests(TestCase):
    """Tests for the cleanup_weather_records management command."""

    def setUp(self):
        WeatherRecord.objects.all().delete()

    def create_weather_record(self, days_ago, weather_type=None):
        """Helper to create a WeatherRecord with a specific age."""
        weather_type = weather_type or WeatherRecord.WeatherType.METAR
        record = WeatherRecord.objects.create(
            weather_type=weather_type,
            target_date=timezone.now().date(),
            station='KACV',
            data={'test': 'data'},
        )
        old_time = timezone.now() - timedelta(days=days_ago)
        WeatherRecord.objects.filter(pk=record.pk).update(fetched_at=old_time)
        return record

    @override_settings(WEATHER_DB_CLEANUP_DAYS=30)
    def test_dry_run_does_not_delete(self):
        self.create_weather_record(days_ago=35)

        out = StringIO()
        call_command('cleanup_weather_records', '--dry-run', stdout=out)

        self.assertEqual(WeatherRecord.objects.count(), 1)
        self.assertIn('Would delete 1 records', out.getvalue())

    @override_settings(WEATHER_DB_CLEANUP_DAYS=30)
    def test_command_deletes_old_records(self):
        self.create_weather_record(days_ago=35)
        self.create_weather_record(days_ago=10)

        out = StringIO()
        call_command('cleanup_weather_records', stdout=out)

        self.assertEqual(WeatherRecord.objects.count(), 1)
        self.assertIn('Deleted 1 records', out.getvalue())

    def test_days_argument_overrides_setting(self):
        self.create_weather_record(days_ago=10)

        out = StringIO()
        call_command('cleanup_weather_records', '--days=5', stdout=out)

        self.assertEqual(WeatherRecord.objects.count(), 0)
        self.assertIn('Deleted 1 records older than 5 days', out.getvalue())

    @override_settings(WEATHER_DB_CLEANUP_DAYS=30)
    def test_dry_run_shows_breakdown_by_type(self):
        self.create_weather_record(days_ago=35, weather_type=WeatherRecord.WeatherType.METAR)
        self.create_weather_record(days_ago=35, weather_type=WeatherRecord.WeatherType.NWS)
        self.create_weather_record(days_ago=35, weather_type=WeatherRecord.WeatherType.NWS)

        out = StringIO()
        call_command('cleanup_weather_records', '--dry-run', stdout=out)

        output = out.getvalue()
        self.assertIn('metar: 1', output)
        self.assertIn('nws: 2', output)
