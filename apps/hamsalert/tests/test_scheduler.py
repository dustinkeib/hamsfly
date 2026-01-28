"""Tests for the scheduler module."""

from datetime import timedelta
from unittest.mock import patch

from django.core.cache import cache
from django.test import TestCase, override_settings
from django.utils import timezone

from apps.hamsalert.scheduler import (
    get_last_run,
    set_last_run,
    should_run_today,
    run_cleanup_weather_records,
)


class SchedulerLastRunTests(TestCase):
    """Tests for get_last_run and set_last_run functions."""

    def setUp(self):
        cache.clear()

    def tearDown(self):
        cache.clear()

    def test_get_last_run_returns_none_when_never_run(self):
        result = get_last_run('test_task')
        self.assertIsNone(result)

    def test_set_and_get_last_run(self):
        now = timezone.now()
        set_last_run('test_task', now)
        result = get_last_run('test_task')
        self.assertEqual(result, now)

    def test_set_last_run_defaults_to_now(self):
        before = timezone.now()
        set_last_run('test_task')
        after = timezone.now()

        result = get_last_run('test_task')
        self.assertIsNotNone(result)
        self.assertGreaterEqual(result, before)
        self.assertLessEqual(result, after)

    def test_different_tasks_have_separate_last_run(self):
        time1 = timezone.now()
        time2 = time1 - timedelta(hours=5)

        set_last_run('task_a', time1)
        set_last_run('task_b', time2)

        self.assertEqual(get_last_run('task_a'), time1)
        self.assertEqual(get_last_run('task_b'), time2)


class ShouldRunTodayTests(TestCase):
    """Tests for should_run_today function."""

    def setUp(self):
        cache.clear()

    def tearDown(self):
        cache.clear()

    def test_should_run_when_never_run_and_past_target_hour(self):
        mock_now = timezone.now().replace(hour=10, minute=0, second=0, microsecond=0)
        with patch('apps.hamsalert.scheduler.timezone.localtime', return_value=mock_now):
            result = should_run_today('test_task', target_hour=3)
        self.assertTrue(result)

    def test_should_not_run_before_target_hour(self):
        mock_now = timezone.now().replace(hour=2, minute=0, second=0, microsecond=0)
        with patch('apps.hamsalert.scheduler.timezone.localtime', return_value=mock_now):
            result = should_run_today('test_task', target_hour=3)
        self.assertFalse(result)

    def test_should_not_run_if_already_run_today(self):
        mock_now = timezone.now().replace(hour=10, minute=0, second=0, microsecond=0)
        last_run = mock_now.replace(hour=4)
        set_last_run('test_task', last_run)

        with patch('apps.hamsalert.scheduler.timezone.localtime', return_value=mock_now):
            result = should_run_today('test_task', target_hour=3)
        self.assertFalse(result)

    def test_should_run_if_last_run_was_yesterday(self):
        mock_now = timezone.now().replace(hour=10, minute=0, second=0, microsecond=0)
        yesterday = mock_now - timedelta(days=1)
        last_run_time = yesterday.replace(hour=4)
        set_last_run('test_task', last_run_time)

        real_localtime = timezone.localtime

        def mock_localtime(dt=None):
            if dt is None:
                return mock_now
            return real_localtime(dt)

        with patch('apps.hamsalert.scheduler.timezone.localtime', side_effect=mock_localtime):
            result = should_run_today('test_task', target_hour=3)
        self.assertTrue(result)


class RunCleanupWeatherRecordsTests(TestCase):
    """Tests for run_cleanup_weather_records function."""

    def setUp(self):
        cache.clear()

    def tearDown(self):
        cache.clear()

    @override_settings(SCHEDULER_CLEANUP_HOUR=3)
    def test_run_cleanup_sets_last_run_on_success(self):
        mock_now = timezone.now().replace(hour=10, minute=0, second=0, microsecond=0)

        with patch('apps.hamsalert.scheduler.timezone.localtime', return_value=mock_now):
            run_cleanup_weather_records()

        last_run = get_last_run('cleanup_weather_records')
        self.assertIsNotNone(last_run)

    @override_settings(SCHEDULER_CLEANUP_HOUR=3)
    def test_run_cleanup_skips_if_already_run_today(self):
        mock_now = timezone.now().replace(hour=10, minute=0, second=0, microsecond=0)
        earlier_today = mock_now.replace(hour=4)
        set_last_run('cleanup_weather_records', earlier_today)

        with patch('apps.hamsalert.scheduler.timezone.localtime', return_value=mock_now):
            with patch('apps.hamsalert.cron.cleanup_weather_records') as mock_cleanup:
                run_cleanup_weather_records()
                mock_cleanup.assert_not_called()

    @override_settings(SCHEDULER_CLEANUP_HOUR=3)
    def test_run_cleanup_skips_before_target_hour(self):
        mock_now = timezone.now().replace(hour=2, minute=0, second=0, microsecond=0)

        with patch('apps.hamsalert.scheduler.timezone.localtime', return_value=mock_now):
            with patch('apps.hamsalert.cron.cleanup_weather_records') as mock_cleanup:
                run_cleanup_weather_records()
                mock_cleanup.assert_not_called()
