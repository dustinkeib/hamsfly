"""Tests for calendar views."""

from django.test import TestCase, Client
from django.urls import reverse


class CalendarNavigationTests(TestCase):
    """Tests for calendar prev/next navigation."""

    def setUp(self):
        self.client = Client()

    def test_prev_goes_to_last_day_of_previous_month(self):
        """Prev button should link to last day of previous month."""
        # February 2026 -> January 31, 2026
        response = self.client.get(reverse('calendar_day', args=[2026, 2, 15]))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context['prev_year'], 2026)
        self.assertEqual(response.context['prev_month'], 1)
        self.assertEqual(response.context['prev_day'], 31)  # January has 31 days

    def test_prev_handles_month_with_30_days(self):
        """Prev from May should go to April 30."""
        response = self.client.get(reverse('calendar_day', args=[2026, 5, 10]))
        self.assertEqual(response.context['prev_year'], 2026)
        self.assertEqual(response.context['prev_month'], 4)
        self.assertEqual(response.context['prev_day'], 30)  # April has 30 days

    def test_prev_handles_leap_year_february(self):
        """Prev from March in leap year should go to Feb 29."""
        # 2024 was a leap year
        response = self.client.get(reverse('calendar_day', args=[2024, 3, 15]))
        self.assertEqual(response.context['prev_year'], 2024)
        self.assertEqual(response.context['prev_month'], 2)
        self.assertEqual(response.context['prev_day'], 29)  # Leap year Feb has 29 days

    def test_prev_handles_non_leap_year_february(self):
        """Prev from March in non-leap year should go to Feb 28."""
        # 2025 is not a leap year
        response = self.client.get(reverse('calendar_day', args=[2025, 3, 15]))
        self.assertEqual(response.context['prev_year'], 2025)
        self.assertEqual(response.context['prev_month'], 2)
        self.assertEqual(response.context['prev_day'], 28)  # Non-leap year Feb has 28 days

    def test_prev_from_january_goes_to_previous_year_december(self):
        """Prev from January should go to December 31 of previous year."""
        response = self.client.get(reverse('calendar_day', args=[2026, 1, 15]))
        self.assertEqual(response.context['prev_year'], 2025)
        self.assertEqual(response.context['prev_month'], 12)
        self.assertEqual(response.context['prev_day'], 31)  # December has 31 days

    def test_next_goes_to_first_day_of_next_month(self):
        """Next button should link to first day of next month."""
        response = self.client.get(reverse('calendar_day', args=[2026, 2, 15]))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context['next_year'], 2026)
        self.assertEqual(response.context['next_month'], 3)
        self.assertEqual(response.context['next_day'], 1)

    def test_next_from_december_goes_to_next_year_january(self):
        """Next from December should go to January 1 of next year."""
        response = self.client.get(reverse('calendar_day', args=[2026, 12, 15]))
        self.assertEqual(response.context['next_year'], 2027)
        self.assertEqual(response.context['next_month'], 1)
        self.assertEqual(response.context['next_day'], 1)

    def test_next_day_is_always_one(self):
        """Next day should always be 1 regardless of current month."""
        for month in range(1, 13):
            response = self.client.get(reverse('calendar_day', args=[2026, month, 10]))
            self.assertEqual(
                response.context['next_day'], 1,
                f"next_day should be 1 for month {month}"
            )
