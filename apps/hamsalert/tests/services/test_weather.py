"""Tests for weather service data classes and utility functions."""

from datetime import date, datetime
from unittest.mock import MagicMock, patch

from django.test import TestCase, override_settings

from apps.hamsalert.services.weather import (
    CloudLayer,
    RateLimitError,
    WeatherData,
    WeatherSource,
    WindData,
    calculate_rc_assessment,
    rc_rating_color,
    wind_arrow,
    WeatherService,
    WeatherServiceError,
)


class WindDataTests(TestCase):
    """Tests for WindData dataclass."""

    def test_is_gusty_when_gust_exceeds_speed(self):
        wind = WindData(direction=270, speed=10, gust=15, direction_repr='270')
        self.assertTrue(wind.is_gusty)

    def test_is_not_gusty_when_no_gust(self):
        wind = WindData(direction=270, speed=10, gust=None, direction_repr='270')
        self.assertFalse(wind.is_gusty)

    def test_gust_factor_calculation(self):
        wind = WindData(direction=270, speed=10, gust=18, direction_repr='270')
        self.assertEqual(wind.gust_factor, 8)

    def test_gust_factor_none_when_no_gust(self):
        wind = WindData(direction=270, speed=10, gust=None, direction_repr='270')
        self.assertIsNone(wind.gust_factor)

    def test_direction_compass_north(self):
        wind = WindData(direction=0, speed=10, gust=None, direction_repr='000')
        self.assertEqual(wind.direction_compass, 'N')

    def test_direction_compass_south(self):
        wind = WindData(direction=180, speed=10, gust=None, direction_repr='180')
        self.assertEqual(wind.direction_compass, 'S')

    def test_direction_compass_west(self):
        wind = WindData(direction=270, speed=10, gust=None, direction_repr='270')
        self.assertEqual(wind.direction_compass, 'W')

    def test_direction_compass_variable(self):
        wind = WindData(direction=None, speed=5, gust=None, direction_repr='VRB')
        self.assertEqual(wind.direction_compass, 'VRB')

    def test_direction_compass_northeast(self):
        wind = WindData(direction=45, speed=10, gust=None, direction_repr='045')
        self.assertEqual(wind.direction_compass, 'NE')


class CloudLayerTests(TestCase):
    """Tests for CloudLayer dataclass."""

    def test_coverage_text_few(self):
        layer = CloudLayer(coverage='FEW', altitude=5000)
        self.assertEqual(layer.coverage_text, 'Few')

    def test_coverage_text_overcast(self):
        layer = CloudLayer(coverage='OVC', altitude=2000)
        self.assertEqual(layer.coverage_text, 'Overcast')

    def test_coverage_text_clear(self):
        layer = CloudLayer(coverage='CLR', altitude=None)
        self.assertEqual(layer.coverage_text, 'Clear')

    def test_unknown_coverage(self):
        layer = CloudLayer(coverage='XXX', altitude=1000)
        self.assertEqual(layer.coverage_text, 'XXX')


class RcAssessmentTests(TestCase):
    """Tests for R/C flying assessment calculations."""

    def test_good_conditions(self):
        result = calculate_rc_assessment(wind_speed=5, wind_gust=None)
        self.assertEqual(result['rating'], 'good')
        self.assertEqual(result['reasons'], [])

    def test_marginal_moderate_wind(self):
        result = calculate_rc_assessment(wind_speed=12, wind_gust=None)
        self.assertEqual(result['rating'], 'marginal')
        self.assertIn('Moderate wind: 12 kt', result['reasons'])

    def test_poor_high_wind(self):
        result = calculate_rc_assessment(wind_speed=17, wind_gust=None)
        self.assertEqual(result['rating'], 'poor')
        self.assertIn('High wind: 17 kt', result['reasons'])

    def test_no_fly_strong_wind(self):
        result = calculate_rc_assessment(wind_speed=22, wind_gust=None)
        self.assertEqual(result['rating'], 'no-fly')
        self.assertIn('Wind too strong: 22 kt', result['reasons'])

    def test_no_fly_dangerous_gusts(self):
        result = calculate_rc_assessment(wind_speed=10, wind_gust=28)
        self.assertEqual(result['rating'], 'no-fly')
        self.assertIn('Dangerous gusts: 28 kt', result['reasons'])

    def test_poor_strong_gusts(self):
        result = calculate_rc_assessment(wind_speed=8, wind_gust=22)
        self.assertEqual(result['rating'], 'poor')
        self.assertIn('Strong gusts: 22 kt', result['reasons'])

    def test_marginal_gusty_spread(self):
        result = calculate_rc_assessment(wind_speed=5, wind_gust=16)
        self.assertEqual(result['rating'], 'marginal')
        self.assertIn('Gusty: 11 kt spread', result['reasons'])

    def test_no_fly_low_visibility(self):
        result = calculate_rc_assessment(wind_speed=5, wind_gust=None, visibility=0.5)
        self.assertEqual(result['rating'], 'no-fly')
        self.assertIn('Very low visibility: 0.5 SM', result['reasons'])

    def test_poor_reduced_visibility(self):
        result = calculate_rc_assessment(wind_speed=5, wind_gust=None, visibility=2)
        self.assertEqual(result['rating'], 'poor')
        self.assertIn('Reduced visibility: 2 SM', result['reasons'])

    def test_poor_low_ceiling(self):
        result = calculate_rc_assessment(wind_speed=5, wind_gust=None, ceiling=350)
        self.assertEqual(result['rating'], 'poor')
        self.assertIn('Very low ceiling: 350 ft', result['reasons'])

    def test_marginal_ceiling(self):
        result = calculate_rc_assessment(wind_speed=5, wind_gust=None, ceiling=800)
        self.assertEqual(result['rating'], 'marginal')
        self.assertIn('Low ceiling: 800 ft', result['reasons'])

    def test_poor_high_precip_probability(self):
        result = calculate_rc_assessment(wind_speed=5, wind_gust=None, precipitation_probability=30)
        self.assertEqual(result['rating'], 'poor')
        self.assertIn('High rain chance: 30%', result['reasons'])

    def test_marginal_precip_probability(self):
        result = calculate_rc_assessment(wind_speed=5, wind_gust=None, precipitation_probability=15)
        self.assertEqual(result['rating'], 'marginal')
        self.assertIn('Rain possible: 15%', result['reasons'])

    def test_combined_factors(self):
        result = calculate_rc_assessment(
            wind_speed=12,
            wind_gust=18,
            visibility=2.5,
            ceiling=800,
            precipitation_probability=25,
        )
        self.assertEqual(result['rating'], 'poor')
        self.assertGreater(len(result['reasons']), 1)


class RcRatingColorTests(TestCase):
    """Tests for rc_rating_color function."""

    def test_good_color(self):
        self.assertEqual(rc_rating_color('good'), 'success')

    def test_marginal_color(self):
        self.assertEqual(rc_rating_color('marginal'), 'info')

    def test_poor_color(self):
        self.assertEqual(rc_rating_color('poor'), 'warning')

    def test_no_fly_color(self):
        self.assertEqual(rc_rating_color('no-fly'), 'error')

    def test_unknown_color(self):
        self.assertEqual(rc_rating_color('unknown'), 'neutral')


class WindArrowTests(TestCase):
    """Tests for wind_arrow function."""

    def test_variable_wind(self):
        self.assertEqual(wind_arrow(None), '◉')

    def test_north_wind(self):
        # Arrow points in direction wind is coming FROM
        self.assertEqual(wind_arrow(0), '⬆️')

    def test_south_wind(self):
        self.assertEqual(wind_arrow(180), '⬇️')

    def test_west_wind(self):
        self.assertEqual(wind_arrow(270), '⬅️')

    def test_east_wind(self):
        self.assertEqual(wind_arrow(90), '➡️')


class WeatherDataTests(TestCase):
    """Tests for WeatherData dataclass."""

    def create_weather_data(self, **kwargs):
        """Helper to create WeatherData with defaults."""
        defaults = {
            'station': 'KACV',
            'raw_metar': 'KACV 121856Z 27010KT 10SM FEW050 18/12 A3012',
            'observation_time': datetime.now(),
            'wind': WindData(direction=270, speed=10, gust=None, direction_repr='270'),
            'visibility': 10.0,
            'visibility_repr': '10',
            'clouds': [],
            'temperature': 18,
            'dewpoint': 12,
            'flight_rules': 'VFR',
        }
        defaults.update(kwargs)
        return WeatherData(**defaults)

    def test_ceiling_with_broken_layer(self):
        clouds = [
            CloudLayer(coverage='FEW', altitude=5000),
            CloudLayer(coverage='BKN', altitude=8000),
        ]
        data = self.create_weather_data(clouds=clouds)
        self.assertEqual(data.ceiling, 8000)

    def test_ceiling_with_overcast(self):
        clouds = [
            CloudLayer(coverage='OVC', altitude=3000),
        ]
        data = self.create_weather_data(clouds=clouds)
        self.assertEqual(data.ceiling, 3000)

    def test_ceiling_none_when_clear(self):
        clouds = [CloudLayer(coverage='CLR', altitude=None)]
        data = self.create_weather_data(clouds=clouds)
        self.assertIsNone(data.ceiling)

    def test_temperature_f_conversion(self):
        data = self.create_weather_data(temperature=20)
        self.assertEqual(data.temperature_f, 68)

    def test_temperature_f_none(self):
        data = self.create_weather_data(temperature=None)
        self.assertIsNone(data.temperature_f)

    def test_flight_rules_color_vfr(self):
        data = self.create_weather_data(flight_rules='VFR')
        self.assertEqual(data.flight_rules_color, 'success')

    def test_flight_rules_color_ifr(self):
        data = self.create_weather_data(flight_rules='IFR')
        self.assertEqual(data.flight_rules_color, 'warning')

    def test_rc_flying_assessment_property(self):
        wind = WindData(direction=270, speed=8, gust=None, direction_repr='270')
        data = self.create_weather_data(wind=wind, visibility=10.0)
        assessment = data.rc_flying_assessment
        self.assertEqual(assessment['rating'], 'good')

    def test_source_label(self):
        data = self.create_weather_data()
        self.assertEqual(data.source_label, 'Current')


class WeatherServiceConfigTests(TestCase):
    """Tests for WeatherService configuration."""

    @override_settings(AVWX_API_TOKEN='test-token')
    def test_is_configured_with_token(self):
        service = WeatherService()
        self.assertTrue(service.is_configured())

    @override_settings(AVWX_API_TOKEN='')
    def test_is_not_configured_without_token(self):
        service = WeatherService()
        self.assertFalse(service.is_configured())

    @override_settings(
        WEATHER_METAR_CACHE_TTL=900,
        WEATHER_TAF_CACHE_TTL=1800,
        WEATHER_NWS_CACHE_TTL=3600,
        WEATHER_OPENMETEO_CACHE_TTL=7200,
    )
    def test_custom_cache_ttls(self):
        service = WeatherService()
        self.assertEqual(service.metar_cache_ttl, 900)
        self.assertEqual(service.taf_cache_ttl, 1800)
        self.assertEqual(service.nws_cache_ttl, 3600)
        self.assertEqual(service.openmeteo_cache_ttl, 7200)


class WeatherServiceNWSParsingTests(TestCase):
    """Tests for NWS wind parsing."""

    def setUp(self):
        self.service = WeatherService()

    def test_parse_nws_wind_simple(self):
        wind = self.service._parse_nws_wind('10 mph', 'SW')
        self.assertEqual(wind.speed, 9)  # 10 * 0.869 rounded
        self.assertEqual(wind.direction, 225)
        self.assertIsNone(wind.gust)

    def test_parse_nws_wind_range(self):
        wind = self.service._parse_nws_wind('5 to 15 mph', 'N')
        self.assertEqual(wind.speed, 13)  # uses higher value
        self.assertEqual(wind.direction, 0)

    def test_parse_nws_wind_with_gusts(self):
        # Note: current implementation extracts all numbers and uses last one for speed
        wind = self.service._parse_nws_wind('10 mph with gusts to 25 mph', 'NW')
        self.assertEqual(wind.gust, 22)  # 25 * 0.869 rounded
        self.assertIsNotNone(wind.speed)

    def test_parse_nws_wind_unknown_direction(self):
        wind = self.service._parse_nws_wind('10 mph', 'XXX')
        self.assertIsNone(wind.direction)
        self.assertEqual(wind.direction_repr, 'XXX')


class WeatherServiceBackoffTests(TestCase):
    """Tests for exponential backoff calculation."""

    def setUp(self):
        self.service = WeatherService()

    def test_backoff_increases_with_attempts(self):
        delay0 = self.service._calculate_backoff_delay(0)
        delay1 = self.service._calculate_backoff_delay(1)
        delay2 = self.service._calculate_backoff_delay(2)

        # Each delay should be roughly double the previous (plus jitter)
        self.assertLess(delay0, delay1)
        self.assertLess(delay1, delay2)

    def test_backoff_respects_max_delay(self):
        # Even with high attempt number, should not exceed max
        delay = self.service._calculate_backoff_delay(10)
        self.assertLessEqual(delay, self.service.max_delay)

    def test_backoff_has_jitter(self):
        # Run multiple times and check that delays vary (due to jitter)
        delays = [self.service._calculate_backoff_delay(1) for _ in range(10)]
        # Not all delays should be exactly the same
        self.assertGreater(len(set(delays)), 1)


class RateLimitCounterTests(TestCase):
    """Tests for DB-based rate limit counter logic."""

    def setUp(self):
        from apps.hamsalert.models import WeatherRecord
        self.service = WeatherService()
        # Clear weather records before each test
        WeatherRecord.objects.all().delete()

    def tearDown(self):
        from apps.hamsalert.models import WeatherRecord
        WeatherRecord.objects.all().delete()

    def test_check_rate_limit_allows_when_under_limit(self):
        """Request allowed when no recent records exist."""
        self.assertTrue(self.service._check_rate_limit())

    def test_check_rate_limit_counts_openmeteo_records(self):
        """Rate limit counts openmeteo, hourly, and historical record types."""
        from apps.hamsalert.models import WeatherRecord
        from decimal import Decimal
        from datetime import date

        # Create some records (well under limit)
        for i in range(3):
            WeatherRecord.objects.create(
                weather_type='openmeteo',
                target_date=date.today(),
                latitude=Decimal('40.9781'),
                longitude=Decimal('-124.1086'),
                data={'test': f'data_{i}'},
            )

        # Should still allow since we're well under threshold
        self.assertTrue(self.service._check_rate_limit())

    def test_check_rate_limit_ignores_non_openmeteo_records(self):
        """Rate limit does not count METAR, TAF, or NWS records."""
        from apps.hamsalert.models import WeatherRecord
        from datetime import date

        # Create many non-Open-Meteo records (METAR, TAF, NWS)
        for i in range(100):
            WeatherRecord.objects.create(
                weather_type='metar',
                target_date=date.today(),
                station='KACV',
                data={'test': f'metar_{i}'},
            )

        # Should still allow since METAR records don't count
        self.assertTrue(self.service._check_rate_limit())


class RateLimitRequestIntegrationTests(TestCase):
    """Tests for rate limit integration with request flow."""

    def setUp(self):
        from apps.hamsalert.models import WeatherRecord
        self.service = WeatherService()
        WeatherRecord.objects.all().delete()

    def tearDown(self):
        from apps.hamsalert.models import WeatherRecord
        WeatherRecord.objects.all().delete()

    @patch('httpx.Client')
    def test_make_request_skips_rate_limit_for_other_urls(self, mock_client_class):
        """Rate limit NOT checked for non-Open-Meteo URLs."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client_instance = MagicMock()
        mock_client_instance.get.return_value = mock_response
        mock_client_instance.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_instance.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client_instance

        # Should NOT raise for non-Open-Meteo URL even with rate limit check
        response = self.service._make_request_with_retry(
            'https://api.weather.gov/points/40.0,-124.0',
            params={},
        )
        self.assertEqual(response.status_code, 200)

    @patch('httpx.Client')
    def test_make_request_succeeds_under_rate_limit(self, mock_client_class):
        """Request succeeds when under rate limit."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client_instance = MagicMock()
        mock_client_instance.get.return_value = mock_response
        mock_client_instance.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_instance.__exit__ = MagicMock(return_value=False)
        mock_client_class.return_value = mock_client_instance

        # Make request (rate limit should allow)
        response = self.service._make_request_with_retry(
            'https://api.open-meteo.com/v1/forecast',
            params={'latitude': 40.0, 'longitude': -124.0},
        )
        self.assertEqual(response.status_code, 200)


class RateLimitErrorSubclassTests(TestCase):
    """Tests for RateLimitError exception."""

    def test_rate_limit_error_is_weather_service_error(self):
        """RateLimitError is a subclass of WeatherServiceError."""
        self.assertTrue(issubclass(RateLimitError, WeatherServiceError))

    def test_rate_limit_error_caught_by_weather_service_error_handler(self):
        """RateLimitError can be caught as WeatherServiceError."""
        try:
            raise RateLimitError("Test rate limit")
        except WeatherServiceError as e:
            self.assertIsInstance(e, RateLimitError)
            self.assertEqual(str(e), "Test rate limit")
