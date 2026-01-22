"""
AVWX Weather API Integration Service

Provides METAR weather data for R/C flying conditions assessment.
Implements caching to stay within API rate limits (4,000 requests/month).
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import httpx
from django.conf import settings
from django.core.cache import cache

logger = logging.getLogger(__name__)


@dataclass
class WindData:
    """Wind information extracted from METAR."""
    direction: Optional[int]  # degrees (0-360), None if variable/calm
    speed: int  # knots
    gust: Optional[int]  # knots, None if no gusts
    direction_repr: str  # "270" or "VRB" for variable

    @property
    def is_gusty(self) -> bool:
        return self.gust is not None and self.gust > self.speed

    @property
    def gust_factor(self) -> Optional[int]:
        """Difference between gust and sustained wind."""
        if self.gust:
            return self.gust - self.speed
        return None


@dataclass
class CloudLayer:
    """Single cloud layer from METAR."""
    coverage: str  # FEW, SCT, BKN, OVC, CLR, SKC
    altitude: Optional[int]  # feet AGL, None for CLR/SKC

    @property
    def coverage_text(self) -> str:
        coverage_map = {
            'FEW': 'Few',
            'SCT': 'Scattered',
            'BKN': 'Broken',
            'OVC': 'Overcast',
            'CLR': 'Clear',
            'SKC': 'Sky Clear',
            'VV': 'Vertical Vis',
        }
        return coverage_map.get(self.coverage, self.coverage)


@dataclass
class WeatherData:
    """Parsed METAR data relevant to R/C flying."""
    station: str
    raw_metar: str
    observation_time: datetime
    wind: WindData
    visibility: float  # statute miles
    visibility_repr: str
    clouds: list[CloudLayer] = field(default_factory=list)
    temperature: Optional[int] = None  # Celsius
    dewpoint: Optional[int] = None
    flight_rules: str = 'VFR'
    cached_at: datetime = field(default_factory=datetime.now)
    from_cache: bool = False

    @property
    def ceiling(self) -> Optional[int]:
        """Return ceiling height (BKN or OVC layer altitude)."""
        for layer in self.clouds:
            if layer.coverage in ('BKN', 'OVC', 'VV'):
                return layer.altitude
        return None

    @property
    def flight_rules_color(self) -> str:
        """Return DaisyUI color class for flight rules."""
        colors = {
            'VFR': 'success',
            'MVFR': 'info',
            'IFR': 'warning',
            'LIFR': 'error',
        }
        return colors.get(self.flight_rules, 'neutral')

    @property
    def rc_flying_assessment(self) -> dict:
        """Assess conditions specifically for R/C flying."""
        reasons = []
        rating = 'good'

        # Wind assessment (most critical for R/C)
        if self.wind.speed >= 20:
            rating = 'no-fly'
            reasons.append(f"Wind too strong: {self.wind.speed} kt")
        elif self.wind.speed >= 15:
            rating = 'poor'
            reasons.append(f"High wind: {self.wind.speed} kt")
        elif self.wind.speed >= 10:
            if rating == 'good':
                rating = 'marginal'
            reasons.append(f"Moderate wind: {self.wind.speed} kt")

        # Gust assessment
        if self.wind.gust:
            if self.wind.gust >= 25:
                rating = 'no-fly'
                reasons.append(f"Dangerous gusts: {self.wind.gust} kt")
            elif self.wind.gust >= 20:
                if rating in ('good', 'marginal'):
                    rating = 'poor'
                reasons.append(f"Strong gusts: {self.wind.gust} kt")
            elif self.wind.gust_factor and self.wind.gust_factor >= 10:
                if rating == 'good':
                    rating = 'marginal'
                reasons.append(f"Gusty: {self.wind.gust_factor} kt spread")

        # Visibility assessment
        if self.visibility < 1:
            rating = 'no-fly'
            reasons.append(f"Very low visibility: {self.visibility} SM")
        elif self.visibility < 3:
            if rating in ('good', 'marginal'):
                rating = 'poor'
            reasons.append(f"Reduced visibility: {self.visibility} SM")

        # Ceiling assessment
        if self.ceiling:
            if self.ceiling < 500:
                if rating in ('good', 'marginal'):
                    rating = 'poor'
                reasons.append(f"Very low ceiling: {self.ceiling} ft")
            elif self.ceiling < 1000:
                if rating == 'good':
                    rating = 'marginal'
                reasons.append(f"Low ceiling: {self.ceiling} ft")

        if not reasons:
            reasons.append("Good flying conditions")

        return {'rating': rating, 'reasons': reasons}

    @property
    def rc_rating_color(self) -> str:
        """DaisyUI color class for R/C flying rating."""
        colors = {
            'good': 'success',
            'marginal': 'info',
            'poor': 'warning',
            'no-fly': 'error',
        }
        return colors.get(self.rc_flying_assessment['rating'], 'neutral')

    @property
    def wind_arrow(self) -> str:
        """Return arrow character indicating wind direction."""
        if self.wind.direction is None:
            return '○'  # Variable/calm
        # Wind FROM direction, arrow shows where it's going TO
        arrow_direction = (self.wind.direction + 180) % 360
        arrows = ['↓', '↙', '←', '↖', '↑', '↗', '→', '↘']
        index = round(arrow_direction / 45) % 8
        return arrows[index]


class WeatherServiceError(Exception):
    """Base exception for weather service errors."""
    pass


class WeatherService:
    """Service for fetching and caching METAR weather data from AVWX API."""

    CACHE_KEY_PREFIX = 'avwx_metar_'

    def __init__(self):
        self.api_token = getattr(settings, 'AVWX_API_TOKEN', '')
        self.base_url = 'https://avwx.rest/api'
        self.default_station = getattr(settings, 'AVWX_DEFAULT_STATION', 'KJFK')
        self.cache_ttl = getattr(settings, 'AVWX_CACHE_TTL', 1800)

    def _cache_key(self, station: str) -> str:
        return f"{self.CACHE_KEY_PREFIX}{station.upper()}"

    def get_weather(self, station: Optional[str] = None) -> Optional[WeatherData]:
        """Get METAR weather data for a station."""
        station = (station or self.default_station).upper()
        cache_key = self._cache_key(station)

        # Try cache first
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            cached_data.from_cache = True
            logger.debug(f"Weather cache hit for {station}")
            return cached_data

        # Fetch from API
        logger.info(f"Fetching weather for {station} from AVWX API")
        try:
            data = self._fetch_from_api(station)
            if data:
                cache.set(cache_key, data, self.cache_ttl)
            return data
        except WeatherServiceError:
            raise

    def _fetch_from_api(self, station: str) -> Optional[WeatherData]:
        """Fetch METAR data from AVWX API."""
        if not self.api_token:
            logger.warning("AVWX_API_TOKEN not configured")
            raise WeatherServiceError("Weather API not configured")

        url = f"{self.base_url}/metar/{station}"
        headers = {'Authorization': f'Token {self.api_token}'}

        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.get(url, headers=headers)

                if response.status_code == 401:
                    raise WeatherServiceError("Invalid API token")
                elif response.status_code == 404:
                    logger.warning(f"Station not found: {station}")
                    return None
                elif response.status_code == 429:
                    raise WeatherServiceError("API rate limit exceeded")
                elif response.status_code != 200:
                    raise WeatherServiceError(f"API error: {response.status_code}")

                return self._parse_response(response.json())

        except httpx.TimeoutException:
            raise WeatherServiceError("API request timed out")
        except httpx.RequestError as e:
            raise WeatherServiceError(f"API request failed: {e}")

    def _parse_response(self, data: dict) -> WeatherData:
        """Parse AVWX API response into WeatherData."""
        try:
            # Parse wind
            wind_dir = data.get('wind_direction') or {}
            wind_speed = data.get('wind_speed') or {}
            wind_gust = data.get('wind_gust') or {}

            wind = WindData(
                direction=wind_dir.get('value'),
                speed=wind_speed.get('value', 0) or 0,
                gust=wind_gust.get('value') if wind_gust else None,
                direction_repr=wind_dir.get('repr', 'VRB'),
            )

            # Parse clouds
            clouds = []
            for cloud in data.get('clouds') or []:
                clouds.append(CloudLayer(
                    coverage=cloud.get('type', 'CLR'),
                    altitude=cloud.get('altitude'),
                ))

            # Parse visibility
            vis = data.get('visibility') or {}
            visibility = vis.get('value', 10) or 10
            visibility_repr = vis.get('repr', str(visibility))

            # Parse temperature
            temp = data.get('temperature') or {}
            dewpoint = data.get('dewpoint') or {}

            # Parse observation time
            time_data = data.get('time') or {}
            obs_time_str = time_data.get('dt', '')
            try:
                obs_time = datetime.fromisoformat(obs_time_str.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                obs_time = datetime.now()

            return WeatherData(
                station=data.get('station', ''),
                raw_metar=data.get('raw', ''),
                observation_time=obs_time,
                wind=wind,
                visibility=visibility,
                visibility_repr=visibility_repr,
                clouds=clouds,
                temperature=temp.get('value'),
                dewpoint=dewpoint.get('value'),
                flight_rules=data.get('flight_rules', 'VFR'),
                cached_at=datetime.now(),
            )

        except (KeyError, TypeError) as e:
            logger.error(f"Failed to parse METAR response: {e}")
            raise WeatherServiceError(f"Failed to parse weather data: {e}")

    def clear_cache(self, station: Optional[str] = None) -> None:
        """Clear cached weather data."""
        station = (station or self.default_station).upper()
        cache.delete(self._cache_key(station))

    def is_configured(self) -> bool:
        """Check if the weather service is properly configured."""
        return bool(self.api_token)
