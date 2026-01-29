"""
Weather API Integration Service

Provides weather data from multiple sources for R/C flying conditions assessment:
- METAR (AVWX) - Current conditions (today)
- TAF (AVWX) - Aviation forecast (~24-30h, tomorrow)
- NWS API - 7-day forecast (2-7 days out)
- Open-Meteo - 16-day extended forecast (8-16 days out)

Implements caching to stay within API rate limits.
"""

import logging
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Optional
from zoneinfo import ZoneInfo

import httpx
from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)


class WeatherSource(Enum):
    """Source of weather data."""
    METAR = 'metar'
    TAF = 'taf'
    NWS = 'nws'
    OPENMETEO = 'openmeteo'
    HISTORICAL = 'historical'
    UNAVAILABLE = 'unavailable'


@dataclass
class WindData:
    """Wind information extracted from weather data."""
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

    @property
    def direction_compass(self) -> str:
        """Return 16-point compass direction (N, NNE, NE, etc.)."""
        if self.direction is None:
            return 'VRB'
        directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                      'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
        index = round(self.direction / 22.5) % 16
        return directions[index]


@dataclass
class CloudLayer:
    """Single cloud layer from METAR/TAF."""
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


def calculate_rc_assessment(
    wind_speed: int,
    wind_gust: Optional[int],
    visibility: Optional[float] = None,
    ceiling: Optional[int] = None,
    precipitation_probability: Optional[int] = None,
) -> dict:
    """
    Calculate R/C flying assessment from weather parameters.

    Works with all weather sources (METAR, TAF, NWS, Open-Meteo).
    """
    reasons = []
    rating = 'good'

    # Wind assessment (most critical for R/C)
    if wind_speed >= 20:
        rating = 'no-fly'
        reasons.append(f"Wind too strong: {wind_speed} kt")
    elif wind_speed >= 15:
        rating = 'poor'
        reasons.append(f"High wind: {wind_speed} kt")
    elif wind_speed >= 10:
        if rating == 'good':
            rating = 'marginal'
        reasons.append(f"Moderate wind: {wind_speed} kt")

    # Gust assessment
    if wind_gust:
        gust_factor = wind_gust - wind_speed
        if wind_gust >= 25:
            rating = 'no-fly'
            reasons.append(f"Dangerous gusts: {wind_gust} kt")
        elif wind_gust >= 20:
            if rating in ('good', 'marginal'):
                rating = 'poor'
            reasons.append(f"Strong gusts: {wind_gust} kt")
        elif gust_factor >= 10:
            if rating == 'good':
                rating = 'marginal'
            reasons.append(f"Gusty: {gust_factor} kt spread")

    # Visibility assessment
    if visibility is not None:
        if visibility < 1:
            rating = 'no-fly'
            reasons.append(f"Very low visibility: {visibility} SM")
        elif visibility < 3:
            if rating in ('good', 'marginal'):
                rating = 'poor'
            reasons.append(f"Reduced visibility: {visibility} SM")

    # Ceiling assessment
    if ceiling is not None:
        if ceiling < 400:
            if rating in ('good', 'marginal'):
                rating = 'poor'
            reasons.append(f"Very low ceiling: {ceiling} ft")
        elif ceiling < 1000:
            if rating == 'good':
                rating = 'marginal'
            reasons.append(f"Low ceiling: {ceiling} ft")

    # Precipitation probability (for forecasts)
    if precipitation_probability is not None:
        if precipitation_probability >= 25:
            if rating in ('good', 'marginal'):
                rating = 'poor'
            reasons.append(f"High rain chance: {precipitation_probability}%")
        elif precipitation_probability >= 10:
            if rating == 'good':
                rating = 'marginal'
            reasons.append(f"Rain possible: {precipitation_probability}%")

    return {'rating': rating, 'reasons': reasons}


def rc_rating_color(rating: str) -> str:
    """DaisyUI color class for R/C flying rating."""
    colors = {
        'good': 'success',
        'marginal': 'info',
        'poor': 'warning',
        'no-fly': 'error',
    }
    return colors.get(rating, 'neutral')


def wind_arrow(direction: Optional[int]) -> str:
    """Return arrow character indicating wind direction."""
    if direction is None:
        return '◉'  # Variable/calm
    # Wind FROM direction, arrow shows where it's going TO
    arrow_direction = (direction + 180) % 360
    # Heavy filled arrows for visibility
    arrows = ['⬇️', '↙️', '⬅️', '↖️', '⬆️', '↗️', '➡️', '↘️']
    index = round(arrow_direction / 45) % 8
    return arrows[index]


WMO_WEATHER_CODES = {
    0: 'Clear sky',
    1: 'Mainly clear',
    2: 'Partly cloudy',
    3: 'Overcast',
    45: 'Fog',
    48: 'Rime fog',
    51: 'Light drizzle',
    53: 'Moderate drizzle',
    55: 'Dense drizzle',
    56: 'Light freezing drizzle',
    57: 'Dense freezing drizzle',
    61: 'Slight rain',
    63: 'Moderate rain',
    65: 'Heavy rain',
    66: 'Light freezing rain',
    67: 'Heavy freezing rain',
    71: 'Slight snow',
    73: 'Moderate snow',
    75: 'Heavy snow',
    77: 'Snow grains',
    80: 'Slight showers',
    81: 'Moderate showers',
    82: 'Violent showers',
    85: 'Slight snow showers',
    86: 'Heavy snow showers',
    95: 'Thunderstorm',
    96: 'Thunderstorm w/ slight hail',
    99: 'Thunderstorm w/ heavy hail',
}


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
    cached_at: datetime = field(default_factory=timezone.now)
    from_cache: bool = False
    source: WeatherSource = WeatherSource.METAR

    @property
    def ceiling(self) -> Optional[int]:
        """Return ceiling height (BKN or OVC layer altitude)."""
        for layer in self.clouds:
            if layer.coverage in ('BKN', 'OVC', 'VV'):
                return layer.altitude
        return None

    @property
    def temperature_f(self) -> Optional[int]:
        """Return temperature in Fahrenheit."""
        if self.temperature is None:
            return None
        return round(self.temperature * 9 / 5 + 32)

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
        return calculate_rc_assessment(
            wind_speed=self.wind.speed,
            wind_gust=self.wind.gust,
            visibility=self.visibility,
            ceiling=self.ceiling,
        )

    @property
    def rc_rating_color(self) -> str:
        return rc_rating_color(self.rc_flying_assessment['rating'])

    @property
    def wind_arrow(self) -> str:
        return wind_arrow(self.wind.direction)

    @property
    def source_label(self) -> str:
        return 'Current'


@dataclass
class TafForecastPeriod:
    """Single forecast period from TAF."""
    start_time: datetime
    end_time: datetime
    wind: WindData
    visibility: float  # statute miles
    clouds: list[CloudLayer] = field(default_factory=list)
    flight_rules: str = 'VFR'
    raw_line: str = ''

    @property
    def ceiling(self) -> Optional[int]:
        for layer in self.clouds:
            if layer.coverage in ('BKN', 'OVC', 'VV'):
                return layer.altitude
        return None


@dataclass
class TafForecastData:
    """TAF forecast data for a specific date."""
    station: str
    raw_taf: str
    issue_time: datetime
    target_date: date
    period: TafForecastPeriod  # The applicable period for target date
    wind: WindData  # Convenience access to period wind
    visibility: float
    clouds: list[CloudLayer] = field(default_factory=list)
    flight_rules: str = 'VFR'
    cached_at: datetime = field(default_factory=timezone.now)
    from_cache: bool = False
    source: WeatherSource = WeatherSource.TAF

    @property
    def ceiling(self) -> Optional[int]:
        return self.period.ceiling

    @property
    def flight_rules_color(self) -> str:
        colors = {
            'VFR': 'success',
            'MVFR': 'info',
            'IFR': 'warning',
            'LIFR': 'error',
        }
        return colors.get(self.flight_rules, 'neutral')

    @property
    def rc_flying_assessment(self) -> dict:
        return calculate_rc_assessment(
            wind_speed=self.wind.speed,
            wind_gust=self.wind.gust,
            visibility=self.visibility,
            ceiling=self.ceiling,
        )

    @property
    def rc_rating_color(self) -> str:
        return rc_rating_color(self.rc_flying_assessment['rating'])

    @property
    def wind_arrow(self) -> str:
        return wind_arrow(self.wind.direction)

    @property
    def source_label(self) -> str:
        return 'TAF'


@dataclass
class NwsForecastPeriod:
    """Single period from NWS forecast."""
    name: str  # "Monday", "Monday Night", etc.
    start_time: datetime
    end_time: datetime
    temperature: int  # Fahrenheit
    temperature_unit: str
    is_daytime: bool
    wind_speed: str  # "5 to 10 mph"
    wind_direction: str  # "SW"
    short_forecast: str  # "Sunny"
    detailed_forecast: str
    precipitation_probability: Optional[int] = None


@dataclass
class NwsForecastData:
    """NWS 7-day forecast data for a specific date."""
    location: tuple[float, float]  # lat, lon
    target_date: date
    periods: list[NwsForecastPeriod]  # Applicable periods for target date
    wind: WindData  # Parsed from worst-case period
    temperature_high: Optional[int] = None  # Fahrenheit
    temperature_low: Optional[int] = None
    short_forecast: str = ''
    precipitation_probability: Optional[int] = None
    cached_at: datetime = field(default_factory=timezone.now)
    from_cache: bool = False
    source: WeatherSource = WeatherSource.NWS

    @property
    def temperature_f(self) -> Optional[int]:
        return self.temperature_high

    @property
    def rc_flying_assessment(self) -> dict:
        return calculate_rc_assessment(
            wind_speed=self.wind.speed,
            wind_gust=self.wind.gust,
            precipitation_probability=self.precipitation_probability,
        )

    @property
    def rc_rating_color(self) -> str:
        return rc_rating_color(self.rc_flying_assessment['rating'])

    @property
    def wind_arrow(self) -> str:
        return wind_arrow(self.wind.direction)

    @property
    def source_label(self) -> str:
        return 'NWS'


@dataclass
class OpenMeteoForecastData:
    """Open-Meteo extended forecast data for a specific date."""
    location: tuple[float, float]  # lat, lon
    target_date: date
    wind: WindData
    temperature_high: Optional[int] = None  # Celsius
    temperature_low: Optional[int] = None
    precipitation_probability: Optional[int] = None
    cached_at: datetime = field(default_factory=timezone.now)
    from_cache: bool = False
    source: WeatherSource = WeatherSource.OPENMETEO

    @property
    def temperature_high_f(self) -> Optional[int]:
        if self.temperature_high is None:
            return None
        return round(self.temperature_high * 9 / 5 + 32)

    @property
    def temperature_low_f(self) -> Optional[int]:
        if self.temperature_low is None:
            return None
        return round(self.temperature_low * 9 / 5 + 32)

    @property
    def temperature_f(self) -> Optional[int]:
        return self.temperature_high_f

    @property
    def rc_flying_assessment(self) -> dict:
        return calculate_rc_assessment(
            wind_speed=self.wind.speed,
            wind_gust=self.wind.gust,
            precipitation_probability=self.precipitation_probability,
        )

    @property
    def rc_rating_color(self) -> str:
        return rc_rating_color(self.rc_flying_assessment['rating'])

    @property
    def wind_arrow(self) -> str:
        return wind_arrow(self.wind.direction)

    @property
    def source_label(self) -> str:
        return 'Extended'


@dataclass
class HourlyForecastEntry:
    """Single hour of Open-Meteo hourly forecast data."""
    time: datetime
    temperature_c: Optional[float] = None
    wind_speed_kmh: Optional[float] = None
    wind_direction: Optional[int] = None
    wind_gusts_kmh: Optional[float] = None
    precipitation_probability: Optional[int] = None
    weather_code: Optional[int] = None

    @property
    def temperature_f(self) -> Optional[int]:
        if self.temperature_c is None:
            return None
        return round(self.temperature_c * 9 / 5 + 32)

    @property
    def wind_speed_kt(self) -> int:
        if self.wind_speed_kmh is None:
            return 0
        return round(self.wind_speed_kmh * 0.54)

    @property
    def wind_gusts_kt(self) -> Optional[int]:
        if self.wind_gusts_kmh is None:
            return None
        return round(self.wind_gusts_kmh * 0.54)

    @property
    def direction_compass(self) -> str:
        if self.wind_direction is None:
            return 'VRB'
        directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                      'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
        index = round(self.wind_direction / 22.5) % 16
        return directions[index]

    @property
    def wind_arrow(self) -> str:
        return wind_arrow(self.wind_direction)

    @property
    def weather_description(self) -> str:
        if self.weather_code is None:
            return '--'
        return WMO_WEATHER_CODES.get(self.weather_code, f'Code {self.weather_code}')


@dataclass
class HourlyForecastData:
    """Hourly forecast data from Open-Meteo for a single day."""
    location: tuple[float, float]
    target_date: date
    hours: list[HourlyForecastEntry] = field(default_factory=list)
    cached_at: datetime = field(default_factory=timezone.now)
    from_cache: bool = False


@dataclass
class HistoricalWeatherData:
    """Historical weather data from Open-Meteo Archive API."""
    location: tuple[float, float]  # lat, lon
    target_date: date
    wind: WindData
    temperature_high: Optional[int] = None  # Celsius
    temperature_low: Optional[int] = None
    precipitation_sum: Optional[float] = None  # mm
    cached_at: datetime = field(default_factory=timezone.now)
    from_cache: bool = False
    source: WeatherSource = WeatherSource.HISTORICAL

    @property
    def temperature_high_f(self) -> Optional[int]:
        if self.temperature_high is None:
            return None
        return round(self.temperature_high * 9 / 5 + 32)

    @property
    def temperature_low_f(self) -> Optional[int]:
        if self.temperature_low is None:
            return None
        return round(self.temperature_low * 9 / 5 + 32)

    @property
    def rc_flying_assessment(self) -> dict:
        return calculate_rc_assessment(
            wind_speed=self.wind.speed,
            wind_gust=self.wind.gust,
        )

    @property
    def rc_rating_color(self) -> str:
        return rc_rating_color(self.rc_flying_assessment['rating'])

    @property
    def wind_arrow(self) -> str:
        return wind_arrow(self.wind.direction)

    @property
    def source_label(self) -> str:
        return 'Historical'


@dataclass
class UnavailableWeatherData:
    """Placeholder when weather data is unavailable."""
    message: str
    source: WeatherSource = WeatherSource.UNAVAILABLE
    from_cache: bool = False

    @property
    def source_label(self) -> str:
        return 'Unavailable'


# Type alias for all weather data types
AnyWeatherData = WeatherData | TafForecastData | NwsForecastData | OpenMeteoForecastData | HistoricalWeatherData | UnavailableWeatherData


@dataclass
class CompositeWeatherData:
    """Combined weather data from all applicable sources for a date."""
    target_date: date
    metar: Optional[WeatherData] = None
    taf: Optional[TafForecastData] = None
    nws: Optional[NwsForecastData] = None
    openmeteo: Optional[OpenMeteoForecastData] = None
    historical: Optional[HistoricalWeatherData] = None
    cached_at: datetime = field(default_factory=timezone.now)
    source: WeatherSource = WeatherSource.METAR  # Primary source for compatibility

    @property
    def sources(self) -> list[WeatherSource]:
        """List of available sources."""
        result = []
        if self.metar:
            result.append(WeatherSource.METAR)
        if self.taf:
            result.append(WeatherSource.TAF)
        if self.nws:
            result.append(WeatherSource.NWS)
        if self.openmeteo:
            result.append(WeatherSource.OPENMETEO)
        if self.historical:
            result.append(WeatherSource.HISTORICAL)
        return result

    @property
    def wind(self) -> Optional[WindData]:
        """Best available wind data (METAR > TAF > NWS > OpenMeteo > Historical)."""
        if self.metar:
            return self.metar.wind
        if self.taf:
            return self.taf.wind
        if self.nws:
            return self.nws.wind
        if self.openmeteo:
            return self.openmeteo.wind
        if self.historical:
            return self.historical.wind
        return None

    @property
    def wind_source(self) -> Optional[WeatherSource]:
        """Source of wind data."""
        if self.metar:
            return WeatherSource.METAR
        if self.taf:
            return WeatherSource.TAF
        if self.nws:
            return WeatherSource.NWS
        if self.openmeteo:
            return WeatherSource.OPENMETEO
        if self.historical:
            return WeatherSource.HISTORICAL
        return None

    @property
    def temperature_f(self) -> Optional[int]:
        """Best available temperature in Fahrenheit (METAR > NWS > OpenMeteo > Historical)."""
        if self.metar and self.metar.temperature_f is not None:
            return self.metar.temperature_f
        if self.nws and self.nws.temperature_high is not None:
            return self.nws.temperature_high
        if self.openmeteo and self.openmeteo.temperature_high_f is not None:
            return self.openmeteo.temperature_high_f
        if self.historical and self.historical.temperature_high_f is not None:
            return self.historical.temperature_high_f
        return None

    @property
    def temperature_source(self) -> Optional[WeatherSource]:
        """Source of temperature data."""
        if self.metar and self.metar.temperature_f is not None:
            return WeatherSource.METAR
        if self.nws and self.nws.temperature_high is not None:
            return WeatherSource.NWS
        if self.openmeteo and self.openmeteo.temperature_high_f is not None:
            return WeatherSource.OPENMETEO
        if self.historical and self.historical.temperature_high_f is not None:
            return WeatherSource.HISTORICAL
        return None

    @property
    def temperature_high_f(self) -> Optional[int]:
        """High temperature for forecast days or historical."""
        if self.nws and self.nws.temperature_high is not None:
            return self.nws.temperature_high
        if self.openmeteo and self.openmeteo.temperature_high_f is not None:
            return self.openmeteo.temperature_high_f
        if self.historical and self.historical.temperature_high_f is not None:
            return self.historical.temperature_high_f
        return None

    @property
    def temperature_low_f(self) -> Optional[int]:
        """Low temperature for forecast days or historical."""
        if self.nws and self.nws.temperature_low is not None:
            return self.nws.temperature_low
        if self.openmeteo and self.openmeteo.temperature_low_f is not None:
            return self.openmeteo.temperature_low_f
        if self.historical and self.historical.temperature_low_f is not None:
            return self.historical.temperature_low_f
        return None

    @property
    def ceiling(self) -> Optional[int]:
        """Best available ceiling (METAR > TAF)."""
        if self.metar:
            return self.metar.ceiling
        if self.taf:
            return self.taf.ceiling
        return None

    @property
    def ceiling_source(self) -> Optional[WeatherSource]:
        """Source of ceiling data."""
        if self.metar and self.metar.ceiling is not None:
            return WeatherSource.METAR
        if self.taf and self.taf.ceiling is not None:
            return WeatherSource.TAF
        return None

    @property
    def visibility(self) -> Optional[float]:
        """Best available visibility (METAR > TAF)."""
        if self.metar:
            return self.metar.visibility
        if self.taf:
            return self.taf.visibility
        return None

    @property
    def visibility_source(self) -> Optional[WeatherSource]:
        """Source of visibility data."""
        if self.metar:
            return WeatherSource.METAR
        if self.taf:
            return WeatherSource.TAF
        return None

    @property
    def precipitation_probability(self) -> Optional[int]:
        """Best available precipitation probability (NWS > OpenMeteo)."""
        if self.nws and self.nws.precipitation_probability is not None:
            return self.nws.precipitation_probability
        if self.openmeteo and self.openmeteo.precipitation_probability is not None:
            return self.openmeteo.precipitation_probability
        return None

    @property
    def precip_source(self) -> Optional[WeatherSource]:
        """Source of precipitation probability."""
        if self.nws and self.nws.precipitation_probability is not None:
            return WeatherSource.NWS
        if self.openmeteo and self.openmeteo.precipitation_probability is not None:
            return WeatherSource.OPENMETEO
        return None

    @property
    def flight_rules(self) -> Optional[str]:
        """Flight rules from METAR or TAF."""
        if self.metar:
            return self.metar.flight_rules
        if self.taf:
            return self.taf.flight_rules
        return None

    @property
    def flight_rules_color(self) -> str:
        """DaisyUI color for flight rules."""
        colors = {
            'VFR': 'success',
            'MVFR': 'info',
            'IFR': 'warning',
            'LIFR': 'error',
        }
        return colors.get(self.flight_rules or '', 'neutral')

    @property
    def short_forecast(self) -> Optional[str]:
        """Short forecast text from NWS."""
        if self.nws:
            return self.nws.short_forecast
        return None

    @property
    def rc_flying_assessment(self) -> dict:
        """Calculate R/C flying assessment from best available data."""
        wind = self.wind
        if not wind:
            return {'rating': 'good', 'reasons': ['No wind data available']}

        return calculate_rc_assessment(
            wind_speed=wind.speed,
            wind_gust=wind.gust,
            visibility=self.visibility,
            ceiling=self.ceiling,
            precipitation_probability=self.precipitation_probability,
        )

    @property
    def rc_rating_color(self) -> str:
        return rc_rating_color(self.rc_flying_assessment['rating'])

    @property
    def wind_arrow(self) -> str:
        wind = self.wind
        if wind:
            return wind_arrow(wind.direction)
        return '◉'

    @property
    def station(self) -> Optional[str]:
        """Station identifier from METAR or TAF."""
        if self.metar:
            return self.metar.station
        if self.taf:
            return self.taf.station
        return None

    @property
    def raw_metar(self) -> Optional[str]:
        """Raw METAR string if available."""
        if self.metar:
            return self.metar.raw_metar
        return None

    @property
    def raw_taf(self) -> Optional[str]:
        """Raw TAF string if available."""
        if self.taf:
            return self.taf.raw_taf
        return None

    @property
    def from_cache(self) -> bool:
        """True if any source was from cache."""
        if self.metar and self.metar.from_cache:
            return True
        if self.taf and self.taf.from_cache:
            return True
        if self.nws and self.nws.from_cache:
            return True
        if self.openmeteo and self.openmeteo.from_cache:
            return True
        if self.historical and self.historical.from_cache:
            return True
        return False

    @property
    def source_label(self) -> str:
        """Label showing all sources."""
        labels = []
        if self.metar:
            labels.append('METAR')
        if self.taf:
            labels.append('TAF')
        if self.nws:
            labels.append('NWS')
        if self.openmeteo:
            labels.append('Extended')
        if self.historical:
            labels.append('Historical')
        return ' + '.join(labels) if labels else 'Unavailable'

    def get_shortest_ttl(self, ttls: dict[WeatherSource, int]) -> int:
        """Get the shortest TTL from available sources."""
        min_ttl = float('inf')
        for source in self.sources:
            if source in ttls:
                min_ttl = min(min_ttl, ttls[source])
        return int(min_ttl) if min_ttl != float('inf') else 0


class WeatherServiceError(Exception):
    """Base exception for weather service errors."""
    pass


class RateLimitError(WeatherServiceError):
    """Raised when proactive rate limiting prevents an API call."""
    pass


class WeatherService:
    """Service for fetching and caching weather data from multiple sources."""

    # Rate limiting configuration
    MAX_RETRIES = 3
    BASE_DELAY = 1.0
    MAX_DELAY = 30.0

    def __init__(self):
        self.api_token = getattr(settings, 'AVWX_API_TOKEN', '')
        self.base_url = 'https://avwx.rest/api'
        self.default_station = getattr(settings, 'AVWX_DEFAULT_STATION', 'KJFK')
        self.nws_location = getattr(settings, 'NWS_DEFAULT_LOCATION', (40.9781, -124.1086))
        self.nws_user_agent = getattr(settings, 'NWS_USER_AGENT', 'HamsAlert/1.0')
        self.local_timezone = ZoneInfo(getattr(settings, 'WEATHER_LOCAL_TIMEZONE', 'America/Los_Angeles'))

        # Cache TTLs
        self.metar_cache_ttl = getattr(settings, 'WEATHER_METAR_CACHE_TTL', 1800)
        self.taf_cache_ttl = getattr(settings, 'WEATHER_TAF_CACHE_TTL', 3600)
        self.nws_cache_ttl = getattr(settings, 'WEATHER_NWS_CACHE_TTL', 7200)
        self.openmeteo_cache_ttl = getattr(settings, 'WEATHER_OPENMETEO_CACHE_TTL', 14400)
        self.historical_cache_ttl = getattr(settings, 'WEATHER_HISTORICAL_CACHE_TTL', 86400)  # 24 hours

        # Rate limiting settings (can be overridden via settings)
        self.max_retries = getattr(settings, 'OPENMETEO_MAX_RETRIES', self.MAX_RETRIES)
        self.base_delay = getattr(settings, 'OPENMETEO_BASE_DELAY', self.BASE_DELAY)
        self.max_delay = getattr(settings, 'OPENMETEO_MAX_DELAY', self.MAX_DELAY)

        # Global rate limit settings
        self.rate_limit_per_minute = getattr(settings, 'OPENMETEO_RATE_LIMIT_PER_MINUTE', 600)
        self.rate_limit_per_hour = getattr(settings, 'OPENMETEO_RATE_LIMIT_PER_HOUR', 5000)
        self.rate_limit_per_day = getattr(settings, 'OPENMETEO_RATE_LIMIT_PER_DAY', 10000)
        self.rate_limit_safety_margin = getattr(settings, 'OPENMETEO_RATE_LIMIT_SAFETY_MARGIN', 0.9)

    def _make_request_with_retry(
        self,
        url: str,
        params: dict,
        timeout: float = 10.0,
        headers: Optional[dict] = None,
    ) -> httpx.Response:
        """
        Make HTTP GET request with exponential backoff retry on 429/timeout errors.

        For Open-Meteo URLs, also checks proactive rate limits before requesting
        and increments counters after successful responses.

        Args:
            url: The URL to request
            params: Query parameters
            timeout: Request timeout in seconds
            headers: Optional HTTP headers

        Returns:
            httpx.Response on success

        Raises:
            RateLimitError if proactive rate limit threshold reached (Open-Meteo only)
            WeatherServiceError on failure after retries
        """
        # Check if this is an Open-Meteo request
        is_openmeteo = 'open-meteo.com' in url

        # Proactive rate limit check for Open-Meteo
        if is_openmeteo and not self._check_rate_limit():
            raise RateLimitError("Open-Meteo rate limit threshold reached")

        last_exception: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                with httpx.Client(timeout=timeout) as client:
                    response = client.get(url, params=params, headers=headers)

                    if response.status_code == 429:
                        if attempt < self.max_retries:
                            # Check for Retry-After header
                            retry_after = response.headers.get('Retry-After')
                            if retry_after:
                                try:
                                    delay = min(float(retry_after), self.max_delay)
                                except ValueError:
                                    delay = self._calculate_backoff_delay(attempt)
                            else:
                                delay = self._calculate_backoff_delay(attempt)

                            logger.warning(
                                f"Rate limited (429) on {url}, attempt {attempt + 1}/{self.max_retries + 1}, "
                                f"retrying in {delay:.1f}s"
                            )
                            time.sleep(delay)
                            continue

                        raise WeatherServiceError("API rate limit exceeded after retries")

                    return response

            except httpx.TimeoutException as e:
                last_exception = e
                if attempt < self.max_retries:
                    delay = self._calculate_backoff_delay(attempt)
                    logger.warning(
                        f"Request timeout on {url}, attempt {attempt + 1}/{self.max_retries + 1}, "
                        f"retrying in {delay:.1f}s"
                    )
                    time.sleep(delay)
                    continue

            except httpx.RequestError as e:
                last_exception = e
                if attempt < self.max_retries:
                    delay = self._calculate_backoff_delay(attempt)
                    logger.warning(
                        f"Request error on {url}: {e}, attempt {attempt + 1}/{self.max_retries + 1}, "
                        f"retrying in {delay:.1f}s"
                    )
                    time.sleep(delay)
                    continue

        # All retries exhausted
        if isinstance(last_exception, httpx.TimeoutException):
            raise WeatherServiceError("API request timed out after retries")
        raise WeatherServiceError(f"API request failed after retries: {last_exception}")

    def _calculate_backoff_delay(self, attempt: int) -> float:
        """
        Calculate exponential backoff delay with jitter.

        Uses exponential backoff: base * 2^attempt
        Adds random jitter (0 to base) to prevent thundering herd.

        Args:
            attempt: Current attempt number (0-indexed)

        Returns:
            Delay in seconds, capped at max_delay
        """
        base = self.base_delay * (2 ** attempt)
        jitter = random.uniform(0, self.base_delay)
        return min(base + jitter, self.max_delay)

    def _check_rate_limit(self) -> bool:
        """
        Check if we're under the Open-Meteo rate limits.

        Counts recent WeatherRecord entries to track API calls.
        Returns:
            True if under limits (OK to proceed), False if at/over threshold.
        """
        from apps.hamsalert.models import WeatherRecord

        now = timezone.now()
        openmeteo_types = ['openmeteo', 'hourly', 'historical']

        # Count requests in last minute
        minute_ago = now - timedelta(minutes=1)
        minute_count = WeatherRecord.objects.filter(
            weather_type__in=openmeteo_types,
            fetched_at__gte=minute_ago
        ).count()

        minute_threshold = int(self.rate_limit_per_minute * self.rate_limit_safety_margin)
        if minute_count >= minute_threshold:
            logger.warning(
                f"Rate limit approaching: {minute_count}/{self.rate_limit_per_minute} per minute"
            )
            return False

        # Count requests in last hour
        hour_ago = now - timedelta(hours=1)
        hour_count = WeatherRecord.objects.filter(
            weather_type__in=openmeteo_types,
            fetched_at__gte=hour_ago
        ).count()

        hour_threshold = int(self.rate_limit_per_hour * self.rate_limit_safety_margin)
        if hour_count >= hour_threshold:
            logger.warning(
                f"Rate limit approaching: {hour_count}/{self.rate_limit_per_hour} per hour"
            )
            return False

        # Count requests in last day
        day_ago = now - timedelta(days=1)
        day_count = WeatherRecord.objects.filter(
            weather_type__in=openmeteo_types,
            fetched_at__gte=day_ago
        ).count()

        day_threshold = int(self.rate_limit_per_day * self.rate_limit_safety_margin)
        if day_count >= day_threshold:
            logger.warning(
                f"Rate limit approaching: {day_count}/{self.rate_limit_per_day} per day"
            )
            return False

        return True

    def _get_from_db(
        self,
        weather_type: str,
        target_date: date,
        station: Optional[str] = None,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        max_age_seconds: Optional[int] = None,
    ) -> Optional[dict]:
        """
        Retrieve weather data from database.

        Args:
            weather_type: Type of weather data (metar, taf, nws, openmeteo, hourly, historical)
            target_date: The date the weather is for
            station: Station identifier (for METAR/TAF)
            lat: Latitude (for NWS/OpenMeteo)
            lon: Longitude (for NWS/OpenMeteo)
            max_age_seconds: Maximum age of data to return (None = any age)

        Returns:
            Stored data dict if found, None otherwise
        """
        from apps.hamsalert.models import WeatherRecord

        query = WeatherRecord.objects.filter(
            weather_type=weather_type,
            target_date=target_date,
        )

        if station:
            query = query.filter(station=station)
        elif lat is not None and lon is not None:
            # Use approximate matching for coordinates (within ~11m precision)
            query = query.filter(
                latitude__range=(Decimal(str(lat)) - Decimal('0.0001'), Decimal(str(lat)) + Decimal('0.0001')),
                longitude__range=(Decimal(str(lon)) - Decimal('0.0001'), Decimal(str(lon)) + Decimal('0.0001')),
            )

        if max_age_seconds is not None:
            cutoff = timezone.now() - timezone.timedelta(seconds=max_age_seconds)
            query = query.filter(fetched_at__gte=cutoff)

        record = query.order_by('-fetched_at').first()
        if record:
            logger.info(f"DB cache hit: {weather_type} for {target_date}")
            return record.data
        return None

    def _save_to_db(
        self,
        weather_type: str,
        target_date: date,
        data: dict,
        station: Optional[str] = None,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        api_response_time_ms: Optional[int] = None,
    ) -> None:
        """
        Update existing record or create new one.

        Args:
            weather_type: Type of weather data
            target_date: The date the weather is for
            data: Weather data dict to store
            station: Station identifier (for METAR/TAF)
            lat: Latitude (for NWS/OpenMeteo)
            lon: Longitude (for NWS/OpenMeteo)
            api_response_time_ms: API response time in milliseconds
        """
        from apps.hamsalert.models import WeatherRecord

        try:
            # Build lookup - always include all fields to match unique constraint
            # Use empty string for station and None for lat/lon when not provided
            lookup = {
                'weather_type': weather_type,
                'target_date': target_date,
                'station': station or '',
                'latitude': Decimal(str(lat)) if lat is not None else None,
                'longitude': Decimal(str(lon)) if lon is not None else None,
            }

            defaults = {
                'data': data,
                'fetched_at': timezone.now(),
                'api_response_time_ms': api_response_time_ms,
            }

            # For records with NULL lat/lon, update_or_create won't match due to NULL != NULL
            # So we need to manually check for existing records first
            if lat is None and lon is None:
                existing = WeatherRecord.objects.filter(
                    weather_type=weather_type,
                    target_date=target_date,
                    station=station or '',
                    latitude__isnull=True,
                    longitude__isnull=True,
                ).first()
                if existing:
                    for key, value in defaults.items():
                        setattr(existing, key, value)
                    existing.save()
                else:
                    WeatherRecord.objects.create(**lookup, **defaults)
            else:
                WeatherRecord.objects.update_or_create(defaults=defaults, **lookup)

            logger.debug(f"Saved {weather_type} to DB for {target_date}")
        except Exception as e:
            logger.warning(f"Failed to save weather to DB: {e}")

    def _serialize_openmeteo_data(self, data: OpenMeteoForecastData) -> dict:
        """Serialize OpenMeteoForecastData to dict for DB storage."""
        return {
            'location': list(data.location),
            'target_date': data.target_date.isoformat(),
            'wind': {
                'direction': data.wind.direction,
                'speed': data.wind.speed,
                'gust': data.wind.gust,
                'direction_repr': data.wind.direction_repr,
            },
            'temperature_high': data.temperature_high,
            'temperature_low': data.temperature_low,
            'precipitation_probability': data.precipitation_probability,
        }

    def _deserialize_openmeteo_data(self, data: dict) -> OpenMeteoForecastData:
        """Deserialize dict to OpenMeteoForecastData."""
        wind_data = data['wind']
        return OpenMeteoForecastData(
            location=tuple(data['location']),
            target_date=date.fromisoformat(data['target_date']),
            wind=WindData(
                direction=wind_data['direction'],
                speed=wind_data['speed'],
                gust=wind_data['gust'],
                direction_repr=wind_data['direction_repr'],
            ),
            temperature_high=data.get('temperature_high'),
            temperature_low=data.get('temperature_low'),
            precipitation_probability=data.get('precipitation_probability'),
            cached_at=timezone.now(),
            from_cache=True,
            source=WeatherSource.OPENMETEO,
        )

    def _serialize_hourly_data(self, data: HourlyForecastData) -> dict:
        """Serialize HourlyForecastData to dict for DB storage."""
        return {
            'location': list(data.location),
            'target_date': data.target_date.isoformat(),
            'hours': [
                {
                    'time': h.time.isoformat(),
                    'temperature_c': h.temperature_c,
                    'wind_speed_kmh': h.wind_speed_kmh,
                    'wind_direction': h.wind_direction,
                    'wind_gusts_kmh': h.wind_gusts_kmh,
                    'precipitation_probability': h.precipitation_probability,
                    'weather_code': h.weather_code,
                }
                for h in data.hours
            ],
        }

    def _deserialize_hourly_data(self, data: dict) -> HourlyForecastData:
        """Deserialize dict to HourlyForecastData."""
        hours = []
        for h in data.get('hours', []):
            hours.append(HourlyForecastEntry(
                time=datetime.fromisoformat(h['time']),
                temperature_c=h.get('temperature_c'),
                wind_speed_kmh=h.get('wind_speed_kmh'),
                wind_direction=h.get('wind_direction'),
                wind_gusts_kmh=h.get('wind_gusts_kmh'),
                precipitation_probability=h.get('precipitation_probability'),
                weather_code=h.get('weather_code'),
            ))

        return HourlyForecastData(
            location=tuple(data['location']),
            target_date=date.fromisoformat(data['target_date']),
            hours=hours,
            cached_at=timezone.now(),
            from_cache=True,
        )

    def _serialize_historical_data(self, data: HistoricalWeatherData) -> dict:
        """Serialize HistoricalWeatherData to dict for DB storage."""
        return {
            'location': list(data.location),
            'target_date': data.target_date.isoformat(),
            'wind': {
                'direction': data.wind.direction,
                'speed': data.wind.speed,
                'gust': data.wind.gust,
                'direction_repr': data.wind.direction_repr,
            },
            'temperature_high': data.temperature_high,
            'temperature_low': data.temperature_low,
            'precipitation_sum': data.precipitation_sum,
        }

    def _deserialize_historical_data(self, data: dict) -> HistoricalWeatherData:
        """Deserialize dict to HistoricalWeatherData."""
        wind_data = data['wind']
        return HistoricalWeatherData(
            location=tuple(data['location']),
            target_date=date.fromisoformat(data['target_date']),
            wind=WindData(
                direction=wind_data['direction'],
                speed=wind_data['speed'],
                gust=wind_data['gust'],
                direction_repr=wind_data['direction_repr'],
            ),
            temperature_high=data.get('temperature_high'),
            temperature_low=data.get('temperature_low'),
            precipitation_sum=data.get('precipitation_sum'),
            cached_at=timezone.now(),
            from_cache=True,
            source=WeatherSource.HISTORICAL,
        )

    def _serialize_metar_data(self, data: WeatherData) -> dict:
        """Serialize WeatherData (METAR) to dict for DB storage."""
        return {
            'station': data.station,
            'raw_metar': data.raw_metar,
            'observation_time': data.observation_time.isoformat(),
            'wind': {
                'direction': data.wind.direction,
                'speed': data.wind.speed,
                'gust': data.wind.gust,
                'direction_repr': data.wind.direction_repr,
            },
            'visibility': data.visibility,
            'visibility_repr': data.visibility_repr,
            'clouds': [
                {'coverage': c.coverage, 'altitude': c.altitude}
                for c in data.clouds
            ],
            'temperature': data.temperature,
            'dewpoint': data.dewpoint,
            'flight_rules': data.flight_rules,
        }

    def _deserialize_metar_data(self, data: dict) -> WeatherData:
        """Deserialize dict to WeatherData (METAR)."""
        wind_data = data['wind']
        obs_time_str = data['observation_time']
        try:
            obs_time = datetime.fromisoformat(obs_time_str)
        except (ValueError, AttributeError):
            obs_time = datetime.now()

        clouds = [
            CloudLayer(coverage=c['coverage'], altitude=c.get('altitude'))
            for c in data.get('clouds', [])
        ]

        return WeatherData(
            station=data['station'],
            raw_metar=data['raw_metar'],
            observation_time=obs_time,
            wind=WindData(
                direction=wind_data['direction'],
                speed=wind_data['speed'],
                gust=wind_data['gust'],
                direction_repr=wind_data['direction_repr'],
            ),
            visibility=data['visibility'],
            visibility_repr=data['visibility_repr'],
            clouds=clouds,
            temperature=data.get('temperature'),
            dewpoint=data.get('dewpoint'),
            flight_rules=data.get('flight_rules', 'VFR'),
            cached_at=timezone.now(),
            from_cache=True,
            source=WeatherSource.METAR,
        )

    def _serialize_taf_data(self, data: TafForecastData) -> dict:
        """Serialize TafForecastData to dict for DB storage."""
        return {
            'station': data.station,
            'raw_taf': data.raw_taf,
            'issue_time': data.issue_time.isoformat(),
            'target_date': data.target_date.isoformat(),
            'period': {
                'start_time': data.period.start_time.isoformat(),
                'end_time': data.period.end_time.isoformat(),
                'wind': {
                    'direction': data.period.wind.direction,
                    'speed': data.period.wind.speed,
                    'gust': data.period.wind.gust,
                    'direction_repr': data.period.wind.direction_repr,
                },
                'visibility': data.period.visibility,
                'clouds': [
                    {'coverage': c.coverage, 'altitude': c.altitude}
                    for c in data.period.clouds
                ],
                'flight_rules': data.period.flight_rules,
                'raw_line': data.period.raw_line,
            },
            'wind': {
                'direction': data.wind.direction,
                'speed': data.wind.speed,
                'gust': data.wind.gust,
                'direction_repr': data.wind.direction_repr,
            },
            'visibility': data.visibility,
            'clouds': [
                {'coverage': c.coverage, 'altitude': c.altitude}
                for c in data.clouds
            ],
            'flight_rules': data.flight_rules,
        }

    def _deserialize_taf_data(self, data: dict) -> TafForecastData:
        """Deserialize dict to TafForecastData."""
        wind_data = data['wind']
        period_data = data['period']
        period_wind = period_data['wind']

        try:
            issue_time = datetime.fromisoformat(data['issue_time'])
        except (ValueError, AttributeError):
            issue_time = datetime.now()

        try:
            period_start = datetime.fromisoformat(period_data['start_time'])
        except (ValueError, AttributeError):
            period_start = datetime.now()

        try:
            period_end = datetime.fromisoformat(period_data['end_time'])
        except (ValueError, AttributeError):
            period_end = datetime.now()

        period_clouds = [
            CloudLayer(coverage=c['coverage'], altitude=c.get('altitude'))
            for c in period_data.get('clouds', [])
        ]

        clouds = [
            CloudLayer(coverage=c['coverage'], altitude=c.get('altitude'))
            for c in data.get('clouds', [])
        ]

        period = TafForecastPeriod(
            start_time=period_start,
            end_time=period_end,
            wind=WindData(
                direction=period_wind['direction'],
                speed=period_wind['speed'],
                gust=period_wind['gust'],
                direction_repr=period_wind['direction_repr'],
            ),
            visibility=period_data['visibility'],
            clouds=period_clouds,
            flight_rules=period_data.get('flight_rules', 'VFR'),
            raw_line=period_data.get('raw_line', ''),
        )

        return TafForecastData(
            station=data['station'],
            raw_taf=data['raw_taf'],
            issue_time=issue_time,
            target_date=date.fromisoformat(data['target_date']),
            period=period,
            wind=WindData(
                direction=wind_data['direction'],
                speed=wind_data['speed'],
                gust=wind_data['gust'],
                direction_repr=wind_data['direction_repr'],
            ),
            visibility=data['visibility'],
            clouds=clouds,
            flight_rules=data.get('flight_rules', 'VFR'),
            cached_at=timezone.now(),
            from_cache=True,
            source=WeatherSource.TAF,
        )

    def _serialize_nws_data(self, data: NwsForecastData) -> dict:
        """Serialize NwsForecastData to dict for DB storage."""
        return {
            'location': list(data.location),
            'target_date': data.target_date.isoformat(),
            'periods': [
                {
                    'name': p.name,
                    'start_time': p.start_time.isoformat(),
                    'end_time': p.end_time.isoformat(),
                    'temperature': p.temperature,
                    'temperature_unit': p.temperature_unit,
                    'is_daytime': p.is_daytime,
                    'wind_speed': p.wind_speed,
                    'wind_direction': p.wind_direction,
                    'short_forecast': p.short_forecast,
                    'detailed_forecast': p.detailed_forecast,
                    'precipitation_probability': p.precipitation_probability,
                }
                for p in data.periods
            ],
            'wind': {
                'direction': data.wind.direction,
                'speed': data.wind.speed,
                'gust': data.wind.gust,
                'direction_repr': data.wind.direction_repr,
            },
            'temperature_high': data.temperature_high,
            'temperature_low': data.temperature_low,
            'short_forecast': data.short_forecast,
            'precipitation_probability': data.precipitation_probability,
        }

    def _deserialize_nws_data(self, data: dict) -> NwsForecastData:
        """Deserialize dict to NwsForecastData."""
        wind_data = data['wind']

        periods = []
        for p in data.get('periods', []):
            try:
                start_time = datetime.fromisoformat(p['start_time'])
            except (ValueError, AttributeError):
                start_time = datetime.now()
            try:
                end_time = datetime.fromisoformat(p['end_time'])
            except (ValueError, AttributeError):
                end_time = datetime.now()

            periods.append(NwsForecastPeriod(
                name=p['name'],
                start_time=start_time,
                end_time=end_time,
                temperature=p['temperature'],
                temperature_unit=p['temperature_unit'],
                is_daytime=p['is_daytime'],
                wind_speed=p['wind_speed'],
                wind_direction=p['wind_direction'],
                short_forecast=p['short_forecast'],
                detailed_forecast=p['detailed_forecast'],
                precipitation_probability=p.get('precipitation_probability'),
            ))

        return NwsForecastData(
            location=tuple(data['location']),
            target_date=date.fromisoformat(data['target_date']),
            periods=periods,
            wind=WindData(
                direction=wind_data['direction'],
                speed=wind_data['speed'],
                gust=wind_data['gust'],
                direction_repr=wind_data['direction_repr'],
            ),
            temperature_high=data.get('temperature_high'),
            temperature_low=data.get('temperature_low'),
            short_forecast=data.get('short_forecast', ''),
            precipitation_probability=data.get('precipitation_probability'),
            cached_at=timezone.now(),
            from_cache=True,
            source=WeatherSource.NWS,
        )

    def get_weather_for_date(
        self,
        target_date: date,
        station: Optional[str] = None
    ) -> AnyWeatherData:
        """
        Get weather data appropriate for the target date.

        - Today: METAR (current conditions)
        - Tomorrow: TAF (aviation forecast)
        - 2-7 days: NWS API
        - 8-15 days: Open-Meteo
        - >15 days: Unavailable
        """
        local_today = datetime.now(self.local_timezone).date()
        days_out = (target_date - local_today).days

        if days_out < 0:
            return UnavailableWeatherData("Historical weather not available")
        if days_out == 0:
            return self._get_metar(station)
        if days_out == 1:
            return self._get_taf(station, target_date)
        if days_out <= 7:
            return self._get_nws_forecast(target_date)
        if days_out <= 15:
            return self._get_openmeteo_forecast(target_date)
        return UnavailableWeatherData("Forecast not available beyond 15 days")

    def get_all_weather_for_date(
        self,
        target_date: date,
        station: Optional[str] = None
    ) -> CompositeWeatherData:
        """
        Fetch weather data from ALL applicable sources for a date.

        Data availability by day:
        - Past dates: Historical (Open-Meteo Archive)
        - Day 0 (today): METAR + TAF + NWS + OpenMeteo
        - Day 1 (tomorrow): TAF + NWS + OpenMeteo
        - Days 2-7: NWS + OpenMeteo
        - Days 8-15: OpenMeteo only
        - >15 days: None

        Uses ThreadPoolExecutor for parallel fetches on cache miss.
        """
        local_today = datetime.now(self.local_timezone).date()
        days_out = (target_date - local_today).days

        # Historical data for past dates
        if days_out < 0:
            try:
                historical = self._get_historical_weather(target_date)
                return CompositeWeatherData(
                    target_date=target_date,
                    historical=historical,
                    cached_at=timezone.now(),
                    source=WeatherSource.HISTORICAL,
                )
            except WeatherServiceError as e:
                logger.warning(f"Historical fetch failed: {e}")
                return CompositeWeatherData(target_date=target_date)

        if days_out > 15:
            return CompositeWeatherData(target_date=target_date)

        # Determine which sources to fetch
        fetch_metar = days_out == 0
        fetch_taf = days_out <= 1
        fetch_nws = days_out <= 7
        fetch_openmeteo = days_out <= 15

        # Prepare fetch tasks
        results = {
            'metar': None,
            'taf': None,
            'nws': None,
            'openmeteo': None,
        }

        def fetch_metar_safe():
            try:
                return self._get_metar(station)
            except WeatherServiceError as e:
                logger.warning(f"METAR fetch failed: {e}")
                return None

        def fetch_taf_safe():
            try:
                return self._get_taf(station, target_date)
            except WeatherServiceError as e:
                logger.warning(f"TAF fetch failed: {e}")
                return None

        def fetch_nws_safe():
            try:
                return self._get_nws_forecast(target_date)
            except WeatherServiceError as e:
                logger.warning(f"NWS fetch failed: {e}")
                return None

        def fetch_openmeteo_safe():
            try:
                return self._get_openmeteo_forecast(target_date)
            except WeatherServiceError as e:
                logger.warning(f"OpenMeteo fetch failed: {e}")
                return None

        # Use ThreadPoolExecutor for parallel fetches
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {}
            if fetch_metar:
                futures[executor.submit(fetch_metar_safe)] = 'metar'
            if fetch_taf:
                futures[executor.submit(fetch_taf_safe)] = 'taf'
            if fetch_nws:
                futures[executor.submit(fetch_nws_safe)] = 'nws'
            if fetch_openmeteo:
                futures[executor.submit(fetch_openmeteo_safe)] = 'openmeteo'

            for future in as_completed(futures):
                source_name = futures[future]
                try:
                    results[source_name] = future.result()
                except Exception as e:
                    logger.error(f"Error fetching {source_name}: {e}")
                    results[source_name] = None

        # Determine primary source for TTL calculation
        if results['metar']:
            primary_source = WeatherSource.METAR
        elif results['taf']:
            primary_source = WeatherSource.TAF
        elif results['nws']:
            primary_source = WeatherSource.NWS
        elif results['openmeteo']:
            primary_source = WeatherSource.OPENMETEO
        else:
            primary_source = WeatherSource.UNAVAILABLE

        return CompositeWeatherData(
            target_date=target_date,
            metar=results['metar'],
            taf=results['taf'],
            nws=results['nws'],
            openmeteo=results['openmeteo'],
            cached_at=timezone.now(),
            source=primary_source,
        )

    def get_weather_from_db(
        self,
        target_date: date,
        station: Optional[str] = None
    ) -> Optional[CompositeWeatherData]:
        """
        Read-only: get weather data from DB. Never calls APIs.

        Returns CompositeWeatherData from DB records only.
        Returns None if no data exists (poller hasn't run yet).

        Used by views - they should never call APIs directly.
        """
        station = (station or self.default_station).upper()
        lat, lon = self.nws_location
        local_today = datetime.now(self.local_timezone).date()
        days_out = (target_date - local_today).days

        results = {
            'metar': None,
            'taf': None,
            'nws': None,
            'openmeteo': None,
            'historical': None,
        }

        # Historical data for past dates
        if days_out < 0:
            db_data = self._get_from_db('historical', target_date, lat=lat, lon=lon)
            if db_data:
                results['historical'] = self._deserialize_historical_data(db_data)
        else:
            # Day 0: METAR
            if days_out == 0:
                db_data = self._get_from_db('metar', target_date, station=station)
                if db_data:
                    results['metar'] = self._deserialize_metar_data(db_data)

            # Days 0-1: TAF
            if days_out <= 1:
                db_data = self._get_from_db('taf', target_date, station=station)
                if db_data:
                    results['taf'] = self._deserialize_taf_data(db_data)

            # Days 2-7: NWS
            if 2 <= days_out <= 7:
                db_data = self._get_from_db('nws', target_date, lat=lat, lon=lon)
                if db_data:
                    results['nws'] = self._deserialize_nws_data(db_data)

            # Days 0-15: OpenMeteo
            if days_out <= 15:
                db_data = self._get_from_db('openmeteo', target_date, lat=lat, lon=lon)
                if db_data:
                    results['openmeteo'] = self._deserialize_openmeteo_data(db_data)

        # Check if we have any data
        has_data = any(results.values())
        if not has_data:
            return None

        # Determine primary source
        if results['metar']:
            primary_source = WeatherSource.METAR
        elif results['taf']:
            primary_source = WeatherSource.TAF
        elif results['nws']:
            primary_source = WeatherSource.NWS
        elif results['openmeteo']:
            primary_source = WeatherSource.OPENMETEO
        elif results['historical']:
            primary_source = WeatherSource.HISTORICAL
        else:
            primary_source = WeatherSource.UNAVAILABLE

        return CompositeWeatherData(
            target_date=target_date,
            metar=results['metar'],
            taf=results['taf'],
            nws=results['nws'],
            openmeteo=results['openmeteo'],
            historical=results['historical'],
            cached_at=timezone.now(),
            source=primary_source,
        )

    def get_hourly_from_db(self, target_date: date) -> Optional['HourlyForecastData']:
        """
        Read-only: get hourly forecast data from DB. Never calls APIs.

        Returns HourlyForecastData from DB records only.
        Returns None if no data exists (poller hasn't run yet).
        """
        lat, lon = self.nws_location
        db_data = self._get_from_db('hourly', target_date, lat=lat, lon=lon)
        if db_data:
            return self._deserialize_hourly_data(db_data)
        return None

    def clear_all_cache_for_date(
        self,
        target_date: date,
        station: Optional[str] = None
    ) -> None:
        """Clear all weather caches for a specific date (DB records only)."""
        from apps.hamsalert.models import WeatherRecord

        station = (station or self.default_station).upper()
        lat, lon = self.nws_location
        local_today = datetime.now(self.local_timezone).date()
        days_out = (target_date - local_today).days

        # Clear all applicable DB records
        if days_out < 0:
            WeatherRecord.objects.filter(
                weather_type='historical', target_date=target_date
            ).delete()
        if days_out == 0:
            WeatherRecord.objects.filter(
                weather_type='metar', target_date=target_date, station=station
            ).delete()
        if days_out <= 1:
            WeatherRecord.objects.filter(
                weather_type='taf', target_date=target_date, station=station
            ).delete()
        if days_out <= 7:
            WeatherRecord.objects.filter(
                weather_type='nws', target_date=target_date
            ).delete()
        if days_out <= 15:
            WeatherRecord.objects.filter(
                weather_type='openmeteo', target_date=target_date
            ).delete()
            WeatherRecord.objects.filter(
                weather_type='hourly', target_date=target_date
            ).delete()

    def get_weather(self, station: Optional[str] = None) -> Optional[WeatherData]:
        """Get METAR weather data for a station (legacy method for today)."""
        return self._get_metar(station)

    def _get_metar(self, station: Optional[str] = None) -> Optional[WeatherData]:
        """Get METAR weather data for a station with DB cache."""
        station = (station or self.default_station).upper()
        local_today = datetime.now(self.local_timezone).date()
        ttl = self.metar_cache_ttl

        # 1. Check DB cache
        db_data = self._get_from_db('metar', local_today, station=station, max_age_seconds=ttl)
        if db_data:
            logger.info(f"Cache hit: METAR {station}")
            return self._deserialize_metar_data(db_data)

        # 2. Fetch from API
        logger.info(f"API fetch: METAR {station}")
        try:
            start_time = time.time()
            data = self._fetch_metar_from_api(station)
            response_time_ms = int((time.time() - start_time) * 1000)

            if data:
                self._save_to_db(
                    'metar', local_today, self._serialize_metar_data(data),
                    station=station, api_response_time_ms=response_time_ms
                )
            return data

        except WeatherServiceError:
            # 3. Fallback to stale DB data
            stale_data = self._get_from_db('metar', local_today, station=station, max_age_seconds=None)
            if stale_data:
                logger.warning(f"Using stale DB data for METAR {station}")
                return self._deserialize_metar_data(stale_data)
            raise

    def _fetch_metar_from_api(self, station: str) -> Optional[WeatherData]:
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

                return self._parse_metar_response(response.json())

        except httpx.TimeoutException:
            raise WeatherServiceError("API request timed out")
        except httpx.RequestError as e:
            raise WeatherServiceError(f"API request failed: {e}")

    def _parse_metar_response(self, data: dict) -> WeatherData:
        """Parse AVWX METAR API response into WeatherData."""
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
                cached_at=timezone.now(),
                source=WeatherSource.METAR,
            )

        except (KeyError, TypeError) as e:
            logger.error(f"Failed to parse METAR response: {e}")
            raise WeatherServiceError(f"Failed to parse weather data: {e}")

    def _get_taf(self, station: Optional[str], target_date: date) -> Optional[TafForecastData]:
        """Get TAF forecast data for a station and target date with DB cache."""
        station = (station or self.default_station).upper()
        ttl = self.taf_cache_ttl

        # 1. Check DB cache
        db_data = self._get_from_db('taf', target_date, station=station, max_age_seconds=ttl)
        if db_data:
            logger.info(f"Cache hit: TAF {station}")
            return self._deserialize_taf_data(db_data)

        # 2. Fetch from API
        logger.info(f"API fetch: TAF {station}")
        try:
            start_time = time.time()
            data = self._fetch_taf_from_api(station, target_date)
            response_time_ms = int((time.time() - start_time) * 1000)

            if data:
                self._save_to_db(
                    'taf', target_date, self._serialize_taf_data(data),
                    station=station, api_response_time_ms=response_time_ms
                )
            return data

        except WeatherServiceError:
            # 3. Fallback to stale DB data
            stale_data = self._get_from_db('taf', target_date, station=station, max_age_seconds=None)
            if stale_data:
                logger.warning(f"Using stale DB data for TAF {station}")
                return self._deserialize_taf_data(stale_data)
            raise

    def _fetch_taf_from_api(self, station: str, target_date: date) -> Optional[TafForecastData]:
        """Fetch TAF data from AVWX API."""
        if not self.api_token:
            logger.warning("AVWX_API_TOKEN not configured")
            raise WeatherServiceError("Weather API not configured")

        url = f"{self.base_url}/taf/{station}"
        headers = {'Authorization': f'Token {self.api_token}'}

        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.get(url, headers=headers)

                if response.status_code == 401:
                    raise WeatherServiceError("Invalid API token")
                elif response.status_code == 404:
                    logger.warning(f"TAF not found for station: {station}")
                    return None
                elif response.status_code == 429:
                    raise WeatherServiceError("API rate limit exceeded")
                elif response.status_code != 200:
                    raise WeatherServiceError(f"API error: {response.status_code}")

                return self._parse_taf_response(response.json(), target_date)

        except httpx.TimeoutException:
            raise WeatherServiceError("API request timed out")
        except httpx.RequestError as e:
            raise WeatherServiceError(f"API request failed: {e}")

    def _parse_taf_response(self, data: dict, target_date: date) -> Optional[TafForecastData]:
        """Parse AVWX TAF API response into TafForecastData."""
        try:
            forecasts = data.get('forecast') or []
            if not forecasts:
                return None

            # Find the forecast period that covers the target date (midday)
            target_datetime = datetime.combine(target_date, datetime.min.time().replace(hour=12))

            applicable_period = None
            for fc in forecasts:
                start_time_str = fc.get('start_time', {}).get('dt', '')
                end_time_str = fc.get('end_time', {}).get('dt', '')

                try:
                    start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))

                    # Make target_datetime timezone-aware for comparison
                    if start_time.tzinfo is not None:
                        from datetime import timezone as dt_timezone
                        target_datetime = target_datetime.replace(tzinfo=dt_timezone.utc)

                    if start_time <= target_datetime <= end_time:
                        applicable_period = fc
                        break
                except (ValueError, AttributeError):
                    continue

            # If no exact match, use the last available period
            if applicable_period is None and forecasts:
                applicable_period = forecasts[-1]

            if applicable_period is None:
                return None

            # Parse wind from applicable period
            wind_dir = applicable_period.get('wind_direction') or {}
            wind_speed = applicable_period.get('wind_speed') or {}
            wind_gust = applicable_period.get('wind_gust') or {}

            wind = WindData(
                direction=wind_dir.get('value'),
                speed=wind_speed.get('value', 0) or 0,
                gust=wind_gust.get('value') if wind_gust else None,
                direction_repr=wind_dir.get('repr', 'VRB'),
            )

            # Parse clouds
            clouds = []
            for cloud in applicable_period.get('clouds') or []:
                clouds.append(CloudLayer(
                    coverage=cloud.get('type', 'CLR'),
                    altitude=cloud.get('altitude'),
                ))

            # Parse visibility
            vis = applicable_period.get('visibility') or {}
            visibility = vis.get('value', 10) or 10

            # Parse times
            time_data = data.get('time') or {}
            issue_time_str = time_data.get('dt', '')
            try:
                issue_time = datetime.fromisoformat(issue_time_str.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                issue_time = datetime.now()

            start_time_data = applicable_period.get('start_time') or {}
            end_time_data = applicable_period.get('end_time') or {}

            try:
                period_start = datetime.fromisoformat(
                    start_time_data.get('dt', '').replace('Z', '+00:00')
                )
            except (ValueError, AttributeError):
                period_start = datetime.now()

            try:
                period_end = datetime.fromisoformat(
                    end_time_data.get('dt', '').replace('Z', '+00:00')
                )
            except (ValueError, AttributeError):
                period_end = datetime.now()

            period = TafForecastPeriod(
                start_time=period_start,
                end_time=period_end,
                wind=wind,
                visibility=visibility,
                clouds=clouds,
                flight_rules=applicable_period.get('flight_rules', 'VFR'),
                raw_line=applicable_period.get('raw', ''),
            )

            return TafForecastData(
                station=data.get('station', ''),
                raw_taf=data.get('raw', ''),
                issue_time=issue_time,
                target_date=target_date,
                period=period,
                wind=wind,
                visibility=visibility,
                clouds=clouds,
                flight_rules=applicable_period.get('flight_rules', 'VFR'),
                cached_at=timezone.now(),
                source=WeatherSource.TAF,
            )

        except (KeyError, TypeError) as e:
            logger.error(f"Failed to parse TAF response: {e}")
            raise WeatherServiceError(f"Failed to parse TAF data: {e}")

    def _get_nws_forecast(self, target_date: date) -> Optional[NwsForecastData]:
        """Get NWS 7-day forecast for a target date with DB cache."""
        lat, lon = self.nws_location
        ttl = self.nws_cache_ttl

        # 1. Check DB cache
        db_data = self._get_from_db('nws', target_date, lat=lat, lon=lon, max_age_seconds=ttl)
        if db_data:
            logger.info(f"Cache hit: NWS {lat},{lon}")
            return self._deserialize_nws_data(db_data)

        # 2. Fetch from API
        logger.info(f"API fetch: NWS {lat},{lon}")
        try:
            start_time = time.time()
            data = self._fetch_nws_forecast(target_date)
            response_time_ms = int((time.time() - start_time) * 1000)

            if data:
                self._save_to_db(
                    'nws', target_date, self._serialize_nws_data(data),
                    lat=lat, lon=lon, api_response_time_ms=response_time_ms
                )
            return data

        except WeatherServiceError:
            # 3. Fallback to stale DB data
            stale_data = self._get_from_db('nws', target_date, lat=lat, lon=lon, max_age_seconds=None)
            if stale_data:
                logger.warning(f"Using stale DB data for NWS {lat},{lon} on {target_date}")
                return self._deserialize_nws_data(stale_data)
            raise

    def _fetch_nws_forecast(self, target_date: date) -> Optional[NwsForecastData]:
        """Fetch forecast from NWS API."""
        lat, lon = self.nws_location
        headers = {
            'User-Agent': self.nws_user_agent,
            'Accept': 'application/geo+json',
        }

        try:
            with httpx.Client(timeout=10.0) as client:
                # Step 1: Get the forecast URL from points endpoint
                points_url = f"https://api.weather.gov/points/{lat},{lon}"
                points_response = client.get(points_url, headers=headers)

                if points_response.status_code != 200:
                    logger.warning(f"NWS points API error: {points_response.status_code}")
                    raise WeatherServiceError(f"NWS API error: {points_response.status_code}")

                points_data = points_response.json()
                forecast_url = points_data.get('properties', {}).get('forecast')

                if not forecast_url:
                    raise WeatherServiceError("NWS forecast URL not found")

                # Step 2: Get the forecast
                forecast_response = client.get(forecast_url, headers=headers)

                if forecast_response.status_code != 200:
                    logger.warning(f"NWS forecast API error: {forecast_response.status_code}")
                    raise WeatherServiceError(f"NWS forecast error: {forecast_response.status_code}")

                return self._parse_nws_response(forecast_response.json(), target_date)

        except httpx.TimeoutException:
            raise WeatherServiceError("NWS API request timed out")
        except httpx.RequestError as e:
            raise WeatherServiceError(f"NWS API request failed: {e}")

    def _parse_nws_wind(self, wind_speed_str: str, wind_direction_str: str) -> WindData:
        """Parse NWS wind text into WindData."""
        # Parse wind speed: "5 to 10 mph" or "10 mph"
        speed = 0
        gust = None

        # Extract numbers from wind speed string
        numbers = re.findall(r'\d+', wind_speed_str)
        if numbers:
            # Convert mph to knots (1 mph = 0.869 knots)
            if len(numbers) >= 2:
                # Range: use higher value
                speed = round(int(numbers[-1]) * 0.869)
            else:
                speed = round(int(numbers[0]) * 0.869)

            # Check for gusts
            if 'gust' in wind_speed_str.lower():
                gust_match = re.search(r'gust[s]?\s+(?:to\s+)?(\d+)', wind_speed_str.lower())
                if gust_match:
                    gust = round(int(gust_match.group(1)) * 0.869)

        # Parse direction
        direction_map = {
            'N': 0, 'NNE': 22, 'NE': 45, 'ENE': 67,
            'E': 90, 'ESE': 112, 'SE': 135, 'SSE': 157,
            'S': 180, 'SSW': 202, 'SW': 225, 'WSW': 247,
            'W': 270, 'WNW': 292, 'NW': 315, 'NNW': 337,
        }
        direction = direction_map.get(wind_direction_str.upper())

        return WindData(
            direction=direction,
            speed=speed,
            gust=gust,
            direction_repr=wind_direction_str or 'VRB',
        )

    def _parse_nws_response(self, data: dict, target_date: date) -> Optional[NwsForecastData]:
        """Parse NWS forecast API response."""
        try:
            properties = data.get('properties', {})
            periods = properties.get('periods', [])

            if not periods:
                return None

            # Find periods that match the target date
            applicable_periods = []
            temp_high = None
            temp_low = None
            precip_prob = None

            for period in periods:
                start_time_str = period.get('startTime', '')
                try:
                    start_time = datetime.fromisoformat(start_time_str)
                    if start_time.date() == target_date:
                        nws_period = NwsForecastPeriod(
                            name=period.get('name', ''),
                            start_time=start_time,
                            end_time=datetime.fromisoformat(period.get('endTime', start_time_str)),
                            temperature=period.get('temperature', 0),
                            temperature_unit=period.get('temperatureUnit', 'F'),
                            is_daytime=period.get('isDaytime', True),
                            wind_speed=period.get('windSpeed', ''),
                            wind_direction=period.get('windDirection', ''),
                            short_forecast=period.get('shortForecast', ''),
                            detailed_forecast=period.get('detailedForecast', ''),
                            precipitation_probability=period.get('probabilityOfPrecipitation', {}).get('value'),
                        )
                        applicable_periods.append(nws_period)

                        # Track high/low temps
                        temp = period.get('temperature')
                        if temp is not None:
                            if period.get('isDaytime', True):
                                temp_high = temp
                            else:
                                temp_low = temp

                        # Track precipitation probability
                        prob = period.get('probabilityOfPrecipitation', {}).get('value')
                        if prob is not None:
                            if precip_prob is None or prob > precip_prob:
                                precip_prob = prob

                except (ValueError, AttributeError):
                    continue

            if not applicable_periods:
                # If no exact match, use first available period
                period = periods[0]
                applicable_periods.append(NwsForecastPeriod(
                    name=period.get('name', ''),
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    temperature=period.get('temperature', 0),
                    temperature_unit=period.get('temperatureUnit', 'F'),
                    is_daytime=period.get('isDaytime', True),
                    wind_speed=period.get('windSpeed', ''),
                    wind_direction=period.get('windDirection', ''),
                    short_forecast=period.get('shortForecast', ''),
                    detailed_forecast=period.get('detailedForecast', ''),
                    precipitation_probability=period.get('probabilityOfPrecipitation', {}).get('value'),
                ))
                temp_high = period.get('temperature')

            # Get wind from daytime period (usually has higher winds)
            daytime_period = next((p for p in applicable_periods if p.is_daytime), applicable_periods[0])
            wind = self._parse_nws_wind(daytime_period.wind_speed, daytime_period.wind_direction)

            return NwsForecastData(
                location=self.nws_location,
                target_date=target_date,
                periods=applicable_periods,
                wind=wind,
                temperature_high=temp_high,
                temperature_low=temp_low,
                short_forecast=daytime_period.short_forecast,
                precipitation_probability=precip_prob,
                cached_at=timezone.now(),
                source=WeatherSource.NWS,
            )

        except (KeyError, TypeError) as e:
            logger.error(f"Failed to parse NWS response: {e}")
            raise WeatherServiceError(f"Failed to parse NWS data: {e}")

    def _get_openmeteo_forecast(self, target_date: date) -> Optional[OpenMeteoForecastData]:
        """Get Open-Meteo extended forecast for a target date with DB cache."""
        lat, lon = self.nws_location  # Reuse same location
        ttl = self.openmeteo_cache_ttl

        # 1. Check DB cache
        db_data = self._get_from_db('openmeteo', target_date, lat=lat, lon=lon, max_age_seconds=ttl)
        if db_data:
            data = self._deserialize_openmeteo_data(db_data)
            return data

        # 2. Fetch from API
        logger.info(f"API fetch: OpenMeteo {lat},{lon}")
        try:
            start_time = time.time()
            data = self._fetch_openmeteo_forecast(target_date)
            response_time_ms = int((time.time() - start_time) * 1000)

            if data:
                self._save_to_db(
                    'openmeteo', target_date, self._serialize_openmeteo_data(data),
                    lat=lat, lon=lon, api_response_time_ms=response_time_ms
                )
            return data

        except WeatherServiceError:
            # 3. Fallback to stale DB data
            stale_data = self._get_from_db('openmeteo', target_date, lat=lat, lon=lon, max_age_seconds=None)
            if stale_data:
                logger.warning(f"Using stale DB data for Open-Meteo {lat},{lon} on {target_date}")
                return self._deserialize_openmeteo_data(stale_data)
            raise

    def _fetch_openmeteo_forecast(self, target_date: date) -> Optional[OpenMeteoForecastData]:
        """Fetch forecast from Open-Meteo API with retry on rate limit."""
        lat, lon = self.nws_location

        params = {
            'latitude': lat,
            'longitude': lon,
            'daily': ','.join([
                'temperature_2m_max',
                'temperature_2m_min',
                'precipitation_probability_max',
                'wind_speed_10m_max',
                'wind_gusts_10m_max',
                'wind_direction_10m_dominant',
            ]),
            'timezone': 'auto',
            'forecast_days': 16,
        }

        response = self._make_request_with_retry(
            'https://api.open-meteo.com/v1/forecast',
            params=params,
        )

        if response.status_code != 200:
            logger.warning(f"Open-Meteo API error: {response.status_code}")
            raise WeatherServiceError(f"Open-Meteo API error: {response.status_code}")

        return self._parse_openmeteo_response(response.json(), target_date)

    def _parse_openmeteo_response(self, data: dict, target_date: date) -> Optional[OpenMeteoForecastData]:
        """Parse Open-Meteo API response."""
        try:
            daily = data.get('daily', {})
            dates = daily.get('time', [])

            # Find index for target date
            target_str = target_date.isoformat()
            if target_str not in dates:
                return None

            idx = dates.index(target_str)

            # Get values for target date
            temp_max = daily.get('temperature_2m_max', [])[idx]
            temp_min = daily.get('temperature_2m_min', [])[idx]
            precip_prob = daily.get('precipitation_probability_max', [])[idx]
            wind_speed_kmh = daily.get('wind_speed_10m_max', [])[idx] or 0
            wind_gust_kmh = daily.get('wind_gusts_10m_max', [])[idx]
            wind_dir = daily.get('wind_direction_10m_dominant', [])[idx]

            # Convert km/h to knots (1 km/h = 0.54 knots)
            wind_speed_kt = round(wind_speed_kmh * 0.54)
            wind_gust_kt = round(wind_gust_kmh * 0.54) if wind_gust_kmh else None

            wind = WindData(
                direction=wind_dir,
                speed=wind_speed_kt,
                gust=wind_gust_kt,
                direction_repr=str(wind_dir) if wind_dir else 'VRB',
            )

            return OpenMeteoForecastData(
                location=self.nws_location,
                target_date=target_date,
                wind=wind,
                temperature_high=round(temp_max) if temp_max is not None else None,
                temperature_low=round(temp_min) if temp_min is not None else None,
                precipitation_probability=precip_prob,
                cached_at=timezone.now(),
                source=WeatherSource.OPENMETEO,
            )

        except (KeyError, TypeError, IndexError) as e:
            logger.error(f"Failed to parse Open-Meteo response: {e}")
            raise WeatherServiceError(f"Failed to parse Open-Meteo data: {e}")

    def get_hourly_forecast(self, target_date: date) -> Optional['HourlyForecastData']:
        """Get hourly forecast data for a target date (0-15 days out) with DB cache."""
        lat, lon = self.nws_location
        ttl = self.openmeteo_cache_ttl

        # 1. Check DB cache
        db_data = self._get_from_db('hourly', target_date, lat=lat, lon=lon, max_age_seconds=ttl)
        if db_data:
            data = self._deserialize_hourly_data(db_data)
            return data

        # 2. Fetch from API
        logger.info(f"API fetch: Hourly {lat},{lon}")
        try:
            start_time = time.time()
            data = self._fetch_hourly_forecast(target_date)
            response_time_ms = int((time.time() - start_time) * 1000)

            if data:
                self._save_to_db(
                    'hourly', target_date, self._serialize_hourly_data(data),
                    lat=lat, lon=lon, api_response_time_ms=response_time_ms
                )
            return data

        except WeatherServiceError:
            # 3. Fallback to stale DB data
            stale_data = self._get_from_db('hourly', target_date, lat=lat, lon=lon, max_age_seconds=None)
            if stale_data:
                logger.warning(f"Using stale DB data for hourly {lat},{lon} on {target_date}")
                return self._deserialize_hourly_data(stale_data)
            raise

    def _fetch_hourly_forecast(self, target_date: date) -> Optional['HourlyForecastData']:
        """Fetch hourly forecast from Open-Meteo API with retry on rate limit."""
        lat, lon = self.nws_location
        date_str = target_date.isoformat()

        params = {
            'latitude': lat,
            'longitude': lon,
            'hourly': ','.join([
                'temperature_2m',
                'wind_speed_10m',
                'wind_direction_10m',
                'wind_gusts_10m',
                'precipitation_probability',
                'weather_code',
            ]),
            'timezone': 'auto',
            'start_date': date_str,
            'end_date': date_str,
        }

        response = self._make_request_with_retry(
            'https://api.open-meteo.com/v1/forecast',
            params=params,
        )

        if response.status_code != 200:
            logger.warning(f"Open-Meteo hourly API error: {response.status_code}")
            raise WeatherServiceError(f"Open-Meteo API error: {response.status_code}")

        return self._parse_hourly_response(response.json(), target_date)

    def _parse_hourly_response(self, data: dict, target_date: date) -> Optional['HourlyForecastData']:
        """Parse Open-Meteo hourly API response."""
        try:
            hourly = data.get('hourly', {})
            times = hourly.get('time', [])

            if not times:
                return None

            temps = hourly.get('temperature_2m', [])
            wind_speeds = hourly.get('wind_speed_10m', [])
            wind_dirs = hourly.get('wind_direction_10m', [])
            wind_gusts = hourly.get('wind_gusts_10m', [])
            precip_probs = hourly.get('precipitation_probability', [])
            weather_codes = hourly.get('weather_code', [])

            hours = []
            for i, time_str in enumerate(times):
                try:
                    hour_time = datetime.fromisoformat(time_str)
                except (ValueError, AttributeError):
                    continue

                hours.append(HourlyForecastEntry(
                    time=hour_time,
                    temperature_c=temps[i] if i < len(temps) else None,
                    wind_speed_kmh=wind_speeds[i] if i < len(wind_speeds) else None,
                    wind_direction=wind_dirs[i] if i < len(wind_dirs) else None,
                    wind_gusts_kmh=wind_gusts[i] if i < len(wind_gusts) else None,
                    precipitation_probability=precip_probs[i] if i < len(precip_probs) else None,
                    weather_code=weather_codes[i] if i < len(weather_codes) else None,
                ))

            return HourlyForecastData(
                location=self.nws_location,
                target_date=target_date,
                hours=hours,
                cached_at=timezone.now(),
            )

        except (KeyError, TypeError, IndexError) as e:
            logger.error(f"Failed to parse hourly response: {e}")
            raise WeatherServiceError(f"Failed to parse hourly data: {e}")

    def _get_historical_weather(self, target_date: date) -> Optional[HistoricalWeatherData]:
        """Get historical weather data for a past date with DB cache."""
        lat, lon = self.nws_location
        ttl = self.historical_cache_ttl

        # 1. Check DB cache
        db_data = self._get_from_db('historical', target_date, lat=lat, lon=lon, max_age_seconds=ttl)
        if db_data:
            data = self._deserialize_historical_data(db_data)
            return data

        # 2. Fetch from API
        logger.info(f"API fetch: Historical {lat},{lon}")
        try:
            start_time = time.time()
            data = self._fetch_historical_weather(target_date)
            response_time_ms = int((time.time() - start_time) * 1000)

            if data:
                self._save_to_db(
                    'historical', target_date, self._serialize_historical_data(data),
                    lat=lat, lon=lon, api_response_time_ms=response_time_ms
                )
            return data

        except WeatherServiceError:
            # 3. Fallback to stale DB data
            stale_data = self._get_from_db('historical', target_date, lat=lat, lon=lon, max_age_seconds=None)
            if stale_data:
                logger.warning(f"Using stale DB data for historical {lat},{lon} on {target_date}")
                return self._deserialize_historical_data(stale_data)
            raise

    def _fetch_historical_weather(self, target_date: date) -> Optional[HistoricalWeatherData]:
        """Fetch historical weather from Open-Meteo Archive API with retry on rate limit."""
        lat, lon = self.nws_location

        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': target_date.isoformat(),
            'end_date': target_date.isoformat(),
            'daily': ','.join([
                'temperature_2m_max',
                'temperature_2m_min',
                'precipitation_sum',
                'wind_speed_10m_max',
                'wind_gusts_10m_max',
                'wind_direction_10m_dominant',
            ]),
            'timezone': 'auto',
        }

        response = self._make_request_with_retry(
            'https://archive-api.open-meteo.com/v1/archive',
            params=params,
        )

        if response.status_code != 200:
            logger.warning(f"Open-Meteo Archive API error: {response.status_code}")
            raise WeatherServiceError(f"Open-Meteo Archive API error: {response.status_code}")

        return self._parse_historical_response(response.json(), target_date)

    def _parse_historical_response(self, data: dict, target_date: date) -> Optional[HistoricalWeatherData]:
        """Parse Open-Meteo Archive API response."""
        try:
            daily = data.get('daily', {})
            dates = daily.get('time', [])

            # Find index for target date
            target_str = target_date.isoformat()
            if target_str not in dates:
                return None

            idx = dates.index(target_str)

            # Get values for target date
            temp_max = daily.get('temperature_2m_max', [])[idx]
            temp_min = daily.get('temperature_2m_min', [])[idx]
            precip_sum = daily.get('precipitation_sum', [])[idx]
            wind_speed_kmh = daily.get('wind_speed_10m_max', [])[idx] or 0
            wind_gust_kmh = daily.get('wind_gusts_10m_max', [])[idx]
            wind_dir = daily.get('wind_direction_10m_dominant', [])[idx]

            # Convert km/h to knots (1 km/h = 0.54 knots)
            wind_speed_kt = round(wind_speed_kmh * 0.54)
            wind_gust_kt = round(wind_gust_kmh * 0.54) if wind_gust_kmh else None

            wind = WindData(
                direction=wind_dir,
                speed=wind_speed_kt,
                gust=wind_gust_kt,
                direction_repr=str(wind_dir) if wind_dir else 'VRB',
            )

            return HistoricalWeatherData(
                location=self.nws_location,
                target_date=target_date,
                wind=wind,
                temperature_high=round(temp_max) if temp_max is not None else None,
                temperature_low=round(temp_min) if temp_min is not None else None,
                precipitation_sum=precip_sum,
                cached_at=timezone.now(),
                source=WeatherSource.HISTORICAL,
            )

        except (KeyError, TypeError, IndexError) as e:
            logger.error(f"Failed to parse historical response: {e}")
            raise WeatherServiceError(f"Failed to parse historical data: {e}")

    def clear_cache(self, station: Optional[str] = None, target_date: Optional[date] = None) -> None:
        """Clear cached weather data (DB records only)."""
        from apps.hamsalert.models import WeatherRecord

        station = (station or self.default_station).upper()
        local_today = datetime.now(self.local_timezone).date()

        if target_date is None:
            target_date = local_today

        days_out = (target_date - local_today).days

        # Clear appropriate DB records based on date
        if days_out == 0:
            WeatherRecord.objects.filter(
                weather_type='metar', target_date=target_date, station=station
            ).delete()
        elif days_out == 1:
            WeatherRecord.objects.filter(
                weather_type='taf', target_date=target_date, station=station
            ).delete()
        elif days_out <= 7:
            WeatherRecord.objects.filter(
                weather_type='nws', target_date=target_date
            ).delete()
        elif days_out <= 16:
            WeatherRecord.objects.filter(
                weather_type='openmeteo', target_date=target_date
            ).delete()
            WeatherRecord.objects.filter(
                weather_type='hourly', target_date=target_date
            ).delete()

    def is_configured(self) -> bool:
        """Check if the weather service is properly configured."""
        return bool(self.api_token)
