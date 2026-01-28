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
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
from django.conf import settings
from django.core.cache import cache
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
        if ceiling < 500:
            if rating in ('good', 'marginal'):
                rating = 'poor'
            reasons.append(f"Very low ceiling: {ceiling} ft")
        elif ceiling < 1000:
            if rating == 'good':
                rating = 'marginal'
            reasons.append(f"Low ceiling: {ceiling} ft")

    # Precipitation probability (for forecasts)
    if precipitation_probability is not None:
        if precipitation_probability >= 70:
            if rating in ('good', 'marginal'):
                rating = 'poor'
            reasons.append(f"High rain chance: {precipitation_probability}%")
        elif precipitation_probability >= 50:
            if rating == 'good':
                rating = 'marginal'
            reasons.append(f"Rain possible: {precipitation_probability}%")

    if not reasons:
        reasons.append("Good flying conditions")

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


class WeatherService:
    """Service for fetching and caching weather data from multiple sources."""

    CACHE_KEY_PREFIX = 'weather_'

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

    def _cache_key(self, prefix: str, identifier: str) -> str:
        return f"{self.CACHE_KEY_PREFIX}{prefix}_{identifier}"

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

    def clear_all_cache_for_date(
        self,
        target_date: date,
        station: Optional[str] = None
    ) -> None:
        """Clear all weather caches for a specific date."""
        station = (station or self.default_station).upper()
        lat, lon = self.nws_location
        local_today = datetime.now(self.local_timezone).date()
        days_out = (target_date - local_today).days

        # Clear all applicable caches
        if days_out < 0:
            cache.delete(self._cache_key('historical', f"{lat}_{lon}_{target_date.isoformat()}"))
        if days_out == 0:
            cache.delete(self._cache_key('metar', station))
        if days_out <= 1:
            cache.delete(self._cache_key('taf', f"{station}_{target_date.isoformat()}"))
        if days_out <= 7:
            cache.delete(self._cache_key('nws', f"{lat}_{lon}_{target_date.isoformat()}"))
        if days_out <= 15:
            cache.delete(self._cache_key('openmeteo', f"{lat}_{lon}_{target_date.isoformat()}"))

    def get_weather(self, station: Optional[str] = None) -> Optional[WeatherData]:
        """Get METAR weather data for a station (legacy method for today)."""
        return self._get_metar(station)

    def _get_metar(self, station: Optional[str] = None) -> Optional[WeatherData]:
        """Get METAR weather data for a station."""
        station = (station or self.default_station).upper()
        cache_key = self._cache_key('metar', station)

        # Try cache first
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            cached_data.from_cache = True
            logger.debug(f"METAR cache hit for {station}")
            return cached_data

        # Fetch from API
        logger.info(f"Fetching METAR for {station} from AVWX API")
        try:
            data = self._fetch_metar_from_api(station)
            if data:
                cache.set(cache_key, data, self.metar_cache_ttl)
            return data
        except WeatherServiceError:
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
        """Get TAF forecast data for a station and target date."""
        station = (station or self.default_station).upper()
        cache_key = self._cache_key('taf', f"{station}_{target_date.isoformat()}")

        # Try cache first
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            cached_data.from_cache = True
            logger.debug(f"TAF cache hit for {station}")
            return cached_data

        # Fetch from API
        logger.info(f"Fetching TAF for {station} from AVWX API")
        try:
            data = self._fetch_taf_from_api(station, target_date)
            if data:
                cache.set(cache_key, data, self.taf_cache_ttl)
            return data
        except WeatherServiceError:
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
        """Get NWS 7-day forecast for a target date."""
        lat, lon = self.nws_location
        cache_key = self._cache_key('nws', f"{lat}_{lon}_{target_date.isoformat()}")

        # Try cache first
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            cached_data.from_cache = True
            logger.debug(f"NWS cache hit for {lat},{lon}")
            return cached_data

        # Fetch from API
        logger.info(f"Fetching NWS forecast for {lat},{lon}")
        try:
            data = self._fetch_nws_forecast(target_date)
            if data:
                cache.set(cache_key, data, self.nws_cache_ttl)
            return data
        except WeatherServiceError:
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
        """Get Open-Meteo extended forecast for a target date."""
        lat, lon = self.nws_location  # Reuse same location
        cache_key = self._cache_key('openmeteo', f"{lat}_{lon}_{target_date.isoformat()}")

        # Try cache first
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            cached_data.from_cache = True
            logger.debug(f"Open-Meteo cache hit for {lat},{lon}")
            return cached_data

        # Fetch from API
        logger.info(f"Fetching Open-Meteo forecast for {lat},{lon}")
        try:
            data = self._fetch_openmeteo_forecast(target_date)
            if data:
                cache.set(cache_key, data, self.openmeteo_cache_ttl)
            return data
        except WeatherServiceError:
            raise

    def _fetch_openmeteo_forecast(self, target_date: date) -> Optional[OpenMeteoForecastData]:
        """Fetch forecast from Open-Meteo API."""
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

        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.get(
                    'https://api.open-meteo.com/v1/forecast',
                    params=params
                )

                if response.status_code != 200:
                    logger.warning(f"Open-Meteo API error: {response.status_code}")
                    raise WeatherServiceError(f"Open-Meteo API error: {response.status_code}")

                return self._parse_openmeteo_response(response.json(), target_date)

        except httpx.TimeoutException:
            raise WeatherServiceError("Open-Meteo API request timed out")
        except httpx.RequestError as e:
            raise WeatherServiceError(f"Open-Meteo API request failed: {e}")

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

    def _get_historical_weather(self, target_date: date) -> Optional[HistoricalWeatherData]:
        """Get historical weather data for a past date."""
        lat, lon = self.nws_location
        cache_key = self._cache_key('historical', f"{lat}_{lon}_{target_date.isoformat()}")

        # Try cache first
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            cached_data.from_cache = True
            logger.debug(f"Historical cache hit for {lat},{lon} on {target_date}")
            return cached_data

        # Fetch from API
        logger.info(f"Fetching historical weather for {lat},{lon} on {target_date}")
        try:
            data = self._fetch_historical_weather(target_date)
            if data:
                cache.set(cache_key, data, self.historical_cache_ttl)
            return data
        except WeatherServiceError:
            raise

    def _fetch_historical_weather(self, target_date: date) -> Optional[HistoricalWeatherData]:
        """Fetch historical weather from Open-Meteo Archive API."""
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

        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.get(
                    'https://archive-api.open-meteo.com/v1/archive',
                    params=params
                )

                if response.status_code != 200:
                    logger.warning(f"Open-Meteo Archive API error: {response.status_code}")
                    raise WeatherServiceError(f"Open-Meteo Archive API error: {response.status_code}")

                return self._parse_historical_response(response.json(), target_date)

        except httpx.TimeoutException:
            raise WeatherServiceError("Open-Meteo Archive API request timed out")
        except httpx.RequestError as e:
            raise WeatherServiceError(f"Open-Meteo Archive API request failed: {e}")

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
        """Clear cached weather data."""
        station = (station or self.default_station).upper()
        lat, lon = self.nws_location
        local_today = datetime.now(self.local_timezone).date()

        if target_date is None:
            target_date = local_today

        days_out = (target_date - local_today).days

        # Clear appropriate cache based on date
        if days_out == 0:
            cache.delete(self._cache_key('metar', station))
        elif days_out == 1:
            cache.delete(self._cache_key('taf', f"{station}_{target_date.isoformat()}"))
        elif days_out <= 7:
            cache.delete(self._cache_key('nws', f"{lat}_{lon}_{target_date.isoformat()}"))
        elif days_out <= 16:
            cache.delete(self._cache_key('openmeteo', f"{lat}_{lon}_{target_date.isoformat()}"))

    def is_configured(self) -> bool:
        """Check if the weather service is properly configured."""
        return bool(self.api_token)
