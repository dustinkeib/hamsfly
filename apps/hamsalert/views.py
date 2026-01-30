import calendar
from datetime import date, datetime

from django.conf import settings as django_settings
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.http import require_GET
from zoneinfo import ZoneInfo

from .models import Event
from .services import CompositeWeatherData, WeatherService, WeatherSource


# Cache TTLs by source (must match settings)
WEATHER_CACHE_TTLS = {
    WeatherSource.METAR: 1800,      # 30 min
    WeatherSource.TAF: 3600,        # 1 hour
    WeatherSource.NWS: 7200,        # 2 hours
    WeatherSource.OPENMETEO: 14400, # 4 hours
    WeatherSource.HISTORICAL: 86400, # 24 hours
}


def get_refresh_info(weather) -> dict:
    """Calculate seconds/minutes until weather cache expires."""
    if not weather or not hasattr(weather, 'cached_at'):
        return {'seconds': 0, 'minutes': 0}

    # For CompositeWeatherData, use the shortest TTL from available sources
    if isinstance(weather, CompositeWeatherData):
        ttl = weather.get_shortest_ttl(WEATHER_CACHE_TTLS)
    elif hasattr(weather, 'source'):
        ttl = WEATHER_CACHE_TTLS.get(weather.source, 0)
    else:
        ttl = 0

    if not ttl:
        return {'seconds': 0, 'minutes': 0}

    # Handle both naive and aware datetimes (for cached data)
    now = timezone.now()
    cached_at = weather.cached_at
    if timezone.is_naive(cached_at):
        cached_at = timezone.make_aware(cached_at)
    elapsed = (now - cached_at).total_seconds()
    remaining = max(int(ttl - elapsed), 60)  # minimum 60s
    return {'seconds': remaining, 'minutes': (remaining + 59) // 60}


@require_GET
def health(request):
    """Health check endpoint to keep Render free instances active."""
    return JsonResponse({'status': 'ok'})


def calendar_view(request):
    """Redirect to today's date."""
    today = date.today()
    return redirect('calendar_day', year=today.year, month=today.month, day=today.day)


def calendar_day_view(request, year, month, day):
    """Display calendar with the selected day's events and weather."""
    today = date.today()

    # Validate date - redirect to today if invalid
    try:
        selected_date = date(year, month, day)
    except ValueError:
        return redirect('calendar_day', year=today.year, month=today.month, day=today.day)

    # Get calendar data
    cal = calendar.Calendar(firstweekday=6)  # Sunday first
    month_days = cal.monthdayscalendar(year, month)

    # Get days with events for this month
    events_in_month = Event.objects.filter(
        date__year=year,
        date__month=month
    ).values_list('date', flat=True)
    days_with_events = set(d.day for d in events_in_month)

    # Navigation
    if month == 1:
        prev_month, prev_year = 12, year - 1
    else:
        prev_month, prev_year = month - 1, year

    if month == 12:
        next_month, next_year = 1, year + 1
    else:
        next_month, next_year = month + 1, year

    # Get events and weather for selected day
    events = Event.objects.filter(date=selected_date)

    weather = None
    weather_error = None
    weather_service = WeatherService()

    if weather_service.is_configured():
        station = request.GET.get('station')
        weather = weather_service.get_weather_from_db(selected_date, station)
        if not weather or not weather.sources:
            weather = None
            days_out = (selected_date - today).days
            if days_out > 15:
                weather_error = "No forecast beyond 16 days"
            elif days_out >= 0:
                weather_error = "Weather data not yet available"

    refresh = get_refresh_info(weather)

    context = {
        'year': year,
        'month': month,
        'month_name': calendar.month_name[month],
        'month_days': month_days,
        'days_with_events': days_with_events,
        'today': today,
        'selected_day': day,
        'selected_date': selected_date,
        'prev_month': prev_month,
        'prev_year': prev_year,
        'next_month': next_month,
        'next_year': next_year,
        # Day events context
        'events': events,
        'date': selected_date,
        'weather': weather,
        'weather_error': weather_error,
        'weather_configured': weather_service.is_configured(),
        'refresh_seconds': refresh['seconds'],
        'refresh_minutes': refresh['minutes'],
    }
    return render(request, 'hamsalert/calendar.html', context)


@require_GET
def weather_refresh(request):
    """HTMX endpoint to refresh weather data (reads from DB)."""
    weather = None
    weather_error = None
    weather_service = WeatherService()

    # Get target date from request params
    year = request.GET.get('year')
    month = request.GET.get('month')
    day = request.GET.get('day')

    if year and month and day:
        try:
            target_date = date(int(year), int(month), int(day))
        except (ValueError, TypeError):
            target_date = date.today()
    else:
        target_date = date.today()

    if weather_service.is_configured():
        station = request.GET.get('station')
        weather = weather_service.get_weather_from_db(target_date, station)
        if not weather or not weather.sources:
            weather = None
            days_out = (target_date - date.today()).days
            if days_out > 15:
                weather_error = "No forecast beyond 16 days"
            elif days_out >= 0:
                weather_error = "Weather data not yet available"

    refresh = get_refresh_info(weather)
    context = {
        'weather': weather,
        'weather_error': weather_error,
        'weather_configured': weather_service.is_configured(),
        'date': target_date,
        'refresh_seconds': refresh['seconds'],
        'refresh_minutes': refresh['minutes'],
    }
    return render(request, 'hamsalert/partials/weather_card.html', context)


@require_GET
def hourly_forecast(request, year, month, day):
    """Display hourly forecast page for a specific date (reads from DB)."""
    try:
        target_date = date(year, month, day)
    except ValueError:
        return render(request, 'hamsalert/hourly_forecast.html', {
            'error': 'Invalid date.',
        })

    weather_service = WeatherService()
    local_tz = ZoneInfo(getattr(django_settings, 'WEATHER_LOCAL_TIMEZONE', 'America/Los_Angeles'))
    local_today = datetime.now(local_tz).date()
    days_out = (target_date - local_today).days

    if days_out < 0 or days_out > 15:
        return render(request, 'hamsalert/hourly_forecast.html', {
            'error': 'Hourly forecast is only available for today through 15 days out.',
            'target_date': target_date,
        })

    hourly_data = weather_service.get_hourly_from_db(target_date)
    error = None
    if not hourly_data or not hourly_data.hours:
        error = 'Hourly forecast data not yet available.'

    is_today = target_date == local_today
    now = datetime.now(local_tz) if is_today else None
    now_hour = str(now.hour) if now else ""
    now_time = now.strftime("%H:%M") if now else ""

    context = {
        'target_date': target_date,
        'hourly': hourly_data,
        'error': error,
        'now_hour': now_hour,
        'now_time': now_time,
    }
    return render(request, 'hamsalert/hourly_forecast.html', context)
