import time
from datetime import datetime, timedelta

from django.conf import settings
from django.contrib import admin, messages
from zoneinfo import ZoneInfo

from .models import Event, WeatherRecord


@admin.register(Event)
class EventAdmin(admin.ModelAdmin):
    list_display = ['club', 'date', 'description']
    list_filter = ['date']
    search_fields = ['club', 'description']
    date_hierarchy = 'date'


def _get_weather_context():
    """Get common context for weather polling."""
    from apps.hamsalert.services import WeatherService
    service = WeatherService()
    local_tz = ZoneInfo(getattr(settings, 'WEATHER_LOCAL_TIMEZONE', 'America/Los_Angeles'))
    local_today = datetime.now(local_tz).date()
    lat, lon = service.nws_location
    return service, local_today, lat, lon


@admin.action(description="Poll All weather sources")
def poll_all_sources(modeladmin, request, queryset):
    """Poll all weather sources."""
    service, local_today, lat, lon = _get_weather_context()
    results = []

    try:
        # METAR
        data = service._fetch_metar_from_api(service.default_station)
        if data:
            service._save_to_db('metar', local_today, service._serialize_metar_data(data), station=service.default_station)
            results.append('METAR')

        # TAF
        for days_out in range(2):
            target = local_today + timedelta(days=days_out)
            data = service._fetch_taf_from_api(service.default_station, target)
            if data:
                service._save_to_db('taf', target, service._serialize_taf_data(data), station=service.default_station)
            time.sleep(1)
        results.append('TAF')

        # NWS
        for days_out in range(2, 8):
            target = local_today + timedelta(days=days_out)
            data = service._fetch_nws_forecast(target)
            if data:
                service._save_to_db('nws', target, service._serialize_nws_data(data), lat=lat, lon=lon)
            time.sleep(1)
        results.append('NWS')

        # OpenMeteo daily
        daily_results = service.fetch_openmeteo_batch(local_today)
        for target, data in daily_results:
            service._save_to_db('openmeteo', target, service._serialize_openmeteo_data(data), lat=lat, lon=lon)
        results.append(f'OpenMeteo ({len(daily_results)} days)')

        time.sleep(2)

        # OpenMeteo hourly
        hourly_results = service.fetch_hourly_batch(local_today, days=16)
        for target, data in hourly_results:
            service._save_to_db('hourly', target, service._serialize_hourly_data(data), lat=lat, lon=lon)
        results.append(f'Hourly ({len(hourly_results)} days)')

        modeladmin.message_user(request, f"Polled: {', '.join(results)}", messages.SUCCESS)
    except Exception as e:
        modeladmin.message_user(request, f"Poll failed: {e}", messages.ERROR)


@admin.action(description="Poll METAR")
def poll_metar(modeladmin, request, queryset):
    """Poll METAR for today."""
    service, local_today, lat, lon = _get_weather_context()
    try:
        data = service._fetch_metar_from_api(service.default_station)
        if data:
            service._save_to_db('metar', local_today, service._serialize_metar_data(data), station=service.default_station)
            modeladmin.message_user(request, f"METAR updated for {local_today}", messages.SUCCESS)
        else:
            modeladmin.message_user(request, "No METAR data returned", messages.WARNING)
    except Exception as e:
        modeladmin.message_user(request, f"METAR poll failed: {e}", messages.ERROR)


@admin.action(description="Poll TAF")
def poll_taf(modeladmin, request, queryset):
    """Poll TAF for days 0-1."""
    service, local_today, lat, lon = _get_weather_context()
    try:
        count = 0
        for days_out in range(2):
            target = local_today + timedelta(days=days_out)
            data = service._fetch_taf_from_api(service.default_station, target)
            if data:
                service._save_to_db('taf', target, service._serialize_taf_data(data), station=service.default_station)
                count += 1
            time.sleep(1)
        modeladmin.message_user(request, f"TAF updated ({count} days)", messages.SUCCESS)
    except Exception as e:
        modeladmin.message_user(request, f"TAF poll failed: {e}", messages.ERROR)


@admin.action(description="Poll NWS")
def poll_nws(modeladmin, request, queryset):
    """Poll NWS for days 2-7."""
    service, local_today, lat, lon = _get_weather_context()
    try:
        count = 0
        for days_out in range(2, 8):
            target = local_today + timedelta(days=days_out)
            data = service._fetch_nws_forecast(target)
            if data:
                service._save_to_db('nws', target, service._serialize_nws_data(data), lat=lat, lon=lon)
                count += 1
            time.sleep(1)
        modeladmin.message_user(request, f"NWS updated ({count} days)", messages.SUCCESS)
    except Exception as e:
        modeladmin.message_user(request, f"NWS poll failed: {e}", messages.ERROR)


@admin.action(description="Poll OpenMeteo (daily)")
def poll_openmeteo(modeladmin, request, queryset):
    """Poll OpenMeteo daily forecast for days 0-15."""
    service, local_today, lat, lon = _get_weather_context()
    try:
        results = service.fetch_openmeteo_batch(local_today)
        for target, data in results:
            service._save_to_db('openmeteo', target, service._serialize_openmeteo_data(data), lat=lat, lon=lon)
        modeladmin.message_user(request, f"OpenMeteo daily updated ({len(results)} days)", messages.SUCCESS)
    except Exception as e:
        modeladmin.message_user(request, f"OpenMeteo poll failed: {e}", messages.ERROR)


@admin.action(description="Poll Hourly forecast")
def poll_hourly(modeladmin, request, queryset):
    """Poll OpenMeteo hourly forecast for days 0-15."""
    service, local_today, lat, lon = _get_weather_context()
    try:
        results = service.fetch_hourly_batch(local_today, days=16)
        for target, data in results:
            service._save_to_db('hourly', target, service._serialize_hourly_data(data), lat=lat, lon=lon)
        modeladmin.message_user(request, f"Hourly updated ({len(results)} days)", messages.SUCCESS)
    except Exception as e:
        modeladmin.message_user(request, f"Hourly poll failed: {e}", messages.ERROR)


@admin.action(description="Poll Historical (past 7 days)")
def poll_historical(modeladmin, request, queryset):
    """Poll historical weather for past 7 days."""
    service, local_today, lat, lon = _get_weather_context()
    try:
        count = 0
        for days_ago in range(1, 8):
            target = local_today - timedelta(days=days_ago)
            try:
                data = service._fetch_historical_weather(target)
                if data:
                    service._save_to_db('historical', target, service._serialize_historical_data(data), lat=lat, lon=lon)
                    count += 1
            except Exception:
                pass  # Skip individual failures
            time.sleep(1)
        modeladmin.message_user(request, f"Historical updated ({count} days)", messages.SUCCESS)
    except Exception as e:
        modeladmin.message_user(request, f"Historical poll failed: {e}", messages.ERROR)


@admin.register(WeatherRecord)
class WeatherRecordAdmin(admin.ModelAdmin):
    list_display = ['weather_type', 'target_date', 'station', 'latitude', 'longitude', 'fetched_at', 'api_response_time_ms']
    list_filter = ['weather_type', 'target_date', 'fetched_at']
    search_fields = ['station']
    date_hierarchy = 'fetched_at'
    readonly_fields = ['fetched_at', 'data']
    ordering = ['-fetched_at']
    actions = [poll_all_sources, poll_metar, poll_taf, poll_nws, poll_openmeteo, poll_hourly, poll_historical]
