import time
from datetime import datetime, timedelta

from django.conf import settings
from django.contrib import admin, messages
from django.http import HttpResponseRedirect
from django.urls import path, reverse
from django.utils.html import format_html
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


def _poll_all(request):
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

        # Visual Crossing daily
        daily_results = service.fetch_visualcrossing_batch(local_today)
        for target, data in daily_results:
            service._save_to_db('openmeteo', target, service._serialize_openmeteo_data(data), lat=lat, lon=lon)
        results.append(f'Daily ({len(daily_results)} days)')

        time.sleep(2)

        # Visual Crossing hourly
        hourly_results = service.fetch_visualcrossing_hourly_batch(local_today, days=15)
        for target, data in hourly_results:
            service._save_to_db('hourly', target, service._serialize_hourly_data(data), lat=lat, lon=lon)
        results.append(f'Hourly ({len(hourly_results)} days)')

        return True, f"Polled: {', '.join(results)}"
    except Exception as e:
        return False, f"Poll failed: {e}"


def _poll_metar(request):
    """Poll METAR for today."""
    service, local_today, lat, lon = _get_weather_context()
    try:
        data = service._fetch_metar_from_api(service.default_station)
        if data:
            service._save_to_db('metar', local_today, service._serialize_metar_data(data), station=service.default_station)
            return True, f"METAR updated for {local_today}"
        return False, "No METAR data returned"
    except Exception as e:
        return False, f"METAR poll failed: {e}"


def _poll_taf(request):
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
        return True, f"TAF updated ({count} days)"
    except Exception as e:
        return False, f"TAF poll failed: {e}"


def _poll_nws(request):
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
        return True, f"NWS updated ({count} days)"
    except Exception as e:
        return False, f"NWS poll failed: {e}"


def _poll_daily(request):
    """Poll Visual Crossing daily forecast for days 0-14."""
    service, local_today, lat, lon = _get_weather_context()
    try:
        results = service.fetch_visualcrossing_batch(local_today)
        for target, data in results:
            service._save_to_db('openmeteo', target, service._serialize_openmeteo_data(data), lat=lat, lon=lon)
        return True, f"Daily forecast updated ({len(results)} days)"
    except Exception as e:
        return False, f"Daily poll failed: {e}"


def _poll_hourly(request):
    """Poll Visual Crossing hourly forecast for days 0-14."""
    service, local_today, lat, lon = _get_weather_context()
    try:
        results = service.fetch_visualcrossing_hourly_batch(local_today, days=15)
        for target, data in results:
            service._save_to_db('hourly', target, service._serialize_hourly_data(data), lat=lat, lon=lon)
        return True, f"Hourly updated ({len(results)} days)"
    except Exception as e:
        return False, f"Hourly poll failed: {e}"


def _poll_historical(request):
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
        return True, f"Historical updated ({count} days)"
    except Exception as e:
        return False, f"Historical poll failed: {e}"


POLL_ACTIONS = {
    'all': ('Poll All', _poll_all),
    'metar': ('METAR', _poll_metar),
    'taf': ('TAF', _poll_taf),
    'nws': ('NWS', _poll_nws),
    'openmeteo': ('Daily', _poll_daily),
    'hourly': ('Hourly', _poll_hourly),
    'historical': ('Historical', _poll_historical),
}


@admin.register(WeatherRecord)
class WeatherRecordAdmin(admin.ModelAdmin):
    list_display = ['weather_type', 'target_date', 'station', 'latitude', 'longitude', 'fetched_at', 'api_response_time_ms']
    list_filter = ['weather_type', 'target_date', 'fetched_at']
    search_fields = ['station']
    date_hierarchy = 'fetched_at'
    readonly_fields = ['fetched_at', 'data']
    ordering = ['-fetched_at']
    change_list_template = 'admin/hamsalert/weatherrecord/change_list.html'

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('poll/<str:source>/', self.admin_site.admin_view(self.poll_view), name='hamsalert_weatherrecord_poll'),
        ]
        return custom_urls + urls

    def poll_view(self, request, source):
        """Handle poll request for a specific source."""
        if source not in POLL_ACTIONS:
            messages.error(request, f"Unknown source: {source}")
        else:
            _, handler = POLL_ACTIONS[source]
            success, message = handler(request)
            if success:
                messages.success(request, message)
            else:
                messages.error(request, message)

        return HttpResponseRedirect(reverse('admin:hamsalert_weatherrecord_changelist'))

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context['poll_buttons'] = [
            {'source': key, 'label': label}
            for key, (label, _) in POLL_ACTIONS.items()
        ]
        return super().changelist_view(request, extra_context=extra_context)
