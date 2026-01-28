from django.contrib import admin
from .models import Event, WeatherRecord


@admin.register(Event)
class EventAdmin(admin.ModelAdmin):
    list_display = ['club', 'date', 'description']
    list_filter = ['date']
    search_fields = ['club', 'description']
    date_hierarchy = 'date'


@admin.register(WeatherRecord)
class WeatherRecordAdmin(admin.ModelAdmin):
    list_display = ['weather_type', 'target_date', 'station', 'latitude', 'longitude', 'fetched_at', 'api_response_time_ms']
    list_filter = ['weather_type', 'target_date', 'fetched_at']
    search_fields = ['station']
    date_hierarchy = 'fetched_at'
    readonly_fields = ['fetched_at', 'data']
    ordering = ['-fetched_at']
