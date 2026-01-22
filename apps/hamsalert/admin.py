from django.contrib import admin
from .models import Event


@admin.register(Event)
class EventAdmin(admin.ModelAdmin):
    list_display = ['club', 'date', 'description']
    list_filter = ['date']
    search_fields = ['club', 'description']
    date_hierarchy = 'date'
