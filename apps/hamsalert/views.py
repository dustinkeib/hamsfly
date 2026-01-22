import calendar
from datetime import date

from django.shortcuts import render
from .models import Event


def calendar_view(request):
    """Display a calendar grid with dots on days that have events."""
    today = date.today()
    year = int(request.GET.get('year', today.year))
    month = int(request.GET.get('month', today.month))

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

    context = {
        'year': year,
        'month': month,
        'month_name': calendar.month_name[month],
        'month_days': month_days,
        'days_with_events': days_with_events,
        'today': today,
        'prev_month': prev_month,
        'prev_year': prev_year,
        'next_month': next_month,
        'next_year': next_year,
    }
    return render(request, 'hamsalert/calendar.html', context)


def day_events(request, year, month, day):
    """Return events for a specific day (HTMX partial)."""
    event_date = date(year, month, day)
    events = Event.objects.filter(date=event_date)
    context = {
        'events': events,
        'date': event_date,
    }
    return render(request, 'hamsalert/partials/day_events.html', context)
