import csv
from datetime import datetime
from pathlib import Path

from django.db import migrations


def load_events(apps, schema_editor):
    Event = apps.get_model('hamsalert', 'Event')
    Event.objects.all().delete()  # Clear existing events to avoid duplicates
    csv_path = Path(__file__).resolve().parent.parent.parent.parent / 'events.csv'

    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        events_to_create = []

        for row in reader:
            event_date = datetime.strptime(row['date'], '%Y-%m-%d').date()
            events_to_create.append(Event(
                club=row['club'],
                date=event_date,
                description=row.get('description', ''),
            ))

        Event.objects.bulk_create(events_to_create)


def clear_events(apps, schema_editor):
    Event = apps.get_model('hamsalert', 'Event')
    Event.objects.all().delete()


class Migration(migrations.Migration):

    dependencies = [
        ('hamsalert', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(load_events, clear_events),
    ]
