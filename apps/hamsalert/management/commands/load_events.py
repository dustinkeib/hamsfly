import csv
from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from apps.hamsalert.models import Event


class Command(BaseCommand):
    help = 'Load events from a CSV file into the database'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', help='Path to the CSV file')
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Clear existing events before loading',
        )

    def handle(self, *args, **options):
        csv_file = options['csv_file']

        if options['clear']:
            deleted, _ = Event.objects.all().delete()
            self.stdout.write(f'Deleted {deleted} existing events')

        try:
            with open(csv_file, 'r', newline='', encoding='utf-8') as f:
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
                self.stdout.write(
                    self.style.SUCCESS(f'Successfully created {len(events_to_create)} events')
                )

        except FileNotFoundError:
            raise CommandError(f'CSV file not found: {csv_file}')
        except KeyError as e:
            raise CommandError(f'Missing required column in CSV: {e}')
        except ValueError as e:
            raise CommandError(f'Invalid date format (expected YYYY-MM-DD): {e}')
