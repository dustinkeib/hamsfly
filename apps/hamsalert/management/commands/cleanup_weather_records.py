from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from apps.hamsalert.models import WeatherRecord


class Command(BaseCommand):
    help = 'Delete old WeatherRecord entries older than WEATHER_DB_CLEANUP_DAYS'

    def add_arguments(self, parser):
        parser.add_argument(
            '--days',
            type=int,
            default=None,
            help='Override WEATHER_DB_CLEANUP_DAYS setting',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be deleted without actually deleting',
        )

    def handle(self, *args, **options):
        days = options['days'] or getattr(settings, 'WEATHER_DB_CLEANUP_DAYS', 30)
        dry_run = options['dry_run']
        cutoff = timezone.now() - timedelta(days=days)

        queryset = WeatherRecord.objects.filter(fetched_at__lt=cutoff)
        count = queryset.count()

        if dry_run:
            self.stdout.write(f'Would delete {count} records older than {days} days')
            # Show breakdown by type
            from django.db.models import Count
            breakdown = queryset.values('weather_type').annotate(count=Count('id'))
            for item in breakdown:
                self.stdout.write(f"  - {item['weather_type']}: {item['count']}")
        else:
            deleted, _ = queryset.delete()
            self.stdout.write(
                self.style.SUCCESS(f'Deleted {deleted} records older than {days} days')
            )
