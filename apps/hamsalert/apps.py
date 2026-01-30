from django.apps import AppConfig
from django.db.backends.signals import connection_created


def enable_wal_mode(sender, connection, **kwargs):
    """Enable WAL mode for SQLite to reduce lock contention."""
    if connection.vendor == 'sqlite':
        cursor = connection.cursor()
        cursor.execute('PRAGMA journal_mode=WAL;')


class HamsalertConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.hamsalert'

    def ready(self):
        connection_created.connect(enable_wal_mode)

        import sys

        # Don't start background threads during tests
        if 'test' in sys.argv:
            return

        from . import keepalive, scheduler, weather_poller
        keepalive.start()
        scheduler.start()
        weather_poller.start()
