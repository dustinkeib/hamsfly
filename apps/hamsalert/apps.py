from django.apps import AppConfig


class HamsalertConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.hamsalert'

    def ready(self):
        from . import keepalive, scheduler, weather_poller
        keepalive.start()
        scheduler.start()
        weather_poller.start()
