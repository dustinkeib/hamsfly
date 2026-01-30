from django.db import models


class Event(models.Model):
    club = models.CharField(max_length=200)
    date = models.DateField(db_index=True)
    description = models.TextField(blank=True)

    class Meta:
        ordering = ['date', 'club']

    def __str__(self):
        return f"{self.club} - {self.date}"


class WeatherRecord(models.Model):
    """
    Persistent storage for weather data from all sources.

    Provides a two-tier caching strategy:
    1. Fast in-memory cache (Django cache)
    2. Database storage (this model) for persistence across restarts

    On API failure, stale DB data can be returned as fallback.
    """

    class WeatherType(models.TextChoices):
        METAR = 'metar', 'METAR'
        TAF = 'taf', 'TAF'
        NWS = 'nws', 'NWS'
        EXTENDED = 'extended', 'Extended'
        HOURLY = 'hourly', 'Hourly'
        HISTORICAL = 'historical', 'Historical'

    weather_type = models.CharField(
        max_length=20,
        choices=WeatherType.choices,
        db_index=True,
    )
    target_date = models.DateField(db_index=True)

    # Location identifiers (use either station OR lat/lon)
    latitude = models.DecimalField(
        max_digits=9,
        decimal_places=6,
        null=True,
        blank=True,
    )
    longitude = models.DecimalField(
        max_digits=9,
        decimal_places=6,
        null=True,
        blank=True,
    )
    station = models.CharField(max_length=10, blank=True, db_index=True)

    # Weather data stored as JSON for flexibility
    data = models.JSONField()

    # Metadata
    fetched_at = models.DateTimeField(db_index=True)
    api_response_time_ms = models.PositiveIntegerField(null=True, blank=True)

    class Meta:
        ordering = ['-fetched_at']
        indexes = [
            models.Index(
                fields=['weather_type', 'target_date', 'station'],
                name='weather_lookup_idx',
            ),
            models.Index(
                fields=['weather_type', 'target_date', 'latitude', 'longitude'],
                name='weather_location_idx',
            ),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['weather_type', 'target_date', 'station', 'latitude', 'longitude'],
                name='weather_unique_record',
            ),
        ]

    def __str__(self):
        location = self.station or f"{self.latitude},{self.longitude}"
        return f"{self.get_weather_type_display()} - {self.target_date} @ {location}"
