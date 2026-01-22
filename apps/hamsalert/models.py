from django.db import models


class Event(models.Model):
    club = models.CharField(max_length=200)
    date = models.DateField(db_index=True)
    description = models.TextField(blank=True)

    class Meta:
        ordering = ['date', 'club']

    def __str__(self):
        return f"{self.club} - {self.date}"
