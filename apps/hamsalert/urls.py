from django.urls import path
from . import views

urlpatterns = [
    path('', views.calendar_view, name='calendar'),
    path('events/<int:year>/<int:month>/<int:day>/', views.day_events, name='day_events'),
    path('weather/refresh/', views.weather_refresh, name='weather_refresh'),
]
