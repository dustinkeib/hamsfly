from django.urls import path
from . import views

urlpatterns = [
    path('', views.calendar_view, name='calendar'),
    path('<int:year>/<int:month>/', views.calendar_month_view, name='calendar_month'),
    path('<int:year>/<int:month>/<int:day>/', views.calendar_day_view, name='calendar_day'),
    path('health/', views.health, name='health'),
    path('weather/refresh/', views.weather_refresh, name='weather_refresh'),
    path('hourly/<int:year>/<int:month>/<int:day>/', views.hourly_forecast, name='hourly_forecast'),
    path('flying/toggle/', views.toggle_flying_intent, name='toggle_flying_intent'),
]
