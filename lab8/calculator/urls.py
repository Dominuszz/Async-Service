from django.urls import path
from . import views

urlpatterns = [
    path('calculate/', views.start_calculation, name='start-calculation'),
    path('health/', views.health_check, name='health-check'),
]