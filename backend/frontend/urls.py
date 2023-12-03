
from django.urls import path
from .views import index, event, trend

urlpatterns = [
    path('', index),
    path('event', event),
    path('trendings', trend)
]
