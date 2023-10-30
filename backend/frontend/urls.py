
from django.urls import path
from .views import index, event

urlpatterns = [
    path('', index),
    path('event', event)
]
