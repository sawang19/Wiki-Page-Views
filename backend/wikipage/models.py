from django.db import models
from django.db.models import UniqueConstraint
import datetime

# Create your models here.

class Wikipage2301(models.Model):
    id = models.CharField(primary_key=True, max_length=128, default='')
    keyword = models.CharField(max_length=32, default='')
    views = models.CharField(max_length=256)

    class Meta:
        indexes = [
            models.Index(fields=['keyword'], name='idx_K')
        ]

    def __str__(self):
        return self.id