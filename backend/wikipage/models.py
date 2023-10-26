from django.db import models
import datetime

# Create your models here.

class Wikipage(models.Model):
    month = models.CharField(max_length=32, default='0000-00')
    keyword = models.CharField(max_length=32, default='')
    title = models.CharField(max_length=128, default='')
    views = models.CharField(max_length=256)

    class Meta:
        indexes = [
            models.Index(fields=['month', 'keyword', 'title'], name='idx_MKT')
        ]

    def __str__(self):
        return self.title