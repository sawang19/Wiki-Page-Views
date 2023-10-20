from django.db import models
import datetime

# Create your models here.

def get_title():
    return 'title_test'

class Wikipage(models.Model):
    date = models.DateField(default=datetime.date(2023, 1, 1))
    title = models.CharField(max_length=32)
    views = models.IntegerField(default=0)

    def __str__(self):
        return self.title