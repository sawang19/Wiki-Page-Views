from django.db import models

# Create your models here.

def get_title():
    return 'title_test'

class Wikipage(models.Model):
    title = models.CharField(max_length=200)
    count = models.IntegerField(default=0)

    def __str__(self):
        return self.title