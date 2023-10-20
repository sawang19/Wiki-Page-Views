from django.apps import AppConfig
from django.core import management


class WikipageConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'wikipage'

    # def ready(self):
    #     management.call_command('importdata')
