import os
from django.conf import settings
import csv
from django.core.management.base import BaseCommand
from wikipage.models import Wikipage
from django.db import transaction

class Command(BaseCommand):
    help = 'Import data from a CSV file into the database'

    def handle(self, *args, **options):
        csv_file_path = os.path.join(settings.BASE_DIR, 'csvfiles', 'selected_data.csv')

        with open(csv_file_path, 'r') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)
            wikipage_list = []
            for row in csv_reader:
                date = row[0]
                title = row[1]
                views = row[2]
                wikipage = Wikipage(date=date, title=title, views=views)
                wikipage_list.append(wikipage)
            with transaction.atomic():
                Wikipage.objects.bulk_create(wikipage_list)
        self.stdout.write(self.style.SUCCESS(f'Successfully imported data'))
