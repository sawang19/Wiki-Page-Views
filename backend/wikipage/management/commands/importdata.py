import os
import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand
from sqlalchemy import create_engine
from wikipage.models import *

class Command(BaseCommand):
    help = 'Import data from a CSV file into the database'

    def handle(self, *args, **options):
        csv_file_path = os.path.join(settings.BASE_DIR, 'csvfiles', 'test.csv')
        df = pd.read_csv(csv_file_path)

        database_config = settings.DATABASES['default']
        db_url = f"mysql://{database_config['USER']}:{database_config['PASSWORD']}@{database_config['HOST']}:{database_config['PORT']}/{database_config['NAME']}"
        engine = create_engine(db_url)

        df.to_sql(Wikipage._meta.db_table, con=engine, if_exists='append', index=False)
        self.stdout.write(self.style.SUCCESS(f'Successfully imported data'))
