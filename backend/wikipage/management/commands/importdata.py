import os
import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand
from sqlalchemy import create_engine
from wikipage.models import *
import glob

class Command(BaseCommand):
    help = 'Import data from a CSV file into the database'

    def handle(self, *args, **options):
        search_pattern = os.path.join(settings.BASE_DIR, 'keywords-202301-bymonth', 'part-*.csv')
        matching_files = glob.glob(search_pattern)
        count = 0

        database_config = settings.DATABASES['default']
        db_url = f"mysql://{database_config['USER']}:{database_config['PASSWORD']}@{database_config['HOST']}:{database_config['PORT']}/{database_config['NAME']}"
        engine = create_engine(db_url)

        for file_path in matching_files:
            df = pd.read_csv(file_path, on_bad_lines='skip')
            column_names = ['id', 'keyword', 'views']
            df.columns = column_names

            df.to_sql(monthly_models['01']._meta.db_table, con=engine, if_exists='append', index=False)
            self.stdout.write(self.style.SUCCESS(f'Successfully imported ' + file_path + ' count = ' + str(count)))
            count += 1
