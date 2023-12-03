import os
import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand
from sqlalchemy import create_engine
from wikipage.models import *
import glob

class Command(BaseCommand):
    help = 'Import daily top10'

    def add_arguments(self, parser):
        # Adding an argument for the month
        parser.add_argument('month', type=str, help='Month indicator (01-12)')

    def handle(self, *args, **options):
        month = options['month']
        folder_name = f'top_titles_byday_2023{month}'
        search_pattern = os.path.join(settings.BASE_DIR, folder_name, 'part-*.csv')
        matching_files = glob.glob(search_pattern)
        count = 0

        database_config = settings.DATABASES['default']
        db_url = f"mysql://{database_config['USER']}:{database_config['PASSWORD']}@{database_config['HOST']}:{database_config['PORT']}/{database_config['NAME']}"
        engine = create_engine(db_url)

        for file_path in matching_files:
            df = pd.read_csv(file_path, on_bad_lines='skip', header=None)
            column_names = ['date', 
                            'title_1', 'views_1', 
                            'title_2', 'views_2',
                            'title_3', 'views_3', 
                            'title_4', 'views_4', 
                            'title_5', 'views_5', 
                            'title_6', 'views_6', 
                            'title_7', 'views_7',
                            'title_8', 'views_8', 
                            'title_9', 'views_9', 
                            'title_10', 'views_10']
            df.columns = column_names

            df.to_sql(wikitop10._meta.db_table, con=engine, if_exists='append', index=False)
            self.stdout.write(self.style.SUCCESS(f'Successfully imported ' + file_path + ' count = ' + str(count)))
            count += 1
