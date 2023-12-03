# to zip files by month
import sys
from pyspark.sql import SparkSession, functions, types
import zipfile
import os
import shutil
import re
from collections import defaultdict

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

comments_schema = types.StructType([
    types.StructField('wiki_code', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('page_id', types.StringType()),
    types.StructField('source', types.StringType()),
    types.StructField('daily_total', types.IntegerType()),
    types.StructField('hourly_counts', types.StringType()),
])

sql_injection_patterns = [
    "(?i)OR '1'='1",
    "(?i); DROP TABLE",
    "(?i)EXEC xp_cmdshell",
    "(?i)UNION SELECT",
    "(?i); INSERT INTO",
    "(?i); SHUTDOWN",
    "(?i)LIKE '%admin%",
    "(?i); UPDATE ",
    "(?i); DELETE FROM",
    "(?i)--",
    "(?i)/\*.*\*/",
    "(?i)@@version",
    "(?i)CHAR\(",
    "(?i)WAITFOR DELAY",
    "(?i)CONVERT\(",
    "(?i)BENCHMARK\(", 
    "(?i)SLEEP\(", 
    "(?i)PG_SLEEP\(", 
    "(?i)DBMS_PIPE.RECEIVE_MESSAGE\(", 
    "(?i)CAST\(",
    "(?i); BEGIN",
    "(?i); COMMIT",
    "(?i)DECLARE\(",
    "(?i)HAVING",
    "(?i)RLIKE ", 
    "(?i)EXTRACTVALUE\(",
    "(?i)LOAD_FILE\(",
    "(?i)DUMPFILE\(", 
    "(?i)OUTFILE", 
    "(?i)HashBytes",
    # "(?i)EXPR",
    "=",
    "(?i)' and '.+='.+'",
    "(?i)' or '.+='.+'"
] # TBC

def unzip_file(input_path, output_folder):
    if input_path.endswith('.zip'):
        with zipfile.ZipFile(input_path, 'r') as zip_ref:
            zip_ref.extractall(output_folder)
    else:
        raise ValueError("Unsupported file format. Please use .zip or .rar")
    
def find_gz_files(root_folder):
    gz_files = []
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file.endswith('.gz'):
                gz_files.append(os.path.join(root, file))
    return gz_files

def zipdir(path, ziph):
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), os.path.join(path, '..')))

def zip_files(files, zip_filename):
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file in files:
            zipf.write(file, os.path.basename(file))

def main(temp_folder, output_zip_name):
    gz_files = find_gz_files(temp_folder)
    if not gz_files:
        raise ValueError("No .gz files found in the temp folder.")

    all_data = spark.createDataFrame([], comments_schema)

    for gz_file in gz_files:
        text_df = spark.read.text(gz_file)

        split_col = functions.split(text_df['value'], ' ')
        df = text_df.select(
            split_col.getItem(0).alias('wiki_code'),
            split_col.getItem(1).alias('title'),
            split_col.getItem(2).alias('page_id'),
            split_col.getItem(3).alias('source'),
            split_col.getItem(4).cast('int').alias('daily_total'),
            functions.lit(None).cast('string').alias('hourly_counts')
        )

        base_filter = (
            (df['wiki_code'] == "en.wikipedia") &
            (df['page_id'] != 'null') &
            ~df['title'].startswith("File:") &
            ~df['title'].endswith(".jpg") &
            ~df['title'].endswith(".svg") &
            ~df['title'].endswith(".png") &
            ~df['title'].rlike("[0-9]+$") &
            (df['title'] != "-")
        )
        for pattern in sql_injection_patterns:
            base_filter = base_filter & (~df['title'].rlike(pattern))

        df_clean = df.filter(base_filter)
        all_data = all_data.union(df_clean)

    if os.path.exists(output_zip_name):
        os.remove(output_zip_name)

    output_folder = output_zip_name.replace('.zip', '') + '_temp'
    os.makedirs(output_folder, exist_ok=True)
    all_data.write.csv(output_folder, mode='overwrite')
    
    with zipfile.ZipFile(output_zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipdir(output_folder, zipf)

    # shutil.rmtree(temp_folder)
    shutil.rmtree(output_folder)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('ETL').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.conf.set("spark.sql.debug.maxToStringFields", 100)
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    monthly_files = defaultdict(list)

    for file in os.listdir('.'):
        if re.match(r'pageviews-\d{8}-user.*\.zip$', file): 
            date_match = re.search(r'pageviews-(\d{8})-user', file) 
            if date_match:
                date_str = date_match.group(1)
                month_str = date_str[:6]
                output_zip_name = f"pageviews-{date_str}-clean.zip"
                temp_folder = "temp_etl_" + date_str
                os.makedirs(temp_folder, exist_ok=True)

                unzip_file(file, temp_folder) 
                main(temp_folder, output_zip_name)
                shutil.rmtree(temp_folder)

                monthly_files[month_str].append(output_zip_name)
            else:
                print(f"Error: Date not found in the filename {file}.")

    for month, files in monthly_files.items():
        bymonth_zip_name = f"pageviews-{month}-bymonth.zip"
        zip_files(files, bymonth_zip_name)

        for f in files:
            os.remove(f)