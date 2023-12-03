import sys
import re
import shutil
from pyspark.sql import SparkSession, functions, types, Row, Window
import nltk
import ssl
import zipfile
import os
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import monotonically_increasing_id

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context
nltk.download('punkt')

comments_schema = types.StructType([
    types.StructField('wiki_code', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('page_id', types.StringType()),
    types.StructField('source', types.StringType()),
    types.StructField('daily_total', types.IntegerType()),
    types.StructField('hourly_counts', types.StringType()),
])

result_schema = types.StructType([
    types.StructField('row_id', types.LongType()),  # primary key
    types.StructField('keyword', types.StringType()),
    types.StructField('views', types.StringType()), # day:views 01:11-02:22-03:33-04:44-05:55......
])

def extract_keywords(title, stop_words): # extract keywords from titles and remove stop words
    if (title == None):
        print("title is none")
        return []
    title = title.replace("_", " ")
    tokens = nltk.word_tokenize(title) # seperate keywords
    return [word.strip().lower() for word in tokens if word.strip().lower() not in stop_words]

def unzip_file(input_path, output_folder):
    if input_path.endswith('.zip'):
        with zipfile.ZipFile(input_path, 'r') as zip_ref:
            zip_ref.extractall(output_folder)
            # print(f"Extracted: {zip_ref.namelist()}") 
    else:
        raise ValueError("Unsupported file format. Please use .zip or .rar")

def process_file(file_path, stop_words, day_temp_folder): # process files of each day
    match = re.search(r"pageviews-(\d{8})-clean", file_path) # fixed input daily file name
    if not match:
        print(f"Could not extract date from file path: {file_path}")
        return None
    date = match.group(1)
    year_month = date[:4] + "-" + date[4:6]
    # print(f"Processing file: {file_path}")
    if os.path.exists(file_path):  
        df = spark.read.csv(file_path, schema=comments_schema)
        # df.show(5)
        keywords_rdd = df.rdd.flatMap(lambda row: [
            ((keyword), f"{date[6:]}:{row.daily_total}") # keyword, 31:views (for 20230131)
            for keyword in extract_keywords(row.title, stop_words) if row.title and row.daily_total > 0
        ])
        # print("Sample of keywords RDD:", keywords_rdd.take(5))
        return keywords_rdd
    else:
        print(f"File does not exist: {file_path}")
        return None


def process_daily_folder(day_temp_folder, stop_words):
    # process each file in the daily folder, like 'part-xxx.csv'
    print(f"Processing daily folder: {day_temp_folder}")
    for root, dirs, files in os.walk(day_temp_folder):
        for file in files:
            if file.startswith("part") and file.endswith(".csv"):
                file_path = os.path.join(root, file)
                # print(f"Processing file: {file_path}")
                yield process_file(file_path, stop_words, day_temp_folder)

def merge_views(a, b):
    a_views = a.split("-") # store 'day:views' records in a list
    b_views = b.split("-")
    views_dict = {}

    for view in a_views + b_views:
        date, count = view.split(":")
        count = int(count)
        if date in views_dict:
            views_dict[date] += count
        else:
            views_dict[date] = count

    merged_views = "-".join([f"{date}:{count}" for date, count in sorted(views_dict.items())])
    return merged_views

def main(year_month):
    # load stop words
    with open('stopwords.txt', 'r') as file:
        custom_stopwords = file.read().splitlines()
    stop_words = set(custom_stopwords) # time complexity: O(n) -> O(1)

    monthly_zip_filename = f"pageviews-{year_month}-bymonth.zip"
    unzip_file(monthly_zip_filename, temp_folder)

    # print("Contents of the temp_folder:")
    # print(os.listdir(temp_folder))

    rdds = []
    
    for daily_zip_filename in os.listdir(temp_folder):
        if daily_zip_filename.endswith("-clean.zip"):
            print(f"Processing daily zip: {daily_zip_filename}")
            # Unzip each daily zip file to a new temp folder within temp_folder
            day_temp_folder = os.path.join(temp_folder, daily_zip_filename.replace('.zip', ''))
            unzip_file(os.path.join(temp_folder, daily_zip_filename), day_temp_folder)
            # Process the unzipped folder
            for rdd in process_daily_folder(day_temp_folder, stop_words):
                # set persistence level of an RDD. RDD should be stored in memory
                rdd.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                rdds.append(rdd)

    if rdds:
        all_data = sc.union(rdds)
        all_data = all_data.repartition(200)

        aggregated_data = all_data.reduceByKey(merge_views)

        print(f"Aggregated {len(rdds)} RDDs")

        if not aggregated_data.isEmpty():
            output_path = f"keywords-{year_month}"
            df = aggregated_data.toDF(["keyword", "views"])
            df = df.withColumn("row_id", monotonically_increasing_id()).select("row_id", "keyword", "views")
            df.write.csv(output_path, mode='overwrite')

            output_zip_path = f"keywords-{year_month}-bymonth.zip"
            with zipfile.ZipFile(output_zip_path, 'w') as output_zip_ref:
                spark_output_dir = f"keywords-{year_month}"
                if os.path.exists(spark_output_dir):
                    for root, dirs, files in os.walk(spark_output_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            output_zip_ref.write(file_path, os.path.relpath(file_path, start=spark_output_dir))

            if os.path.exists(spark_output_dir):
                shutil.rmtree(spark_output_dir)

            print(f"Data written to {output_zip_path}")
            print(f"Data frame has {aggregated_data.count()} rows")
        else:
            print("Aggregated data is empty. No data to write.")

    if os.path.exists(temp_folder):
        shutil.rmtree(temp_folder)


if __name__ == "__main__":
    year_month = sys.argv[1]

    spark = SparkSession.builder.appName('Pageviews Analysis').config("spark.executor.memory", "9g").config("spark.driver.memory", "4g").getOrCreate()
    
    spark.conf.set("spark.sql.debug.maxToStringFields", 100)
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    temp_folder = "temp_pageviews" 
    os.makedirs(temp_folder, exist_ok=True)

    main(year_month)