from pyspark.sql import SparkSession, Window, functions, types, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import row_number, col, lit
import os
import zipfile
import sys
import re
import os
import shutil
from pyspark.storagelevel import StorageLevel

comments_schema = types.StructType([
    types.StructField('wiki_code', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('page_id', types.StringType()),
    types.StructField('source', types.StringType()),
    types.StructField('daily_total', types.IntegerType()),
    types.StructField('hourly_counts', types.StringType()),
])

processed_schema = StructType([
    StructField('page_id', StringType()),
    StructField('title', StringType()),
    StructField('daily_total', IntegerType()),
    StructField('date', StringType()),
])

result_schema = StructType([ # 21 cols
    StructField('date', StringType()),
    StructField('title_1', StringType()),
    StructField('views_1', IntegerType()),
    StructField('title_2', StringType()),
    StructField('views_2', IntegerType()),
    StructField('title_3', StringType()),
    StructField('views_3', IntegerType()),
    StructField('title_4', StringType()),
    StructField('views_4', IntegerType()),
    StructField('title_5', StringType()),
    StructField('views_5', IntegerType()),
    StructField('title_6', StringType()),
    StructField('views_6', IntegerType()),
    StructField('title_7', StringType()),
    StructField('views_7', IntegerType()),
    StructField('title_8', StringType()),
    StructField('views_8', IntegerType()),
    StructField('title_9', StringType()),
    StructField('views_9', IntegerType()),
    StructField('title_10', StringType()),
    StructField('views_10', IntegerType()),
])

def unzip_file(input_path, output_folder):
    if input_path.endswith('.zip'):
        with zipfile.ZipFile(input_path, 'r') as zip_ref:
            zip_ref.extractall(output_folder)
            # print(f"Extracted: {zip_ref.namelist()}") 
    else:
        raise ValueError("Unsupported file format. Please use .zip or .rar")

def process_file(spark, file_path):
    date_match = re.search(r'pageviews-(\d{4})(\d{2})(\d{2})-clean', file_path)
    if not date_match:
        raise ValueError(f"Date not found in file path: {file_path}")
    
    date_str = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"

    df = spark.read.csv(file_path, schema=comments_schema)

    df = df.select('page_id', 'title', 'daily_total')
    df = df.withColumn('date', lit(date_str))

    df = df.filter(~df.title.isin(["Main_Page", "Wikipedia:Featured_pictures"]))
    df = df.filter(~df.title.startswith("Index_("))
    df = df.filter(~df.title.startswith("The_Menu_("))
    # filters TBC
    return df

def process_daily_folder(spark, day_temp_folder):
    print(f"Processing daily folder: {day_temp_folder}")
    for root, dirs, files in os.walk(day_temp_folder):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                df = process_file(spark, file_path)
                yield df

def to_row(data):
    row_data = {'date': data[0]}
    for title, views, rank in data[1]:
        row_data[f'title_{rank}'] = title
        row_data[f'views_{rank}'] = views
    return Row(**row_data)

def main(year_month):
    monthly_zip_filename = f"pageviews-{year_month}-bymonth.zip"
    unzip_file(monthly_zip_filename, temp_folder)

    all_dataframes = []
    for daily_zip_filename in os.listdir(temp_folder):
        if daily_zip_filename.endswith("-clean.zip"):
            day_temp_folder = os.path.join(temp_folder, daily_zip_filename.replace('.zip', ''))
            unzip_file(os.path.join(temp_folder, daily_zip_filename), day_temp_folder)

            for df in process_daily_folder(spark, day_temp_folder):
                df.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
                all_dataframes.append(df)

    complete_df = spark.createDataFrame([], processed_schema)

    for df in all_dataframes:
        complete_df = complete_df.union(df)

    # add duplicate records
    aggregated_df = complete_df.groupBy("date", "page_id", "title").agg(functions.sum("daily_total").alias("sum_daily_total")).repartition(1500)

    windowSpec = Window.partitionBy("date").orderBy(col("sum_daily_total").desc())
    top_titles_df = aggregated_df.withColumn("rank", row_number().over(windowSpec)) \
                               .filter(col("rank") <= 10)

    top_titles_rdd = top_titles_df.rdd.map(lambda row: (row.date, (row.title, row.sum_daily_total, row.rank)))
    grouped_rdd = top_titles_rdd.groupByKey().mapValues(list).map(to_row)

    result_df = spark.createDataFrame(grouped_rdd, result_schema)

    result_df.write.csv(f"top_titles_byday_{year_month}", mode='overwrite')

    if os.path.exists(temp_folder):
        shutil.rmtree(temp_folder)

if __name__ == "__main__":
    year_month = sys.argv[1]

    spark = SparkSession.builder.appName('top 10 Analysis').config("spark.executor.memory", "12g").config("spark.driver.memory", "5g").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 1500)
    spark.conf.set("spark.sql.debug.maxToStringFields", 100)
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    temp_folder = "temp_pageviews" 
    os.makedirs(temp_folder, exist_ok=True)

    main(year_month)