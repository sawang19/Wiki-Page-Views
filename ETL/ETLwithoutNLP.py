import sys
from pyspark.sql import SparkSession, functions, types

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

comments_schema = types.StructType([
    types.StructField('wiki_code', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('page_id', types.StringType()),
    types.StructField('source', types.StringType()),
    types.StructField('daily_total', types.IntegerType()),
    types.StructField('hourly_counts', types.StringType()),
])

def main(inputs, output):
    df = spark.read.csv(inputs, header=False, sep=" ").cache()

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
        "(?i)BENCHMARK\(",  # Used in MySQL for Time-Based Blind SQL Injection
        "(?i)SLEEP\(",  # Used in MySQL for Time-Based Blind SQL Injection
        "(?i)PG_SLEEP\(",  # Used in PostgreSQL for Time-Based Blind SQL Injection
        "(?i)DBMS_PIPE.RECEIVE_MESSAGE\(",  # Used in Oracle for Time-Based Blind SQL Injection
        "(?i)CAST\(",
        "(?i); BEGIN",
        "(?i); COMMIT",
        "(?i)DECLARE\(",  # Often used for declaring variables in injected SQL
        "(?i)HAVING",
        "(?i)RLIKE ",  # Regular expression matching in MySQL
        "(?i)EXTRACTVALUE\(",  # Extracting data in MySQL via XML
        "(?i)LOAD_FILE\(",  # Reading files in MySQL
        "(?i)DUMPFILE\(",  # Writing to files in MySQL
        "(?i)OUTFILE",  # Writing query results to files in MySQL
        "(?i)HashBytes",
        # "(?i)EXPR",
        "=",
        "(?i)' and '.+='.+'",
        "(?i)' or '.+='.+'"

    ] # TBC

    base_filter = (
        (df._c0 == "en.wikipedia") &
        (~df._c1.startswith("File:")) &
        (~df._c1.endswith(".jpg")) &
        (~df._c1.endswith(".svg")) &
        (~df._c1.endswith(".png")) &
        (~df._c1.rlike("[0-9]+$")) &
        (df._c1 != "-")
    )
    for pattern in sql_injection_patterns:
        base_filter &= (~df._c1.rlike(pattern))

    df_clean = df.filter(base_filter)
    df_clean.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('ETL').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.conf.set("spark.sql.debug.maxToStringFields", 100)
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
