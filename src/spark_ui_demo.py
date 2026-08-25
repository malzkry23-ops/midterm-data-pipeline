import time

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.sql import functions as F


SAMPLE_FILE = (
    r"D:\midterm-data-pipeline"
    r"\data\orders_small_sample.csv"
)


# Fixed Schema
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_phone", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("delivery_type", StringType(), True),
    StructField("delivery_cost", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("items_json", StringType(), True),
])


spark = (
    SparkSession.builder
    .appName("Midterm-Spark-UI-Evidence")
    .master("local[4]")
    .config("spark.ui.enabled", "true")
    .config("spark.ui.port", "4040")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


df = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("quote", '"')
    .option("escape", '"')
    .schema(schema)
    .csv(SAMPLE_FILE)
)


print("\n===================================")
print("SPARK UI DEMO")
print("===================================")

print("INPUT PARTITIONS:", df.rdd.getNumPartitions())


# إنشاء Shuffle حتى تظهر Stages و Tasks بوضوح
work_df = df.repartition(8)

print(
    "PROCESSING PARTITIONS:",
    work_df.rdd.getNumPartitions()
)


# Job 1
work_df.groupBy(
    "status"
).count().orderBy(
    F.desc("count")
).show(
    truncate=False
)


# Job 2
row_count = work_df.count()

print("TOTAL ROWS:", row_count)

print("\nSPARK UI:")
print("http://localhost:4040")

print(
    "\nSpark UI will stay open for 10 minutes."
)

print(
    "DO NOT CLOSE THIS POWERSHELL WINDOW YET."
)


time.sleep(600)

spark.stop()
