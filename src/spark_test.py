import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

# جعل Terminal يدعم العربية
sys.stdout.reconfigure(encoding="utf-8")


# Schema ثابتة كما طلب الدكتور
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
    .appName("MidtermSparkTest")
    .master("local[*]")
    .getOrCreate()
)

# تقليل الرسائل الكثيرة
spark.sparkContext.setLogLevel("WARN")


try:

    file_path = (
        r"D:\midterm-data-pipeline\data\orders_small_sample.csv"
    )

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(file_path)
    )

    rows = df.count()
    partitions = df.rdd.getNumPartitions()

    print("\n========================")
    print("SPARK TEST SUCCESS")
    print("========================")

    print("Rows:", rows)
    print("Partitions:", partitions)

finally:

    spark.stop()