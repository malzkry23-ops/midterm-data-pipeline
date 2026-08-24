from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


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
    .appName("SparkMongoTest")
    .master("local[*]")
    .config(
        "spark.mongodb.write.connection.uri",
        "mongodb://127.0.0.1:27017/midterm_pipeline.spark_connector_test"
    )
    .getOrCreate()
)

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
        .limit(100)
    )

    df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .save()

    print("==============================")
    print("SPARK -> MONGODB SUCCESS")
    print("ROWS WRITTEN: 100")
    print("==============================")

finally:
    spark.stop()