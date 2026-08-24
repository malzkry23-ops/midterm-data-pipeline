from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

FILE = r"D:\orders_huge_mixed_quality.csv"

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
    .appName("CSVParsingTest")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read
    .option("header", True)
    .option("quote", '"')
    .option("escape", '"')
    .schema(schema)
    .csv(FILE)
)

rows = df.select(
    "order_id",
    "items_json"
).limit(5).collect()

for row in rows:
    print("\nORDER:", row["order_id"])
    print("ITEMS:", repr(row["items_json"]))

spark.stop()