from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Phase2_Portability_Smoke_Test")
    .master("local[2]")
    .getOrCreate()
)

count = spark.range(10).count()

print("SPARK_SMOKE_TEST_OK")
print("COUNT =", count)
print("SPARK_VERSION =", spark.version)

spark.stop()
