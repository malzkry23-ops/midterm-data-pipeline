# =========================================================
# إعدادات المشروع المركزية
# =========================================================

# حد اختيار المحرك بالميجابايت
SMALL_FILE_THRESHOLD_MB = 200

# حجم Batch في مسار Python
BATCH_SIZE = 5000

# MongoDB
MONGO_URI = "mongodb://127.0.0.1:27017"
DATABASE_NAME = "midterm_pipeline"

RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "quarantine_orders"

# Spark
SPARK_TEMP = r"D:\spark-temp"

# Python المستخدم مع PySpark
PYTHON_EXECUTABLE = (
    r"C:\Users\Hp\AppData\Local\Programs"
    r"\Python\Python313\python.exe"
)

# العينة الصغيرة
DEFAULT_SAMPLE_ROWS = 100000

# =========================================================
# بيئة Spark على جهاز المناقشة
# =========================================================

JAVA_HOME = (
    r"C:\Program Files\Eclipse Adoptium"
    r"\jdk-17.0.20.8-hotspot"
)

SPARK_HOME = (
    r"C:\Users\Hp\AppData\Local\Programs"
    r"\Python\Python313\Lib\site-packages\pyspark"
)
