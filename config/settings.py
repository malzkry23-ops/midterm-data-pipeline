import os
import sys
import shutil
import tempfile
from pathlib import Path


# =========================================================
# ????????? ???????? ???????
# =========================================================

SMALL_FILE_THRESHOLD_MB = int(
    os.getenv("SMALL_FILE_THRESHOLD_MB", "200")
)

BATCH_SIZE = int(
    os.getenv("BATCH_SIZE", "5000")
)

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://127.0.0.1:27017"
)

DATABASE_NAME = os.getenv(
    "DATABASE_NAME",
    "midterm_pipeline"
)

RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "quarantine_orders"

DEFAULT_SAMPLE_ROWS = int(
    os.getenv("DEFAULT_SAMPLE_ROWS", "100000")
)


# =========================================================
# Python
# =========================================================

PYTHON_EXECUTABLE = (
    os.getenv("PYTHON_EXECUTABLE")
    or sys.executable
)


# =========================================================
# Spark Temp
# =========================================================

SPARK_TEMP = os.getenv(
    "SPARK_TEMP",
    str(
        Path(tempfile.gettempdir())
        / "midterm-spark-temp"
    )
)


# =========================================================
# Java
# =========================================================

JAVA_HOME = os.getenv("JAVA_HOME", "")

if not JAVA_HOME:

    java_path = shutil.which("java")

    if java_path:

        try:
            JAVA_HOME = str(
                Path(java_path)
                .resolve()
                .parents[1]
            )
        except Exception:
            JAVA_HOME = ""


# =========================================================
# PySpark
# =========================================================

SPARK_HOME = os.getenv("SPARK_HOME", "")

if not SPARK_HOME:

    try:

        import pyspark

        SPARK_HOME = str(
            Path(pyspark.__file__)
            .resolve()
            .parent
        )

    except Exception:

        SPARK_HOME = ""
