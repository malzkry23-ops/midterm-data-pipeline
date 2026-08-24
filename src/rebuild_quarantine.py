import os
import sys
import json
import time
from pathlib import Path

from pymongo import MongoClient

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

# نفس قواعد التنظيف التي استخدمناها في التشغيل الناجح
from spark_transform import transform_data, FIELDS


# =========================================================
# الإعدادات
# =========================================================

MONGO_URI = "mongodb://127.0.0.1:27017"

DATABASE_NAME = "midterm_pipeline"

RAW_COLLECTION = "orders_raw"

QUARANTINE_COLLECTION = "quarantine_orders"

SPARK_TEMP = r"D:\spark-temp"


# Python المستخدم مع Spark على Windows
PYTHON_EXECUTABLE = (
    r"C:\Users\Hp\AppData\Local\Programs"
    r"\Python\Python313\python.exe"
)


os.environ["PYSPARK_PYTHON"] = PYTHON_EXECUTABLE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXECUTABLE


# =========================================================
# Schema ثابتة للـ Raw
# =========================================================

RAW_RECORD_SCHEMA = StructType([
    StructField(
        field,
        StringType(),
        True
    )
    for field in FIELDS
])


RAW_SCHEMA = StructType([

    StructField(
        "run_id",
        StringType(),
        True
    ),

    StructField(
        "source_file",
        StringType(),
        True
    ),

    StructField(
        "engine_used",
        StringType(),
        True
    ),

    StructField(
        "raw_record",
        RAW_RECORD_SCHEMA,
        True
    ),
])


# =========================================================
# إعادة بناء Quarantine فقط
# =========================================================

def rebuild_quarantine(run_id):

    project_root = (
        Path(__file__)
        .resolve()
        .parents[1]
    )


    # =====================================================
    # نقرأ النتيجة السابقة
    # حتى نعرف العدد المتوقع للـ Quarantine
    # =====================================================

    previous_result_file = (
        project_root
        / "reports"
        / "spark_transform_fast_results.json"
    )


    expected_quarantine = None


    if previous_result_file.exists():

        try:

            with open(
                previous_result_file,
                "r",
                encoding="utf-8"
            ) as file:

                previous_results = json.load(
                    file
                )


            if (
                previous_results.get("run_id")
                == run_id
            ):

                expected_quarantine = (
                    previous_results.get(
                        "quarantine"
                    )
                )

        except Exception:

            expected_quarantine = None


    # =====================================================
    # MongoDB
    # =====================================================

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=None
    )


    db = client[
        DATABASE_NAME
    ]


    raw_collection = db[
        RAW_COLLECTION
    ]


    quarantine_collection = db[
        QUARANTINE_COLLECTION
    ]


    # =====================================================
    # التأكد أن Run موجود
    # =====================================================

    first_raw = raw_collection.find_one(

        {
            "run_id":
                run_id
        },

        {
            "source_file": 1
        }
    )


    if not first_raw:

        print(
            "ERROR: Run ID not found in orders_raw"
        )

        client.close()

        return


    source_file = first_raw.get(
        "source_file",
        ""
    )


    # =====================================================
    # حذف Quarantine الجزئية لنفس Run فقط
    # =====================================================

    print(
        "\nRemoving partial Quarantine..."
    )


    deleted = (
        quarantine_collection
        .delete_many(
            {
                "run_id":
                    run_id
            }
        )
        .deleted_count
    )


    print(
        "Old quarantine deleted:",
        deleted
    )


    # Index للبحث حسب Run ID
    quarantine_collection.create_index(
        "run_id"
    )


    print(
        "\n===================================="
    )

    print(
        "REBUILD QUARANTINE ONLY"
    )

    print(
        "===================================="
    )

    print(
        "Run ID:",
        run_id
    )

    print(
        "Source:",
        source_file
    )


    if expected_quarantine is not None:

        print(
            "Expected Quarantine:",
            expected_quarantine
        )


    # =====================================================
    # Spark
    # =====================================================

    spark = (

        SparkSession.builder

        .appName(
            "RebuildQuarantineOnly"
        )

        # 4 Threads أكثر استقراراً على Windows
        .master(
            "local[4]"
        )

        .config(
            "spark.pyspark.python",
            PYTHON_EXECUTABLE
        )

        .config(
            "spark.pyspark.driver.python",
            PYTHON_EXECUTABLE
        )

        .config(
            "spark.python.worker.reuse",
            "true"
        )

        .config(
            "spark.local.dir",
            SPARK_TEMP
        )

        .config(
            "spark.sql.shuffle.partitions",
            "96"
        )

        .config(
            "spark.mongodb.read.connection.uri",
            MONGO_URI
        )

        .config(
            "spark.mongodb.write.connection.uri",
            MONGO_URI
        )

        .getOrCreate()
    )


    spark.sparkContext.setLogLevel(
        "WARN"
    )


    start_time = time.time()


    try:

        # =================================================
        # نقرأ فقط الـ Run الصحيح
        # =================================================

        pipeline = json.dumps(
            [
                {
                    "$match": {
                        "run_id":
                            run_id
                    }
                }
            ]
        )


        raw_df = (

            spark.read

            .format(
                "mongodb"
            )

            .schema(
                RAW_SCHEMA
            )

            .option(
                "connection.uri",
                MONGO_URI
            )

            .option(
                "database",
                DATABASE_NAME
            )

            .option(
                "collection",
                RAW_COLLECTION
            )

            .option(
                "aggregation.pipeline",
                pipeline
            )

            .option(
                "partitioner",
                "com.mongodb.spark.sql.connector."
                "read.partitioner.SamplePartitioner"
            )

            .option(
                "partitioner.options.partition.field",
                "_id"
            )

            .option(
                "partitioner.options.partition.size",
                "128"
            )

            .option(
                "partitioner.options.samples.per.partition",
                "10"
            )

            .load()
        )


        input_partitions = (
            raw_df.rdd
            .getNumPartitions()
        )


        print(
            "Mongo Input Partitions:",
            input_partitions
        )


        print(
            "\nApplying quality rules..."
        )


        # =================================================
        # نستخدم نفس Transform الذي أعطى
        # النتيجة النهائية الصحيحة سابقاً
        # =================================================

        transformed = transform_data(
            raw_df
        )


        # =================================================
        # نأخذ Quarantine فقط
        # =================================================

        quarantine_df = (

            transformed

            .filter(
                F.col(
                    "quality_status"
                )
                == "quarantined"
            )

            .select(

                F.lit(
                    run_id
                ).alias(
                    "run_id"
                ),

                F.col(
                    "source_file"
                ),

                F.lit(
                    "pyspark"
                ).alias(
                    "engine_used"
                ),

                F.col(
                    "quality_status"
                ),

                F.col(
                    "error_codes"
                ),

                F.col(
                    "error_details"
                ),

                F.col(
                    "corrections"
                ),

                F.col(
                    "_raw_record_struct"
                ).alias(
                    "raw_record"
                ),

                F.col(
                    "_clean_record_struct"
                ).alias(
                    "clean_record"
                )
            )
        )


        print(
            "\nWriting Quarantine only..."
        )


        # =================================================
        # الكتابة مباشرة بواسطة MongoDB Spark Connector
        #
        # لا نستخدم foreachPartition Python هنا
        # لذلك نتجنب مشكلة Python Worker Timeout
        # =================================================

        (
            quarantine_df.write

            .format(
                "mongodb"
            )

            .mode(
                "append"
            )

            .option(
                "connection.uri",
                MONGO_URI
            )

            .option(
                "database",
                DATABASE_NAME
            )

            .option(
                "collection",
                QUARANTINE_COLLECTION
            )

            .option(
                "ordered",
                "false"
            )

            .option(
                "maxBatchSize",
                "1000"
            )

            .save()
        )


        # =================================================
        # التحقق النهائي
        # =================================================

        quarantine_count = (
            quarantine_collection
            .count_documents(
                {
                    "run_id":
                        run_id
                }
            )
        )


        elapsed = (
            time.time()
            - start_time
        )


        # =================================================
        # عدد كل Error Code
        # =================================================

        error_counts = {}


        error_pipeline = [

            {
                "$match": {
                    "run_id":
                        run_id
                }
            },

            {
                "$unwind":
                    "$error_codes"
            },

            {
                "$group": {

                    "_id":
                        "$error_codes",

                    "count": {
                        "$sum": 1
                    }
                }
            }
        ]


        for item in (
            quarantine_collection.aggregate(
                error_pipeline,
                allowDiskUse=True
            )
        ):

            error_counts[
                item["_id"]
            ] = item[
                "count"
            ]


        # =================================================
        # مقارنة العدد بالنتيجة السابقة
        # =================================================

        matches_expected = (

            expected_quarantine is None

            or

            quarantine_count
            == expected_quarantine
        )


        # =================================================
        # حفظ Metrics
        # =================================================

        metrics = {

            "run_id":
                run_id,

            "source_file":
                source_file,

            "operation":
                "rebuild_quarantine_only",

            "input_partitions":
                input_partitions,

            "expected_quarantine":
                expected_quarantine,

            "actual_quarantine":
                quarantine_count,

            "matches_expected":
                matches_expected,

            "error_counts":
                error_counts,

            "elapsed_seconds":
                round(
                    elapsed,
                    2
                )
        }


        result_file = (

            project_root
            / "reports"
            / "rebuild_quarantine_results.json"
        )


        with open(
            result_file,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                metrics,
                file,
                ensure_ascii=False,
                indent=4
            )


        # =================================================
        # النتيجة
        # =================================================

        print(
            "\n===================================="
        )

        print(
            "QUARANTINE REBUILD COMPLETED"
        )

        print(
            "===================================="
        )

        print(
            "EXPECTED:",
            expected_quarantine
        )

        print(
            "ACTUAL:",
            quarantine_count
        )

        print(
            "MATCH:",
            matches_expected
        )

        print(
            "TIME:",
            round(
                elapsed,
                2
            ),
            "seconds"
        )

        print(
            "\nRESULTS:"
        )

        print(
            result_file
        )


    finally:

        spark.stop()

        client.close()


# =========================================================
# نقطة التشغيل
# =========================================================

if __name__ == "__main__":

    if len(
        sys.argv
    ) < 2:

        print(
            "Usage: "
            "spark-submit "
            "rebuild_quarantine.py RUN_ID"
        )

    else:

        rebuild_quarantine(
            sys.argv[1]
        )