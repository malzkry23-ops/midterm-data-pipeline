import os
import sys
import time
import uuid
import json
from pathlib import Path

from pymongo import MongoClient

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,
    struct,
    col,
    current_timestamp,
    spark_partition_id
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


# =========================================================
# الإعدادات
# =========================================================

MONGO_URI = "mongodb://127.0.0.1:27017"

DATABASE_NAME = "midterm_pipeline"

RAW_COLLECTION = "orders_raw"


# =========================================================
# Schema ثابتة كما طلب الدكتور
# نخزن جميع الحقول String في Raw
# حتى نحافظ على البيانات كما وصلت قبل التنظيف
# =========================================================

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

    StructField("items_json", StringType(), True)

])


# =========================================================
# تشغيل Spark Raw Loader
# =========================================================

def load_big_file(file_path):

    # Run ID جديد لكل عملية تحميل
    run_id = str(uuid.uuid4())

    file_size_mb = (
        os.path.getsize(file_path)
        / (1024 * 1024)
    )


    # =====================================================
    # إنشاء Spark Session
    # =====================================================

    spark = (
        SparkSession.builder

        .appName(
            "MidtermBigDataRawLoader"
        )

        .master(
            "local[*]"
        )

        # حجم كل Partition عند قراءة الملف
        # لا نستخدم repartition حتى لا نسبب Shuffle غير ضروري
        .config(
            "spark.sql.files.maxPartitionBytes",
            "134217728"
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

        print(
            "\n===================================="
        )

        print(
            "PYSPARK RAW LOAD"
        )

        print(
            "===================================="
        )


        print(
            "Run ID:",
            run_id
        )


        print(
            "File Size:",
            round(
                file_size_mb,
                2
            ),
            "MB"
        )


        # =================================================
        # قراءة CSV
        #
        # مهم:
        # quote + escape ضروريان لأن items_json يحتوي
        # على علامات اقتباس مزدوجة داخل CSV
        # =================================================

        df = (
            spark.read

            .option(
                "header",
                True
            )

            # الملف UTF-8
            .option(
                "encoding",
                "UTF-8"
            )

            # بداية ونهاية النص المحاط بعلامات اقتباس
            .option(
                "quote",
                '"'
            )

            # يسمح بقراءة "" داخل items_json بشكل صحيح
            .option(
                "escape",
                '"'
            )

            # لا نستخدم inferSchema
            # Schema ثابتة كما طلب الدكتور
            .schema(
                schema
            )

            .csv(
                file_path
            )
        )


        # =================================================
        # عدد Partitions الأصلية
        # =================================================

        input_partitions = (
            df.rdd
            .getNumPartitions()
        )


        print(
            "Input Partitions:",
            input_partitions
        )


        # =================================================
        # إنشاء raw_record
        #
        # جميع بيانات CSV تبقى داخل raw_record
        # بدون تنظيف أو تعديل
        # =================================================

        raw_record = struct(
            *[
                col(
                    field.name
                ).alias(
                    field.name
                )

                for field in schema.fields
            ]
        )


        # =================================================
        # إضافة Metadata إلى Raw
        # =================================================

        raw_df = (

            df.select(

                lit(
                    run_id
                ).alias(
                    "run_id"
                ),

                lit(
                    file_path
                ).alias(
                    "source_file"
                ),

                # رقم Partition يساعد في التتبع
                spark_partition_id().alias(
                    "source_partition_id"
                ),

                current_timestamp().alias(
                    "ingested_at"
                ),

                lit(
                    "pyspark"
                ).alias(
                    "engine_used"
                ),

                raw_record.alias(
                    "raw_record"
                )
            )
        )


        print(
            "\nWriting RAW data to MongoDB..."
        )

        print(
            "Please wait...\n"
        )


        # =================================================
        # الكتابة المتوازية إلى MongoDB
        # =================================================

        (
            raw_df.write

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
                RAW_COLLECTION
            )

            .save()
        )


        # =================================================
        # التأكد من عدد السجلات التي وصلت إلى MongoDB
        # =================================================

        client = MongoClient(
            MONGO_URI
        )


        db = client[
            DATABASE_NAME
        ]


        raw_collection = db[
            RAW_COLLECTION
        ]


        loaded_raw = (
            raw_collection
            .count_documents(
                {
                    "run_id":
                        run_id
                }
            )
        )


        client.close()


        # =================================================
        # Metrics
        # =================================================

        elapsed = (
            time.time()
            - start_time
        )


        throughput = (
            loaded_raw
            / elapsed

            if elapsed > 0

            else 0
        )


        metrics = {

            "run_id":
                run_id,

            "file_name":
                os.path.basename(
                    file_path
                ),

            "file_size_mb":
                round(
                    file_size_mb,
                    2
                ),

            "used_engine":
                "pyspark",

            "read_rows":
                loaded_raw,

            "loaded_raw":
                loaded_raw,

            "input_partitions":
                input_partitions,

            "seconds_elapsed":
                round(
                    elapsed,
                    2
                ),

            "throughput":
                round(
                    throughput,
                    2
                )
        }


        # =================================================
        # حفظ Metrics
        # =================================================

        project_root = (
            Path(__file__)
            .resolve()
            .parents[1]
        )


        results_file = (
            project_root
            / "reports"
            / "spark_raw_results.json"
        )


        with open(
            results_file,
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
        # النتيجة النهائية
        # =================================================

        print(
            "\n===================================="
        )

        print(
            "PYSPARK RAW LOAD COMPLETED"
        )

        print(
            "===================================="
        )


        print(
            "Run ID:",
            run_id
        )


        print(
            "Loaded Raw:",
            loaded_raw
        )


        print(
            "Partitions:",
            input_partitions
        )


        print(
            "Time:",
            round(
                elapsed,
                2
            ),
            "seconds"
        )


        print(
            "Throughput:",
            round(
                throughput,
                2
            ),
            "rows/sec"
        )


        print(
            "\nMetrics:"
        )


        print(
            results_file
        )


    except Exception as error:

        print(
            "\nPYSPARK LOAD FAILED"
        )


        print(
            "ERROR:",
            error
        )


        raise


    finally:

        spark.stop()


# =========================================================
# نقطة التشغيل
# =========================================================

if __name__ == "__main__":

    if len(
        sys.argv
    ) < 2:

        print(
            "Usage: "
            "spark-submit spark_loader.py FILE_PATH"
        )

    else:

        load_big_file(
            sys.argv[1]
        )