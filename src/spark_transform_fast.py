import os
import sys
import json
import time
from pathlib import Path

from pymongo import MongoClient, UpdateOne, ReplaceOne

from pyspark import TaskContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

# نستخدم نفس قواعد التنظيف التي نجحت على 100 ألف سجل
from spark_transform import transform_data, FIELDS


# =========================================================
# الإعدادات
# =========================================================

MONGO_URI = "mongodb://127.0.0.1:27017"

DATABASE_NAME = "midterm_pipeline"

RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "quarantine_orders"
STATS_COLLECTION = "spark_partition_stats"

SPARK_TEMP = r"D:\spark-temp"

PYTHON_EXECUTABLE = (
    r"C:\Users\Hp\AppData\Local\Programs\Python"
    r"\Python313\python.exe"
)

BULK_SIZE = 1000


# =========================================================
# نحدد Python بشكل صريح
# مهم جدًا على Windows
# =========================================================

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
# تحويل Spark Row إلى Dictionary
# =========================================================

def to_python(value):

    if value is None:
        return None

    if hasattr(value, "asDict"):

        return value.asDict(
            recursive=True
        )

    if isinstance(value, list):

        return [
            to_python(item)
            for item in value
        ]

    return value


# =========================================================
# كتابة Valid / Corrected
# =========================================================

def flush_validated(
    collection,
    operations,
    counters
):

    if not operations:
        return

    result = collection.bulk_write(
        operations,
        ordered=False
    )

    counters["inserted"] += (
        result.upserted_count
    )

    counters["updated"] += (
        result.modified_count
    )

    counters["unchanged"] += max(
        0,
        result.matched_count
        - result.modified_count
    )

    operations.clear()


# =========================================================
# كتابة Quarantine
# =========================================================

def flush_quarantine(
    collection,
    operations
):

    if not operations:
        return

    collection.bulk_write(
        operations,
        ordered=False
    )

    operations.clear()


# =========================================================
# معالجة Partition واحدة
# =========================================================

def write_partition(
    rows,
    run_id
):

    context = TaskContext.get()

    partition_id = (
        context.partitionId()
        if context
        else 0
    )

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=None
    )

    db = client[
        DATABASE_NAME
    ]

    validated_collection = db[
        VALIDATED_COLLECTION
    ]

    quarantine_collection = db[
        QUARANTINE_COLLECTION
    ]

    stats_collection = db[
        STATS_COLLECTION
    ]

    validated_ops = []
    quarantine_ops = []

    counters = {

        "processed": 0,

        "valid": 0,

        "corrected": 0,

        "quarantine": 0,

        "inserted": 0,

        "updated": 0,

        "unchanged": 0,
    }

    try:

        for row_number, row in enumerate(
            rows
        ):

            counters[
                "processed"
            ] += 1

            quality_status = row[
                "quality_status"
            ]


            # =================================================
            # Valid / Corrected
            # =================================================

            if quality_status in (
                "valid",
                "corrected"
            ):

                if quality_status == "valid":

                    counters[
                        "valid"
                    ] += 1

                else:

                    counters[
                        "corrected"
                    ] += 1


                order_id = row[
                    "order_id"
                ]


                document = {

                    "order_id":
                        order_id,

                    "quality_status":
                        quality_status,

                    "record":
                        to_python(
                            row[
                                "clean_record"
                            ]
                        ),

                    "corrections":
                        to_python(
                            row[
                                "corrections"
                            ]
                        ),

                    "record_hash":
                        row[
                            "record_hash"
                        ],

                    "last_run_id":
                        run_id,

                    "engine_used":
                        "pyspark",
                }


                validated_ops.append(

                    UpdateOne(

                        {
                            "order_id":
                                order_id
                        },

                        {
                            "$set":
                                document,

                            "$setOnInsert": {

                                "first_run_id":
                                    run_id
                            }
                        },

                        upsert=True
                    )
                )


                if len(
                    validated_ops
                ) >= BULK_SIZE:

                    flush_validated(
                        validated_collection,
                        validated_ops,
                        counters
                    )


            # =================================================
            # Quarantine
            # =================================================

            else:

                counters[
                    "quarantine"
                ] += 1


                # ID ثابت لمنع Duplicate عند إعادة Task
                quarantine_id = (
                    f"{run_id}:"
                    f"{partition_id}:"
                    f"{row_number}"
                )


                quarantine_document = {

                    "_id":
                        quarantine_id,

                    "run_id":
                        run_id,

                    "source_file":
                        row[
                            "source_file"
                        ],

                    "engine_used":
                        "pyspark",

                    "quality_status":
                        "quarantined",

                    "error_codes":
                        to_python(
                            row[
                                "error_codes"
                            ]
                        ),

                    "error_details":
                        to_python(
                            row[
                                "error_details"
                            ]
                        ),

                    "corrections":
                        to_python(
                            row[
                                "corrections"
                            ]
                        ),

                    "raw_record":
                        to_python(
                            row[
                                "raw_record"
                            ]
                        ),

                    "clean_record":
                        to_python(
                            row[
                                "clean_record"
                            ]
                        ),
                }


                quarantine_ops.append(

                    ReplaceOne(

                        {
                            "_id":
                                quarantine_id
                        },

                        quarantine_document,

                        upsert=True
                    )
                )


                if len(
                    quarantine_ops
                ) >= BULK_SIZE:

                    flush_quarantine(
                        quarantine_collection,
                        quarantine_ops
                    )


        # =====================================================
        # آخر Batch
        # =====================================================

        flush_validated(
            validated_collection,
            validated_ops,
            counters
        )


        flush_quarantine(
            quarantine_collection,
            quarantine_ops
        )


        # =====================================================
        # Metrics للـ Partition
        # =====================================================

        stats_id = (
            f"{run_id}:"
            f"{partition_id}"
        )


        stats_collection.replace_one(

            {
                "_id":
                    stats_id
            },

            {
                "_id":
                    stats_id,

                "run_id":
                    run_id,

                "partition_id":
                    partition_id,

                **counters
            },

            upsert=True
        )


    finally:

        client.close()


# =========================================================
# تشغيل النسخة السريعة
# =========================================================

def run_fast_transform(
    run_id
):

    os.makedirs(
        SPARK_TEMP,
        exist_ok=True
    )


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

    validated_collection = db[
        VALIDATED_COLLECTION
    ]

    quarantine_collection = db[
        QUARANTINE_COLLECTION
    ]

    stats_collection = db[
        STATS_COLLECTION
    ]


    # =====================================================
    # التأكد من Run ID
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
            "ERROR: Run ID not found"
        )

        client.close()

        return


    source_file = first_raw.get(
        "source_file",
        ""
    )


    raw_count = (
        raw_collection
        .count_documents(
            {
                "run_id":
                    run_id
            }
        )
    )


    # =====================================================
    # Indexes
    # =====================================================

    validated_collection.create_index(
        "order_id",
        unique=True
    )


    quarantine_collection.create_index(
        "run_id"
    )


    # =====================================================
    # حذف نتائج التشغيل الجزئية لنفس Run فقط
    # =====================================================

    print(
        "\nRemoving previous partial quarantine..."
    )

    quarantine_collection.delete_many(
        {
            "run_id":
                run_id
        }
    )


    stats_collection.delete_many(
        {
            "run_id":
                run_id
        }
    )


    print(
        "\n===================================="
    )

    print(
        "FAST PYSPARK TRANSFORM"
    )

    print(
        "===================================="
    )

    print(
        "Run ID:",
        run_id
    )

    print(
        "RAW:",
        raw_count
    )

    print(
        "Source:",
        source_file
    )


    # =====================================================
    # Spark Session
    # =====================================================

    spark = (

        SparkSession.builder

        .appName(
            "MidtermFastBigDataTransform"
        )

        # مهم:
        # لا نستخدم local[*] على Windows
        # نستخدم 4 Workers فقط لزيادة الاستقرار
        .master(
            "local[4]"
        )

        # Python المستخدم داخل Spark Workers
        .config(
            "spark.pyspark.python",
            PYTHON_EXECUTABLE
        )

        # Python المستخدم على Driver
        .config(
            "spark.pyspark.driver.python",
            PYTHON_EXECUTABLE
        )

        # إعادة استخدام Python Workers
        # بدل إنشاء Worker جديد لكل Task
        .config(
            "spark.python.worker.reuse",
            "true"
        )

        # أقصى عدد Workers خاملين نحتفظ بهم
        .config(
            "spark.python.factory.idleWorkerMaxPoolSize",
            "4"
        )

        # إظهار تفاصيل أفضل لو حصل خطأ Python
        .config(
            "spark.python.worker.faulthandler.enabled",
            "true"
        )

        .config(
            "spark.local.dir",
            SPARK_TEMP
        )

        # نحتفظ بعدد مناسب من Shuffle Partitions
        # لكن التنفيذ المتزامن سيكون 4 فقط
        .config(
            "spark.sql.shuffle.partitions",
            "96"
        )

        .config(
            "spark.mongodb.read.connection.uri",
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
        # نقرأ فقط Run المطلوب
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

            # Schema ثابتة
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

            # القراءة المتوازية من MongoDB
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
            "\nApplying cleaning rules..."
        )


        # =================================================
        # نفس قواعد التنظيف المجربة
        # =================================================

        transformed = transform_data(
            raw_df
        )


        # =================================================
        # الأعمدة المطلوبة فقط
        # =================================================

        output_df = transformed.select(

            F.col(
                "order_id"
            ),

            F.col(
                "quality_status"
            ),

            F.col(
                "_clean_record_struct"
            ).alias(
                "clean_record"
            ),

            F.col(
                "_raw_record_struct"
            ).alias(
                "raw_record"
            ),

            F.col(
                "corrections"
            ),

            F.col(
                "error_codes"
            ),

            F.col(
                "error_details"
            ),

            F.col(
                "_record_hash"
            ).alias(
                "record_hash"
            ),

            F.col(
                "source_file"
            )
        )


        print(
            "\nCleaning + writing in ONE PASS..."
        )


        # =================================================
        # Action واحدة
        # =================================================

        output_df.foreachPartition(

            lambda rows:
                write_partition(
                    rows,
                    run_id
                )
        )


        # =================================================
        # Metrics النهائية
        # =================================================

        count_valid = (
            validated_collection
            .count_documents(
                {
                    "last_run_id":
                        run_id,

                    "quality_status":
                        "valid"
                }
            )
        )


        count_corrected = (
            validated_collection
            .count_documents(
                {
                    "last_run_id":
                        run_id,

                    "quality_status":
                        "corrected"
                }
            )
        )


        count_quarantine = (
            quarantine_collection
            .count_documents(
                {
                    "run_id":
                        run_id
                }
            )
        )


        # =================================================
        # تجميع إحصائيات Partitions
        # =================================================

        stats = list(

            stats_collection.aggregate([

                {
                    "$match": {

                        "run_id":
                            run_id
                    }
                },

                {
                    "$group": {

                        "_id":
                            None,

                        "processed": {
                            "$sum":
                                "$processed"
                        },

                        "inserted": {
                            "$sum":
                                "$inserted"
                        },

                        "updated": {
                            "$sum":
                                "$updated"
                        },

                        "unchanged": {
                            "$sum":
                                "$unchanged"
                        }
                    }
                }
            ])
        )


        if stats:

            write_stats = stats[
                0
            ]

        else:

            write_stats = {}


        elapsed = (
            time.time()
            - start_time
        )


        throughput = (

            raw_count
            / elapsed

            if elapsed > 0

            else 0
        )


        classified = (

            count_valid
            + count_corrected
            + count_quarantine
        )


        consistency = (
            raw_count
            == classified
        )


        # =================================================
        # عدد الأخطاء
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
        # النتائج
        # =================================================

        metrics = {

            "run_id":
                run_id,

            "source_file":
                source_file,

            "engine":
                "pyspark_fast",

            "raw_count":
                raw_count,

            "valid":
                count_valid,

            "corrected":
                count_corrected,

            "quarantine":
                count_quarantine,

            "input_partitions":
                input_partitions,

            "inserted":
                write_stats.get(
                    "inserted",
                    0
                ),

            "updated":
                write_stats.get(
                    "updated",
                    0
                ),

            "unchanged":
                write_stats.get(
                    "unchanged",
                    0
                ),

            "error_counts":
                error_counts,

            "elapsed_seconds":
                round(
                    elapsed,
                    2
                ),

            "throughput_rows_per_sec":
                round(
                    throughput,
                    2
                ),

            "consistency":
                consistency
        }


        project_root = (
            Path(__file__)
            .resolve()
            .parents[1]
        )


        result_file = (

            project_root
            / "reports"
            / "spark_transform_fast_results.json"
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
        # عرض النتيجة
        # =================================================

        print(
            "\n===================================="
        )

        print(
            "FAST TRANSFORM COMPLETED"
        )

        print(
            "===================================="
        )

        print(
            "RAW:",
            raw_count
        )

        print(
            "VALID:",
            count_valid
        )

        print(
            "CORRECTED:",
            count_corrected
        )

        print(
            "QUARANTINE:",
            count_quarantine
        )

        print(
            "INSERTED:",
            write_stats.get(
                "inserted",
                0
            )
        )

        print(
            "UPDATED:",
            write_stats.get(
                "updated",
                0
            )
        )

        print(
            "UNCHANGED:",
            write_stats.get(
                "unchanged",
                0
            )
        )

        print(
            "CONSISTENCY:",
            consistency
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
            "THROUGHPUT:",
            round(
                throughput,
                2
            ),
            "rows/sec"
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
            "spark_transform_fast.py RUN_ID"
        )

    else:

        run_fast_transform(
            sys.argv[1]
        )