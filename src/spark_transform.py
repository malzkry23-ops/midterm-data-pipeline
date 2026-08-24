import os
import sys
import time
import json
from pathlib import Path

from pymongo import MongoClient

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
    DoubleType,
)


# =========================================================
# الإعدادات
# =========================================================

MONGO_URI = "mongodb://127.0.0.1:27017"

DATABASE_NAME = "midterm_pipeline"

RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "quarantine_orders"

SPARK_TEMP = r"D:\spark-temp"


# =========================================================
# أعمدة ملف الدكتور
# =========================================================

FIELDS = [
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "delivery_cost",
    "payment_method",
    "payment_status",
    "payment_amount",
    "currency",
    "total_amount",
    "items_json",
]


# =========================================================
# القيم الرقمية المكتوبة بالكلمات
# =========================================================

KNOWN_NUMBER_WORDS = {
    "ألف": "1000",
    "ألفان": "2000",
    "ألفين": "2000",
    "ثلاثة آلاف": "3000",
    "أربعة آلاف": "4000",
    "خمسة آلاف": "5000",
    "ستة آلاف": "6000",
    "سبعة آلاف": "7000",
    "ثمانية آلاف": "8000",
    "تسعة آلاف": "9000",
    "عشرة آلاف": "10000",
}


# =========================================================
# حالات الطلب الصحيحة
# =========================================================

VALID_STATUSES = [
    "مرتجع",
    "مؤكد",
    "ملغي",
    "قيد الشحن",
    "قيد الانتظار",
    "تم التسليم",
]


# =========================================================
# وصف أخطاء Quarantine
# =========================================================

ERROR_DETAILS = {
    "MISSING_ORDER_ID":
        "معرف الطلب مفقود ولا يمكن استنتاجه",

    "MISSING_CUSTOMER_ID":
        "معرف العميل مفقود",

    "INVALID_DATE":
        "التاريخ غير صالح أو غير قابل للتفسير",

    "INVALID_EMAIL":
        "البريد الإلكتروني غير صالح ولا يمكن تصحيحه بأمان",

    "UNKNOWN_CURRENCY":
        "العملة غير معروفة",

    "UNKNOWN_STATUS":
        "حالة الطلب غير معروفة",

    "CORRUPTED_ITEMS_JSON":
        "items_json تالف",

    "EMPTY_ITEMS":
        "الطلب لا يحتوي على عناصر",

    "TOTAL_CANNOT_BE_CALCULATED":
        "لا يمكن إعادة حساب إجمالي الطلب",

    "DUPLICATE_ORDER_ID":
        "order_id مكرر داخل نفس التشغيل",
}


# =========================================================
# Schema الخاص بـ items_json
# =========================================================

ITEMS_SCHEMA = ArrayType(
    StructType([
        StructField("sku", StringType(), True),
        StructField("name", StringType(), True),
        StructField("qty", DoubleType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total", DoubleType(), True),
    ])
)


# =========================================================
# إنشاء Audit Trail
# =========================================================

def correction(
    field_name,
    old_value,
    new_value,
    rule_code,
    condition
):

    return F.when(
        condition,

        F.struct(
            F.lit(field_name).alias("field"),

            old_value.cast("string").alias(
                "original_value"
            ),

            new_value.cast("string").alias(
                "corrected_value"
            ),

            F.lit(rule_code).alias(
                "rule_code"
            )
        )
    )


# =========================================================
# تنظيف وتصنيف البيانات
# =========================================================

def transform_data(df):

    # -----------------------------------------------------
    # استخراج السجل الخام
    # -----------------------------------------------------

    work = df.select(
        F.col("source_file"),
        F.col("engine_used"),

        *[
            F.col(f"raw_record.{field}")
            .cast("string")
            .alias(f"raw_{field}")

            for field in FIELDS
        ]
    )

    correction_columns = []


    # =====================================================
    # 1- إزالة المسافات الزائدة
    # =====================================================

    for field in FIELDS:

        old_value = F.col(
            f"raw_{field}"
        )

        new_value = F.trim(
            old_value
        )

        name = (
            f"_corr_trim_{field}"
        )

        work = work.withColumn(
            name,

            correction(
                field,
                old_value,
                new_value,
                "TRIM_SPACES",

                old_value.isNotNull()
                & (old_value != new_value)
            )
        )

        correction_columns.append(
            name
        )

        work = work.withColumn(
            field,
            new_value
        )


    # =====================================================
    # 2- الأرقام المكتوبة بالكلمات
    # =====================================================

    number_fields = [
        "delivery_cost",
        "payment_amount",
        "total_amount",
    ]

    for field in number_fields:

        old_value = F.col(
            field
        )

        expressions = [

            F.when(
                old_value == word,
                F.lit(number)
            )

            for word, number
            in KNOWN_NUMBER_WORDS.items()
        ]

        new_value = F.coalesce(
            *expressions,
            old_value
        )

        condition = old_value.isin(
            list(
                KNOWN_NUMBER_WORDS.keys()
            )
        )

        name = (
            f"_corr_words_{field}"
        )

        work = work.withColumn(
            name,

            correction(
                field,
                old_value,
                new_value,
                "KNOWN_NUMBER_WORD_TO_NUMBER",
                condition
            )
        )

        correction_columns.append(
            name
        )

        work = work.withColumn(
            field,
            new_value
        )


    # =====================================================
    # 3- الأرقام العربية → الإنجليزية
    # =====================================================

    arabic_numeric_fields = [
        "customer_phone",
        "delivery_cost",
        "payment_amount",
        "total_amount",
    ]

    for field in arabic_numeric_fields:

        old_value = F.col(
            field
        )

        new_value = F.translate(
            old_value,
            "٠١٢٣٤٥٦٧٨٩٫",
            "0123456789."
        )

        condition = (
            old_value.isNotNull()
            & (old_value != new_value)
        )

        name = (
            f"_corr_digits_{field}"
        )

        work = work.withColumn(
            name,

            correction(
                field,
                old_value,
                new_value,
                "ARABIC_DIGITS_TO_LATIN",
                condition
            )
        )

        correction_columns.append(
            name
        )

        work = work.withColumn(
            field,
            new_value
        )


    # =====================================================
    # 4- إزالة فواصل الآلاف
    # =====================================================

    for field in number_fields:

        old_value = F.col(
            field
        )

        new_value = F.regexp_replace(
            old_value,
            r"[,٬]",
            ""
        )

        condition = (
            old_value.isNotNull()
            & (old_value != new_value)
        )

        name = (
            f"_corr_separator_{field}"
        )

        work = work.withColumn(
            name,

            correction(
                field,
                old_value,
                new_value,
                "REMOVE_THOUSANDS_SEPARATOR",
                condition
            )
        )

        correction_columns.append(
            name
        )

        work = work.withColumn(
            field,
            new_value
        )


    # =====================================================
    # 5- توحيد العملة
    # =====================================================

    old_currency = F.col(
        "currency"
    )

    new_currency = (
        F.when(
            old_currency == "ريال يمني",
            F.lit("YER")
        )
        .otherwise(
            old_currency
        )
    )

    work = work.withColumn(
        "_corr_currency",

        correction(
            "currency",
            old_currency,
            new_currency,
            "NORMALIZE_CURRENCY_YER",

            old_currency
            == "ريال يمني"
        )
    )

    correction_columns.append(
        "_corr_currency"
    )

    work = work.withColumn(
        "currency",
        new_currency
    )


    # =====================================================
    # 6- توحيد حالة الدفع
    # =====================================================

    old_payment_status = F.col(
        "payment_status"
    )

    new_payment_status = (
        F.when(
            old_payment_status == "مدفوع",
            F.lit("تم الدفع")
        )
        .otherwise(
            old_payment_status
        )
    )

    work = work.withColumn(
        "_corr_payment_status",

        correction(
            "payment_status",
            old_payment_status,
            new_payment_status,
            "NORMALIZE_PAYMENT_STATUS",

            old_payment_status
            == "مدفوع"
        )
    )

    correction_columns.append(
        "_corr_payment_status"
    )

    work = work.withColumn(
        "payment_status",
        new_payment_status
    )


    # =====================================================
    # 7- تنظيف رقم الهاتف
    # =====================================================

    old_phone = F.col(
        "customer_phone"
    )

    compact_phone = F.regexp_replace(
        old_phone,
        r"[\s\-\(\)]",
        ""
    )

    new_phone = (
        F.when(
            compact_phone.startswith(
                "+967"
            ),

            F.substring(
                compact_phone,
                5,
                100
            )
        )

        .when(
            compact_phone.startswith(
                "967"
            )
            & (
                F.length(
                    compact_phone
                ) > 9
            ),

            F.substring(
                compact_phone,
                4,
                100
            )
        )

        .otherwise(
            compact_phone
        )
    )

    work = work.withColumn(
        "_corr_phone",

        correction(
            "customer_phone",
            old_phone,
            new_phone,
            "NORMALIZE_PHONE",

            old_phone.isNotNull()
            & (
                old_phone
                != new_phone
            )
        )
    )

    correction_columns.append(
        "_corr_phone"
    )

    work = work.withColumn(
        "customer_phone",
        new_phone
    )


    # =====================================================
    # 8- توحيد التاريخ
    # =====================================================

    old_date = F.col(
        "order_date"
    )

    parsed_date = F.coalesce(

        F.try_to_timestamp(
            old_date,
            F.lit(
                "yyyy-MM-dd'T'HH:mm:ss"
            )
        ),

        F.try_to_timestamp(
            old_date,
            F.lit(
                "yyyy/MM/dd HH:mm:ss"
            )
        ),

        F.try_to_timestamp(
            old_date,
            F.lit(
                "dd-MM-yyyy HH:mm:ss"
            )
        ),

        F.try_to_timestamp(
            old_date,
            F.lit(
                "yyyy-MM-dd HH:mm:ss"
            )
        ),

        F.try_to_timestamp(
            old_date,
            F.lit(
                "dd/MM/yyyy HH:mm:ss"
            )
        )
    )

    normalized_date = F.when(
        parsed_date.isNotNull(),

        F.date_format(
            parsed_date,
            "yyyy-MM-dd'T'HH:mm:ss"
        )
    ).otherwise(
        old_date
    )

    work = work.withColumn(
        "_parsed_date",
        parsed_date
    )

    work = work.withColumn(
        "_corr_date",

        correction(
            "order_date",
            old_date,
            normalized_date,
            "NORMALIZE_DATE",

            parsed_date.isNotNull()
            & (
                old_date
                != normalized_date
            )
        )
    )

    correction_columns.append(
        "_corr_date"
    )

    work = work.withColumn(
        "order_date",
        normalized_date
    )


    # =====================================================
    # 9- إصلاح البريد الإلكتروني
    # =====================================================

    old_email = F.col(
        "customer_email"
    )

    fixed_email = F.regexp_replace(
        old_email,
        r"@+",
        "@"
    )

    fixed_email = F.regexp_replace(
        fixed_email,
        r"\.+",
        "."
    )

    email_pattern = (
        r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    )

    fixed_email_valid = (
        fixed_email.rlike(
            email_pattern
        )
    )

    new_email = F.when(
        fixed_email_valid,
        fixed_email
    ).otherwise(
        old_email
    )

    work = work.withColumn(
        "_corr_email",

        correction(
            "customer_email",
            old_email,
            new_email,
            "FIX_EMAIL_REPEATED_SYMBOLS",

            old_email.isNotNull()
            & fixed_email_valid
            & (
                old_email
                != fixed_email
            )
        )
    )

    correction_columns.append(
        "_corr_email"
    )

    work = work.withColumn(
        "customer_email",
        new_email
    )


    # =====================================================
    # 10- تحليل items_json
    # =====================================================

    work = work.withColumn(
        "_items",

        F.from_json(
            F.col(
                "items_json"
            ),
            ITEMS_SCHEMA
        )
    )


    # =====================================================
    # اكتشاف العناصر التي لا يمكن حسابها
    # =====================================================

    work = work.withColumn(
        "_item_calc_impossible",

        F.when(
            F.col(
                "_items"
            ).isNull(),

            F.lit(True)
        )

        .otherwise(

            F.exists(
                F.col(
                    "_items"
                ),

                lambda item:

                    item[
                        "total"
                    ].isNull()

                    & (
                        item[
                            "qty"
                        ].isNull()

                        | item[
                            "unit_price"
                        ].isNull()
                    )
            )
        )
    )


    # =====================================================
    # مجموع العناصر
    # =====================================================

    work = work.withColumn(
        "_items_total",

        F.when(
            F.col(
                "_items"
            ).isNotNull(),

            F.aggregate(
                F.col(
                    "_items"
                ),

                F.lit(
                    0.0
                ),

                lambda total, item:

                    total

                    + F.coalesce(
                        item[
                            "total"
                        ],

                        item[
                            "qty"
                        ]

                        * item[
                            "unit_price"
                        ]
                    )
            )
        )
    )


    # =====================================================
    # مهم:
    # نستخدم try_cast بدل cast
    #
    # "2000" -> 2000.0
    # "???"  -> NULL
    # =====================================================

    work = work.withColumn(
        "_delivery_number",

        F.expr(
            "try_cast(delivery_cost AS DOUBLE)"
        )
    )


    work = work.withColumn(
        "_current_total_number",

        F.expr(
            "try_cast(total_amount AS DOUBLE)"
        )
    )


    work = work.withColumn(
        "_can_calculate_total",

        F.col(
            "_items"
        ).isNotNull()

        & (
            F.size(
                F.col(
                    "_items"
                )
            ) > 0
        )

        & (
            ~F.col(
                "_item_calc_impossible"
            )
        )

        & F.col(
            "_delivery_number"
        ).isNotNull()
    )


    work = work.withColumn(
        "_expected_total",

        F.col(
            "_items_total"
        )

        + F.col(
            "_delivery_number"
        )
    )


    # =====================================================
    # 11- إعادة حساب إجمالي الطلب
    # =====================================================

    recalculate_total = (

        F.col(
            "_can_calculate_total"
        )

        & (
            F.col(
                "_current_total_number"
            ).isNull()

            |

            (
                F.abs(
                    F.col(
                        "_current_total_number"
                    )

                    - F.col(
                        "_expected_total"
                    )
                )

                > 0.01
            )
        )
    )


    old_total = F.col(
        "total_amount"
    )

    new_total = F.when(
        recalculate_total,

        F.col(
            "_expected_total"
        ).cast(
            "string"
        )
    ).otherwise(
        old_total
    )


    work = work.withColumn(
        "_corr_total",

        correction(
            "total_amount",
            old_total,
            new_total,
            "RECALCULATE_TOTAL",
            recalculate_total
        )
    )

    correction_columns.append(
        "_corr_total"
    )

    work = work.withColumn(
        "total_amount",
        new_total
    )


    # =====================================================
    # 12- اكتشاف order_id المكرر
    # =====================================================

    duplicate_window = (
        Window.partitionBy(
            "order_id"
        )
    )

    work = work.withColumn(
        "_order_id_count",

        F.count(
            F.lit(1)
        ).over(
            duplicate_window
        )
    )

    duplicate_order = (

        (
            F.coalesce(
                F.col(
                    "order_id"
                ),
                F.lit("")
            )
            != ""
        )

        & (
            F.col(
                "_order_id_count"
            )
            > 1
        )
    )


    # =====================================================
    # إنشاء Audit Trail
    # =====================================================

    work = work.withColumn(
        "corrections",

        F.filter(
            F.array(
                *[
                    F.col(name)

                    for name
                    in correction_columns
                ]
            ),

            lambda value:
                value.isNotNull()
        )
    )


    # =====================================================
    # شروط الأخطاء
    # =====================================================

    missing_order_id = (

        F.trim(
            F.coalesce(
                F.col(
                    "order_id"
                ),
                F.lit("")
            )
        )

        == ""
    )


    missing_customer_id = (

        F.trim(
            F.coalesce(
                F.col(
                    "customer_id"
                ),
                F.lit("")
            )
        )

        == ""
    )


    invalid_date = (
        F.col(
            "_parsed_date"
        ).isNull()
    )


    invalid_email = (

        F.col(
            "customer_email"
        ).isNull()

        |

        (
            ~F.col(
                "customer_email"
            ).rlike(
                email_pattern
            )
        )
    )


    unknown_currency = (

        F.coalesce(
            F.col(
                "currency"
            ),
            F.lit("")
        )

        != "YER"
    )


    unknown_status = (

        ~F.col(
            "status"
        ).isin(
            VALID_STATUSES
        )
    )


    corrupted_items = (
        F.col(
            "_items"
        ).isNull()
    )


    empty_items = (

        F.col(
            "_items"
        ).isNotNull()

        & (
            F.size(
                F.col(
                    "_items"
                )
            )

            == 0
        )
    )


    total_cannot_be_calculated = (

        F.col(
            "_items"
        ).isNotNull()

        & (
            F.size(
                F.col(
                    "_items"
                )
            ) > 0
        )

        & (
            ~F.col(
                "_can_calculate_total"
            )
        )
    )


    # =====================================================
    # error_codes
    # =====================================================

    work = work.withColumn(
        "error_codes",

        F.filter(
            F.array(

                F.when(
                    missing_order_id,
                    F.lit(
                        "MISSING_ORDER_ID"
                    )
                ),

                F.when(
                    missing_customer_id,
                    F.lit(
                        "MISSING_CUSTOMER_ID"
                    )
                ),

                F.when(
                    invalid_date,
                    F.lit(
                        "INVALID_DATE"
                    )
                ),

                F.when(
                    invalid_email,
                    F.lit(
                        "INVALID_EMAIL"
                    )
                ),

                F.when(
                    unknown_currency,
                    F.lit(
                        "UNKNOWN_CURRENCY"
                    )
                ),

                F.when(
                    unknown_status,
                    F.lit(
                        "UNKNOWN_STATUS"
                    )
                ),

                F.when(
                    corrupted_items,
                    F.lit(
                        "CORRUPTED_ITEMS_JSON"
                    )
                ),

                F.when(
                    empty_items,
                    F.lit(
                        "EMPTY_ITEMS"
                    )
                ),

                F.when(
                    total_cannot_be_calculated,
                    F.lit(
                        "TOTAL_CANNOT_BE_CALCULATED"
                    )
                ),

                F.when(
                    duplicate_order,
                    F.lit(
                        "DUPLICATE_ORDER_ID"
                    )
                ),
            ),

            lambda value:
                value.isNotNull()
        )
    )


    # =====================================================
    # error_details
    # =====================================================

    map_values = []

    for code, description in (
        ERROR_DETAILS.items()
    ):

        map_values.extend([
            F.lit(code),
            F.lit(description)
        ])


    error_map = F.create_map(
        *map_values
    )


    work = work.withColumn(
        "error_details",

        F.transform(
            F.col(
                "error_codes"
            ),

            lambda code:

                F.element_at(
                    error_map,
                    code
                )
        )
    )


    # =====================================================
    # التصنيف النهائي
    # =====================================================

    work = work.withColumn(
        "quality_status",

        F.when(
            F.size(
                F.col(
                    "error_codes"
                )
            ) > 0,

            F.lit(
                "quarantined"
            )
        )

        .when(
            F.size(
                F.col(
                    "corrections"
                )
            ) > 0,

            F.lit(
                "corrected"
            )
        )

        .otherwise(
            F.lit(
                "valid"
            )
        )
    )


    # =====================================================
    # بناء Raw Record
    # =====================================================

    raw_record = F.struct(
        *[
            F.col(
                f"raw_{field}"
            ).alias(
                field
            )

            for field in FIELDS
        ]
    )


    # =====================================================
    # بناء Clean Record
    # =====================================================

    clean_record = F.struct(
        *[
            F.col(
                field
            ).alias(
                field
            )

            for field in FIELDS
        ]
    )


    work = work.withColumn(
        "_raw_record_struct",
        raw_record
    )


    work = work.withColumn(
        "_clean_record_struct",
        clean_record
    )


    # =====================================================
    # Hash للسجل المنظف
    # =====================================================

    work = work.withColumn(
        "_record_hash",

        F.sha2(
            F.to_json(
                clean_record
            ),
            256
        )
    )


    return work


# =========================================================
# تشغيل التنظيف
# =========================================================

def run_transform(run_id):

    os.makedirs(
        SPARK_TEMP,
        exist_ok=True
    )


    mongo_client = MongoClient(
        MONGO_URI
    )


    database = mongo_client[
        DATABASE_NAME
    ]


    raw_collection = database[
        RAW_COLLECTION
    ]


    validated_collection = database[
        VALIDATED_COLLECTION
    ]


    quarantine_collection = database[
        QUARANTINE_COLLECTION
    ]


    # =====================================================
    # Unique Index على order_id
    # =====================================================

    validated_collection.create_index(
        "order_id",
        unique=True
    )


    quarantine_collection.create_index(
        "run_id"
    )


    # =====================================================
    # عند إعادة نفس Run نحذف Quarantine القديمة فقط
    # حتى لا يحدث Duplicate
    # =====================================================

    quarantine_collection.delete_many(
        {
            "run_id":
                run_id
        }
    )


    first_raw = raw_collection.find_one(
        {
            "run_id":
                run_id
        },

        {
            "source_file": 1,
            "engine_used": 1
        }
    )


    if not first_raw:

        print(
            "ERROR: Run ID not found in orders_raw"
        )

        mongo_client.close()

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


    validated_before = (
        validated_collection
        .estimated_document_count()
    )


    print(
        "\n===================================="
    )

    print(
        "PYSPARK TRANSFORM"
    )

    print(
        "===================================="
    )

    print(
        "Run ID:",
        run_id
    )

    print(
        "RAW Records:",
        raw_count
    )

    print(
        "Source:",
        source_file
    )


    # =====================================================
    # Spark
    # =====================================================

    spark = (
        SparkSession.builder

        .appName(
            "MidtermBigDataTransform"
        )

        .master(
            "local[*]"
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
        # قراءة Raw لهذا Run ID فقط
        # =================================================

        mongo_pipeline = json.dumps(
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
                mongo_pipeline
            )

            .option(
                "sampleSize",
                "1000"
            )

            .load()
        )


        input_partitions = (
            raw_df.rdd
            .getNumPartitions()
        )


        print(
            "Input Partitions:",
            input_partitions
        )

        print(
            "\nApplying cleaning rules..."
        )


        transformed = transform_data(
            raw_df
        )


        # =================================================
        # Valid + Corrected
        # =================================================

        validated_df = (

            transformed

            .filter(
                F.col(
                    "quality_status"
                ).isin(
                    "valid",
                    "corrected"
                )
            )

            .select(

                F.col(
                    "order_id"
                ),

                F.col(
                    "quality_status"
                ),

                F.col(
                    "_clean_record_struct"
                ).alias(
                    "record"
                ),

                F.col(
                    "corrections"
                ),

                F.col(
                    "_record_hash"
                ).alias(
                    "record_hash"
                ),

                F.lit(
                    run_id
                ).alias(
                    "last_run_id"
                ),

                F.lit(
                    "pyspark"
                ).alias(
                    "engine_used"
                )
            )
        )


        # =================================================
        # Quarantine
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


        # =================================================
        # كتابة Validated باستخدام Upsert
        # =================================================

        print(
            "\nWriting Valid/Corrected..."
        )


        (
            validated_df.write

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
                VALIDATED_COLLECTION
            )

            .option(
                "operationType",
                "replace"
            )

            .option(
                "idFieldList",
                "order_id"
            )

            .option(
                "upsertDocument",
                "true"
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
        # كتابة Quarantine
        # =================================================

        print(
            "\nWriting Quarantine..."
        )


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
        # Metrics
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


        validated_after = (
            validated_collection
            .estimated_document_count()
        )


        inserted_estimated = max(
            0,

            validated_after
            - validated_before
        )


        replaced_existing = max(
            0,

            (
                count_valid
                + count_corrected
            )

            - inserted_estimated
        )


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


        consistency_ok = (
            raw_count
            == classified
        )


        # =================================================
        # عدد كل نوع من الأخطاء
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
            quarantine_collection
            .aggregate(
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
        # حفظ النتائج
        # =================================================

        metrics = {

            "run_id":
                run_id,

            "source_file":
                source_file,

            "used_engine":
                "pyspark",

            "raw_count":
                raw_count,

            "count_valid":
                count_valid,

            "count_corrected":
                count_corrected,

            "count_quarantine":
                count_quarantine,

            "input_partitions":
                input_partitions,

            "count_inserted_estimated":
                inserted_estimated,

            "count_replaced_existing":
                replaced_existing,

            "count_error_cases":
                error_counts,

            "seconds_elapsed":
                round(
                    elapsed,
                    2
                ),

            "throughput":
                round(
                    throughput,
                    2
                ),

            "consistency_ok":
                consistency_ok
        }


        project_root = (
            Path(__file__)
            .resolve()
            .parents[1]
        )


        result_file = (
            project_root
            / "reports"
            / "spark_transform_results.json"
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
        # النتيجة النهائية
        # =================================================

        print(
            "\n===================================="
        )

        print(
            "PYSPARK TRANSFORM COMPLETED"
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
            "INSERTED ESTIMATED:",
            inserted_estimated
        )

        print(
            "REPLACED EXISTING:",
            replaced_existing
        )

        print(
            "CONSISTENCY:",
            consistency_ok
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

        mongo_client.close()


# =========================================================
# نقطة التشغيل
# =========================================================

if __name__ == "__main__":

    if len(
        sys.argv
    ) < 2:

        print(
            "Usage: "
            "spark-submit spark_transform.py RUN_ID"
        )

    else:

        run_transform(
            sys.argv[1]
        )