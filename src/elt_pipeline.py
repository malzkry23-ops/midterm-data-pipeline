import sys
import time
import json
import os
from pathlib import Path
from collections import Counter
from pymongo import MongoClient

from quality_rules import clean_row


# =========================================================
# إعدادات MongoDB
# =========================================================

MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "midterm_pipeline"

RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "quarantine_orders"


# =========================================================
# وصف أسباب العزل
# =========================================================

ERROR_DETAILS = {
    "MISSING_ORDER_ID":
        "معرف الطلب مفقود ولا يمكن استنتاجه",

    "MISSING_CUSTOMER_ID":
        "معرف العميل مفقود",

    "INVALID_DATE":
        "التاريخ غير صالح أو غير قابل للتفسير",

    "CORRUPTED_ITEMS_JSON":
        "بيانات items_json تالفة",

    "EMPTY_ITEMS":
        "الطلب لا يحتوي على عناصر",

    "INVALID_EMAIL":
        "البريد الإلكتروني غير صالح ولا يمكن تصحيحه بأمان",

    "UNKNOWN_CURRENCY":
        "العملة غير معروفة",

    "UNKNOWN_STATUS":
        "حالة الطلب غير معروفة",

    "TOTAL_CANNOT_BE_CALCULATED":
        "لا يمكن حساب إجمالي الطلب",

    "ID_ORDER_DUPLICATE":
        "معرف الطلب مكرر داخل نفس التشغيل"
}


# =========================================================
# تشغيل ELT
# =========================================================

def process_run(run_id):

    client = MongoClient(MONGO_URI)

    db = client[DATABASE_NAME]

    raw = db[RAW_COLLECTION]

    # Index ?????? ????? ??? Run ID
    raw.create_index(
        [("run_id", 1)],
        name="run_id_1"
    )

    validated = db[VALIDATED_COLLECTION]
    quarantine = db[QUARANTINE_COLLECTION]

    # -----------------------------------------------------
    # Unique Index على order_id
    # -----------------------------------------------------

    validated.create_index(
        "order_id",
        unique=True
    )

    # منع تكرار سجل Quarantine عند إعادة نفس التشغيل


    # -----------------------------------------------------
    # عدادات
    # -----------------------------------------------------

    count_valid = 0
    count_corrected = 0
    count_quarantine = 0

    count_inserted = 0
    count_updated = 0
    count_unchanged = 0

    error_counts = Counter()

    seen_order_ids = set()

    start_time = time.time()

    try:

        # عدد سجلات Raw لهذا التشغيل
        raw_count = raw.count_documents({
            "run_id": run_id
        })

        if raw_count == 0:

            print("ERROR: No RAW records found for this run_id")
            return

        print("RAW RECORDS:", raw_count)
        print("PROCESSING STARTED...\n")

        first_raw = raw.find_one({
            "run_id": run_id
        })

        source_file = first_raw.get(
            "source_file",
            ""
        )

        engine_used = first_raw.get(
            "engine_used",
            ""
        )

        file_size_mb = 0

        if (
            source_file
            and os.path.exists(source_file)
        ):

            file_size_mb = (
                os.path.getsize(source_file)
                / (1024 * 1024)
            )

        # -------------------------------------------------
        # قراءة Raw من MongoDB
        # -------------------------------------------------

        cursor = raw.find({
            "run_id": run_id
        }).sort(
            "source_row_number",
            1
        )

        processed = 0

        for raw_doc in cursor:

            processed += 1

            original_record = raw_doc[
                "raw_record"
            ]

            # ---------------------------------------------
            # تطبيق قواعد الجودة
            # ---------------------------------------------

            result = clean_row(
                original_record
            )

            clean_record = result[
                "clean_record"
            ]

            quality_status = result[
                "quality_status"
            ]

            corrections = result[
                "corrections"
            ]

            error_codes = result[
                "error_codes"
            ]

            order_id = clean_record.get(
                "order_id",
                ""
            )

            # ---------------------------------------------
            # اكتشاف التكرار داخل نفس التشغيل
            # ---------------------------------------------

            if order_id:

                if order_id in seen_order_ids:

                    if (
                        "ID_ORDER_DUPLICATE"
                        not in error_codes
                    ):

                        error_codes.append(
                            "ID_ORDER_DUPLICATE"
                        )

                    quality_status = (
                        "quarantined"
                    )

                else:

                    seen_order_ids.add(
                        order_id
                    )

            # ---------------------------------------------
            # Quarantine
            # ---------------------------------------------

            if quality_status == "quarantined":

                count_quarantine += 1

                for code in error_codes:
                    error_counts[code] += 1

                details = [
                    ERROR_DETAILS.get(
                        code,
                        "خطأ جودة غير معروف"
                    )
                    for code in error_codes
                ]

                quarantine_document = {

                    "run_id":
                        run_id,

                    "source_file":
                        raw_doc.get(
                            "source_file"
                        ),

                    "source_row_number":
                        raw_doc.get(
                            "source_row_number"
                        ),

                    "engine_used":
                        raw_doc.get(
                            "engine_used"
                        ),

                    "quality_status":
                        "quarantined",

                    "error_codes":
                        error_codes,

                    "error_details":
                        details,

                    "corrections":
                        corrections,

                    "raw_record":
                        original_record,

                    "clean_record":
                        clean_record
                }

                quarantine.update_one(

                    {
                        "run_id":
                            run_id,

                        "source_row_number":
                            raw_doc[
                                "source_row_number"
                            ]
                    },

                    {
                        "$set":
                            quarantine_document
                    },

                    upsert=True
                )

            # ---------------------------------------------
            # Valid أو Corrected
            # ---------------------------------------------

            else:

                if quality_status == "valid":

                    count_valid += 1

                else:

                    count_corrected += 1

                validated_document = {

                    "order_id":
                        order_id,

                    "quality_status":
                        quality_status,

                    "record":
                        clean_record,

                    "corrections":
                        corrections
                }

                # -----------------------------------------
                # فحص السجل الموجود
                # -----------------------------------------

                existing = validated.find_one(
                    {
                        "order_id":
                            order_id
                    }
                )

                # إذا موجود ونفس البيانات
                if (
                    existing
                    and
                    existing.get("record")
                    == clean_record
                    and
                    existing.get(
                        "quality_status"
                    )
                    == quality_status
                    and
                    existing.get(
                        "corrections",
                        []
                    )
                    == corrections
                ):

                    count_unchanged += 1

                else:

                    # -------------------------------------
                    # Upsert
                    # -------------------------------------

                    result_upsert = (
                        validated.update_one(
                            {
                                "order_id":
                                    order_id
                            },

                            {
                                "$set":
                                    validated_document,

                                "$setOnInsert": {
                                    "first_run_id":
                                        run_id
                                }
                            },

                            upsert=True
                        )
                    )

                    if (
                        result_upsert.upserted_id
                        is not None
                    ):

                        count_inserted += 1

                    else:

                        count_updated += 1

            # ---------------------------------------------
            # Progress
            # ---------------------------------------------

            if processed % 5000 == 0:

                print(
                    f"Processed: "
                    f"{processed}/{raw_count}"
                )

        # -------------------------------------------------
        # الحسابات النهائية
        # -------------------------------------------------

        elapsed = (
            time.time()
            - start_time
        )

        throughput = (
            raw_count / elapsed
            if elapsed > 0
            else 0
        )

        classified_count = (
            count_valid
            + count_corrected
            + count_quarantine
        )

        # -------------------------------------------------
        # فحص الاتساق
        # -------------------------------------------------

        consistency_ok = (
            raw_count
            == classified_count
        )

        # -------------------------------------------------
        # Metrics
        # -------------------------------------------------

        metrics = {

            "run_id":
                run_id,

            "file_name":
                os.path.basename(
                    source_file
                ),

            "file_size_mb":
                round(
                    file_size_mb,
                    2
                ),

            "used_engine":
                engine_used,

            "read_rows":
                raw_count,

            "loaded_raw":
                raw_count,

            "count_valid":
                count_valid,

            "count_corrected":
                count_corrected,

            "count_quarantine":
                count_quarantine,

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

            "batch_size":
                5000,

            "count_error_cases":
                dict(
                    error_counts
                ),

            "count_inserted":
                count_inserted,

            "count_updated":
                count_updated,

            "count_unchanged":
                count_unchanged,

            "consistency_ok":
                consistency_ok
        }

        # -------------------------------------------------
        # حفظ results.json
        # -------------------------------------------------

        project_root = (
            Path(__file__)
            .resolve()
            .parents[1]
        )

        report_file = (
            project_root
            / "reports"
            / "latest_run.json"
        )

        with open(
            report_file,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                metrics,
                file,
                ensure_ascii=False,
                indent=4
            )

        # -------------------------------------------------
        # النتيجة
        # -------------------------------------------------

        print("\n==============================")
        print("ELT PROCESS COMPLETED")
        print("==============================")

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
            count_inserted
        )

        print(
            "UPDATED:",
            count_updated
        )

        print(
            "UNCHANGED:",
            count_unchanged
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
            "\nRESULTS FILE:"
        )

        print(
            report_file
        )

    finally:

        client.close()


# =========================================================
# تشغيل البرنامج
# =========================================================

if __name__ == "__main__":

    if len(sys.argv) < 2:

        print(
            "Usage: "
            "python elt_pipeline.py RUN_ID"
        )

    else:

        process_run(
            sys.argv[1]
        )