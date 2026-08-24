import csv
import sys
import time
import uuid
from datetime import datetime, timezone
from pymongo import MongoClient


# إعدادات MongoDB
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "midterm_pipeline"
COLLECTION_NAME = "orders_raw"

# عدد السجلات في كل دفعة
BATCH_SIZE = 5000


def load_csv_to_raw(file_path):
    # معرف فريد لهذا التشغيل
    run_id = str(uuid.uuid4())

    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    total_rows = 0
    batch_number = 0
    start_total = time.time()

    try:
        # قراءة الملف Streaming بدون تحميله كاملًا في الذاكرة
        with open(file_path, "r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            batch = []

            for row_number, row in enumerate(reader, start=1):

                # حفظ السجل الخام كما وصل
                document = {
                    "run_id": run_id,
                    "source_file": file_path,
                    "source_row_number": row_number,
                    "ingested_at": datetime.now(timezone.utc),
                    "engine_used": "python_batch",
                    "raw_record": row
                }

                batch.append(document)

                # عند اكتمال الدفعة يتم إدخالها إلى MongoDB
                if len(batch) >= BATCH_SIZE:
                    batch_number += 1
                    start_batch = time.time()

                    collection.insert_many(batch)

                    elapsed = time.time() - start_batch
                    total_rows += len(batch)
                    rate = len(batch) / elapsed if elapsed > 0 else 0

                    print(
                        f"Batch {batch_number} | "
                        f"Rows: {len(batch)} | "
                        f"Time: {elapsed:.2f}s | "
                        f"Rate: {rate:.2f} rows/sec"
                    )

                    batch = []

            # إدخال آخر دفعة إذا كانت أقل من BATCH_SIZE
            if batch:
                batch_number += 1
                start_batch = time.time()

                collection.insert_many(batch)

                elapsed = time.time() - start_batch
                total_rows += len(batch)

                print(
                    f"Batch {batch_number} | "
                    f"Rows: {len(batch)} | "
                    f"Time: {elapsed:.2f}s"
                )

        total_time = time.time() - start_total

        print("\nRAW LOAD COMPLETED")
        print(f"Run ID: {run_id}")
        print(f"Total Rows: {total_rows}")
        print(f"Total Batches: {batch_number}")
        print(f"Total Time: {total_time:.2f}s")

    except Exception as error:
        print(f"ERROR: {error}")

    finally:
        client.close()


if __name__ == "__main__":
    load_csv_to_raw(sys.argv[1])