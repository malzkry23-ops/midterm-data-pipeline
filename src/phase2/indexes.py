import json
import sys
import time
from pathlib import Path

from pymongo import ASCENDING, MongoClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    VALIDATED_COLLECTION
)


REPORT_DIR = PROJECT_ROOT / "reports" / "phase2" / "indexes"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


INDEX_DEFINITIONS = [
    {
        "name": "idx_city",
        "keys": [
            ("record.city", ASCENDING)
        ],
        "reason": "Speeds up orders_by_city query."
    },
    {
        "name": "idx_customer_id",
        "keys": [
            ("record.customer_id", ASCENDING)
        ],
        "reason": "Speeds up orders_by_customer query."
    },
    {
        "name": "idx_status_order_date",
        "keys": [
            ("record.status", ASCENDING),
            ("record.order_date", ASCENDING)
        ],
        "reason": (
            "Compound index for filtering by status "
            "and date range while supporting date sorting."
        )
    }
]


def create_phase2_indexes():

    client = MongoClient(MONGO_URI)

    results = []

    try:

        db = client[DATABASE_NAME]
        collection = db[VALIDATED_COLLECTION]

        for definition in INDEX_DEFINITIONS:

            print(
                f"\nCreating index: "
                f"{definition['name']}"
            )

            start = time.perf_counter()

            index_name = collection.create_index(
                definition["keys"],
                name=definition["name"]
            )

            elapsed = time.perf_counter() - start

            result = {
                "name": index_name,
                "keys": [
                    [field, direction]
                    for field, direction
                    in definition["keys"]
                ],
                "reason": definition["reason"],
                "creation_time_seconds": round(
                    elapsed,
                    2
                )
            }

            results.append(result)

            print(
                f"Created: {index_name}"
            )

            print(
                f"Time: {elapsed:.2f} seconds"
            )

        report = {
            "database": DATABASE_NAME,
            "collection": VALIDATED_COLLECTION,
            "indexes": results
        }

        report_path = (
            REPORT_DIR /
            "index_creation.json"
        )

        report_path.write_text(
            json.dumps(
                report,
                ensure_ascii=False,
                indent=2
            ),
            encoding="utf-8"
        )

        print("\nCurrent indexes:")

        for name, info in (
            collection.index_information().items()
        ):

            print(
                f"{name} => {info['key']}"
            )

        print(
            f"\nReport saved to: {report_path}"
        )

        return results

    finally:

        client.close()


if __name__ == "__main__":
    create_phase2_indexes()
