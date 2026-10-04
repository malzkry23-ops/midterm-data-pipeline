import json
import sys
from pathlib import Path

from bson import json_util
from pymongo import MongoClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    VALIDATED_COLLECTION
)


REPORT_DIR = PROJECT_ROOT / "reports" / "phase2" / "explain_after"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def collect_stages(obj, stages=None):

    if stages is None:
        stages = []

    if isinstance(obj, dict):

        if "stage" in obj:
            stages.append(obj["stage"])

        for value in obj.values():
            collect_stages(value, stages)

    elif isinstance(obj, list):

        for value in obj:
            collect_stages(value, stages)

    return stages


def run_explain(db, collection, name, command):

    print(f"\nRunning Explain AFTER: {name}")

    explain = db.command(
        "explain",
        command,
        verbosity="executionStats"
    )

    stats = explain.get(
        "executionStats",
        {}
    )

    stages = list(
        dict.fromkeys(
            collect_stages(
                explain.get(
                    "queryPlanner",
                    {}
                )
            )
        )
    )

    summary = {
        "query_name": name,
        "phase": "after_indexes",
        "nReturned": stats.get("nReturned"),
        "executionTimeMillis": stats.get(
            "executionTimeMillis"
        ),
        "totalKeysExamined": stats.get(
            "totalKeysExamined"
        ),
        "totalDocsExamined": stats.get(
            "totalDocsExamined"
        ),
        "winning_plan_stages": stages
    }

    raw_path = REPORT_DIR / f"{name}_raw.json"

    raw_path.write_text(
        json_util.dumps(
            explain,
            indent=2,
            ensure_ascii=False
        ),
        encoding="utf-8"
    )

    summary_path = (
        REPORT_DIR /
        f"{name}_summary.json"
    )

    summary_path.write_text(
        json.dumps(
            summary,
            indent=2,
            ensure_ascii=False
        ),
        encoding="utf-8"
    )

    print(
        json.dumps(
            summary,
            ensure_ascii=False,
            indent=2
        )
    )

    return summary


def main():

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]
        collection = db[VALIDATED_COLLECTION]

        summaries = []

        # Query 1 - City
        summaries.append(
            run_explain(
                db,
                collection,
                "orders_by_city",
                {
                    "find": collection.name,
                    "filter": {
                        "record.city": "صنعاء"
                    },
                    "limit": 10
                }
            )
        )

        # Query 2 - Customer
        summaries.append(
            run_explain(
                db,
                collection,
                "orders_by_customer",
                {
                    "find": collection.name,
                    "filter": {
                        "record.customer_id":
                        "عميل-51"
                    },
                    "limit": 10
                }
            )
        )

        # Query 3 - Status + Date
        summaries.append(
            run_explain(
                db,
                collection,
                "orders_by_status_and_date",
                {
                    "find": collection.name,
                    "filter": {
                        "record.status":
                        "قيد الشحن",
                        "record.order_date": {
                            "$gte":
                            "2025-01-01T00:00:00",
                            "$lte":
                            "2025-12-31T23:59:59"
                        }
                    },
                    "sort": {
                        "record.order_date": 1
                    },
                    "limit": 10,
                    "allowDiskUse": True
                }
            )
        )

        all_path = (
            REPORT_DIR /
            "explain_after_summary.json"
        )

        all_path.write_text(
            json.dumps(
                summaries,
                ensure_ascii=False,
                indent=2
            ),
            encoding="utf-8"
        )

        print(
            "\nExplain AFTER completed."
        )

        print(
            f"Reports saved in: {REPORT_DIR}"
        )

    finally:

        client.close()


if __name__ == "__main__":
    main()
