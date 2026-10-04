import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BEFORE_FILE = (
    PROJECT_ROOT
    / "reports"
    / "phase2"
    / "explain_before"
    / "explain_before_summary.json"
)

AFTER_FILE = (
    PROJECT_ROOT
    / "reports"
    / "phase2"
    / "explain_after"
    / "explain_after_summary.json"
)

OUTPUT_FILE = (
    PROJECT_ROOT
    / "reports"
    / "phase2"
    / "explain_comparison.json"
)


INDEX_INFO = {
    "orders_by_city": {
        "index": "idx_city",
        "reason": (
            "Index on record.city supports filtering "
            "orders by city without collection scanning."
        )
    },
    "orders_by_customer": {
        "index": "idx_customer_id",
        "reason": (
            "Index on record.customer_id provides fast "
            "lookup of orders for a specific customer."
        )
    },
    "orders_by_status_and_date": {
        "index": "idx_status_order_date",
        "reason": (
            "Compound index on record.status and "
            "record.order_date supports status filtering, "
            "date range filtering, and date sorting."
        )
    }
}


def main():

    before = json.loads(
        BEFORE_FILE.read_text(encoding="utf-8")
    )

    after = json.loads(
        AFTER_FILE.read_text(encoding="utf-8")
    )

    before_map = {
        item["query_name"]: item
        for item in before
    }

    after_map = {
        item["query_name"]: item
        for item in after
    }

    comparisons = []

    for query_name in before_map:

        b = before_map[query_name]
        a = after_map[query_name]

        before_time = b.get("executionTimeMillis") or 0
        after_time = a.get("executionTimeMillis") or 0

        before_docs = b.get("totalDocsExamined") or 0
        after_docs = a.get("totalDocsExamined") or 0

        comparison = {
            "query_name": query_name,
            "index_used": INDEX_INFO[query_name]["index"],
            "index_reason": INDEX_INFO[query_name]["reason"],

            "before": {
                "executionTimeMillis": before_time,
                "totalKeysExamined":
                    b.get("totalKeysExamined"),
                "totalDocsExamined":
                    before_docs,
                "plan":
                    b.get("winning_plan_stages")
            },

            "after": {
                "executionTimeMillis": after_time,
                "totalKeysExamined":
                    a.get("totalKeysExamined"),
                "totalDocsExamined":
                    after_docs,
                "plan":
                    a.get("winning_plan_stages")
            },

            "documents_examined_reduction": (
                before_docs - after_docs
            )
        }

        if before_time > 0 and after_time > 0:

            comparison["speedup_ratio"] = round(
                before_time / after_time,
                2
            )

        comparisons.append(comparison)

    report = {
        "title": "MongoDB Explain Before vs After Indexes",
        "comparison_count": len(comparisons),
        "comparisons": comparisons
    }

    OUTPUT_FILE.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2
        ),
        encoding="utf-8"
    )

    print(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2
        )
    )

    print(
        f"\nSaved to: {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    main()
