import json
import sys
import time
from pathlib import Path

from pymongo import MongoClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    VALIDATED_COLLECTION
)


REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "phase2"
    / "aggregations"
)

REPORT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


def sales_by_city():

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]
        collection = db[VALIDATED_COLLECTION]

        pipeline = [
            {
                "$match": {
                    "record.city": {
                        "$nin": [None, ""]
                    }
                }
            },
            {
                "$project": {
                    "city": "$record.city",
                    "total_amount": {
                        "$convert": {
                            "input":
                                "$record.total_amount",
                            "to": "double",
                            "onError": 0,
                            "onNull": 0
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$city",
                    "order_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum": "$total_amount"
                    },
                    "average_order_value": {
                        "$avg": "$total_amount"
                    }
                }
            },
            {
                "$sort": {
                    "total_sales": -1
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "city": "$_id",
                    "order_count": 1,
                    "total_sales": {
                        "$round": [
                            "$total_sales",
                            2
                        ]
                    },
                    "average_order_value": {
                        "$round": [
                            "$average_order_value",
                            2
                        ]
                    }
                }
            }
        ]

        print(
            "Running aggregation: sales_by_city"
        )

        start = time.perf_counter()

        results = list(
            collection.aggregate(
                pipeline,
                allowDiskUse=True
            )
        )

        elapsed = (
            time.perf_counter() - start
        )

        report = {
            "aggregation_name":
                "sales_by_city",
            "description":
                "Sales summary grouped by city.",
            "database":
                DATABASE_NAME,
            "collection":
                VALIDATED_COLLECTION,
            "execution_time_seconds":
                round(elapsed, 2),
            "returned_count":
                len(results),
            "results":
                results
        }

        output_path = (
            REPORT_DIR /
            "sales_by_city.json"
        )

        output_path.write_text(
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
            f"\nSaved to: {output_path}"
        )

        return report

    finally:

        client.close()


if __name__ == "__main__":
    sales_by_city()


# =========================================================
# Aggregation 2
# top_customers
# =========================================================

def top_customers(limit=20):

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]
        collection = db[VALIDATED_COLLECTION]

        pipeline = [
            {
                "$match": {
                    "record.customer_id": {
                        "$nin": [None, ""]
                    }
                }
            },
            {
                "$project": {
                    "customer_id":
                        "$record.customer_id",
                    "customer_name":
                        "$record.customer_name",
                    "total_amount": {
                        "$convert": {
                            "input":
                                "$record.total_amount",
                            "to": "double",
                            "onError": 0,
                            "onNull": 0
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "customer_id":
                            "$customer_id",
                        "customer_name":
                            "$customer_name"
                    },
                    "order_count": {
                        "$sum": 1
                    },
                    "total_spent": {
                        "$sum": "$total_amount"
                    },
                    "average_order_value": {
                        "$avg": "$total_amount"
                    }
                }
            },
            {
                "$sort": {
                    "total_spent": -1
                }
            },
            {
                "$limit": limit
            },
            {
                "$project": {
                    "_id": 0,
                    "customer_id":
                        "$_id.customer_id",
                    "customer_name":
                        "$_id.customer_name",
                    "order_count": 1,
                    "total_spent": {
                        "$round": [
                            "$total_spent",
                            2
                        ]
                    },
                    "average_order_value": {
                        "$round": [
                            "$average_order_value",
                            2
                        ]
                    }
                }
            }
        ]

        print(
            "Running aggregation: top_customers"
        )

        start = time.perf_counter()

        results = list(
            collection.aggregate(
                pipeline,
                allowDiskUse=True
            )
        )

        elapsed = (
            time.perf_counter() - start
        )

        report = {
            "aggregation_name":
                "top_customers",
            "description":
                "Top customers by total purchase value.",
            "database":
                DATABASE_NAME,
            "collection":
                VALIDATED_COLLECTION,
            "execution_time_seconds":
                round(elapsed, 2),
            "returned_count":
                len(results),
            "results":
                results
        }

        output_path = (
            REPORT_DIR /
            "top_customers.json"
        )

        output_path.write_text(
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
            f"\nSaved to: {output_path}"
        )

        return report

    finally:

        client.close()


# =========================================================
# Aggregation 3
# orders_by_status
# =========================================================

def orders_by_status():

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]
        collection = db[VALIDATED_COLLECTION]

        pipeline = [
            {
                "$match": {
                    "record.status": {
                        "$nin": [None, ""]
                    }
                }
            },
            {
                "$project": {
                    "status": "$record.status",
                    "total_amount": {
                        "$convert": {
                            "input": "$record.total_amount",
                            "to": "double",
                            "onError": 0,
                            "onNull": 0
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$status",
                    "order_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum": "$total_amount"
                    },
                    "average_order_value": {
                        "$avg": "$total_amount"
                    }
                }
            },
            {
                "$sort": {
                    "order_count": -1
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "status": "$_id",
                    "order_count": 1,
                    "total_sales": {
                        "$round": [
                            "$total_sales",
                            2
                        ]
                    },
                    "average_order_value": {
                        "$round": [
                            "$average_order_value",
                            2
                        ]
                    }
                }
            }
        ]

        print(
            "Running aggregation: orders_by_status"
        )

        start = time.perf_counter()

        results = list(
            collection.aggregate(
                pipeline,
                allowDiskUse=True
            )
        )

        elapsed = (
            time.perf_counter() - start
        )

        report = {
            "aggregation_name":
                "orders_by_status",
            "description":
                "Order distribution and sales by status.",
            "database":
                DATABASE_NAME,
            "collection":
                VALIDATED_COLLECTION,
            "execution_time_seconds":
                round(elapsed, 2),
            "returned_count":
                len(results),
            "results":
                results
        }

        output_path = (
            REPORT_DIR /
            "orders_by_status.json"
        )

        output_path.write_text(
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
            f"\nSaved to: {output_path}"
        )

        return report

    finally:

        client.close()


# =========================================================
# Aggregation 4
# sales_by_month
# =========================================================

def sales_by_month():

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]
        collection = db[VALIDATED_COLLECTION]

        pipeline = [
            {
                "$match": {
                    "record.order_date": {
                        "$nin": [None, ""]
                    }
                }
            },
            {
                "$project": {
                    "month": {
                        "$substrBytes": [
                            "$record.order_date",
                            0,
                            7
                        ]
                    },
                    "total_amount": {
                        "$convert": {
                            "input": "$record.total_amount",
                            "to": "double",
                            "onError": 0,
                            "onNull": 0
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$month",
                    "order_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum": "$total_amount"
                    },
                    "average_order_value": {
                        "$avg": "$total_amount"
                    }
                }
            },
            {
                "$sort": {
                    "_id": 1
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "month": "$_id",
                    "order_count": 1,
                    "total_sales": {
                        "$round": ["$total_sales", 2]
                    },
                    "average_order_value": {
                        "$round": [
                            "$average_order_value",
                            2
                        ]
                    }
                }
            }
        ]

        print("Running aggregation: sales_by_month")

        start = time.perf_counter()

        results = list(
            collection.aggregate(
                pipeline,
                allowDiskUse=True
            )
        )

        elapsed = time.perf_counter() - start

        report = {
            "aggregation_name": "sales_by_month",
            "description": "Monthly sales and order summary.",
            "database": DATABASE_NAME,
            "collection": VALIDATED_COLLECTION,
            "execution_time_seconds": round(elapsed, 2),
            "returned_count": len(results),
            "results": results
        }

        output_path = REPORT_DIR / "sales_by_month.json"

        output_path.write_text(
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

        print(f"`nSaved to: {output_path}")

        return report

    finally:
        client.close()


# =========================================================
# Aggregation 5
# payment_methods_summary
# =========================================================

def payment_methods_summary():

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]
        collection = db[VALIDATED_COLLECTION]

        pipeline = [
            {
                "$match": {
                    "record.payment_method": {
                        "$nin": [None, ""]
                    }
                }
            },
            {
                "$project": {
                    "payment_method":
                        "$record.payment_method",
                    "payment_status":
                        "$record.payment_status",
                    "total_amount": {
                        "$convert": {
                            "input":
                                "$record.total_amount",
                            "to": "double",
                            "onError": 0,
                            "onNull": 0
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$payment_method",

                    "order_count": {
                        "$sum": 1
                    },

                    "total_sales": {
                        "$sum": "$total_amount"
                    },

                    "average_order_value": {
                        "$avg": "$total_amount"
                    },

                    "paid_orders": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$eq": [
                                        "$payment_status",
                                        "تم الدفع"
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    },

                    "pending_payment_orders": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$eq": [
                                        "$payment_status",
                                        "بانتظار الدفع"
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    }
                }
            },
            {
                "$sort": {
                    "order_count": -1
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "payment_method": "$_id",
                    "order_count": 1,
                    "paid_orders": 1,
                    "pending_payment_orders": 1,

                    "total_sales": {
                        "$round": [
                            "$total_sales",
                            2
                        ]
                    },

                    "average_order_value": {
                        "$round": [
                            "$average_order_value",
                            2
                        ]
                    }
                }
            }
        ]

        print(
            "Running aggregation: payment_methods_summary"
        )

        start = time.perf_counter()

        results = list(
            collection.aggregate(
                pipeline,
                allowDiskUse=True
            )
        )

        elapsed = (
            time.perf_counter() - start
        )

        report = {
            "aggregation_name":
                "payment_methods_summary",

            "description":
                "Payment methods and payment status summary.",

            "database":
                DATABASE_NAME,

            "collection":
                VALIDATED_COLLECTION,

            "execution_time_seconds":
                round(elapsed, 2),

            "returned_count":
                len(results),

            "results":
                results
        }

        output_path = (
            REPORT_DIR /
            "payment_methods_summary.json"
        )

        output_path.write_text(
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
            f"\nSaved to: {output_path}"
        )

        return report

    finally:

        client.close()


# =========================================================
# Aggregation Registry
# =========================================================

def get_aggregation_names():

    return [
        "sales_by_city",
        "top_customers",
        "orders_by_status",
        "sales_by_month",
        "payment_methods_summary"
    ]


def run_aggregation(name, limit=20):

    if name == "sales_by_city":
        return sales_by_city()

    if name == "top_customers":
        return top_customers(limit)

    if name == "orders_by_status":
        return orders_by_status()

    if name == "sales_by_month":
        return sales_by_month()

    if name == "payment_methods_summary":
        return payment_methods_summary()

    raise ValueError(
        f"Unknown aggregation: {name}"
    )
