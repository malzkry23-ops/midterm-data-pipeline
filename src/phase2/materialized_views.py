import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient, ReplaceOne


PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    VALIDATED_COLLECTION
)


DAILY_MV = "daily_sales_summary"
CITY_MV = "city_sales_summary"
MV_STATE = "materialized_view_state"


def _amount_expression():
    return {
        "$convert": {
            "input": "$record.total_amount",
            "to": "double",
            "onError": 0,
            "onNull": 0
        }
    }


# =========================================================
# Materialized View 1
# daily_sales_summary
# =========================================================

def full_refresh_daily():

    client = MongoClient(MONGO_URI)

    try:
        db = client[DATABASE_NAME]
        source = db[VALIDATED_COLLECTION]
        target = db[DAILY_MV]

        print("Building Materialized View: daily_sales_summary")

        start = time.perf_counter()

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
                    "day": {
                        "$substrBytes": [
                            "$record.order_date",
                            0,
                            10
                        ]
                    },
                    "amount": _amount_expression()
                }
            },
            {
                "$group": {
                    "_id": "$day",
                    "order_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum": "$amount"
                    },
                    "average_order_value": {
                        "$avg": "$amount"
                    }
                }
            },
            {
                "$sort": {
                    "_id": 1
                }
            }
        ]

        results = list(
            source.aggregate(
                pipeline,
                allowDiskUse=True
            )
        )

        target.delete_many({})

        operations = []

        refreshed_at = datetime.now(timezone.utc)

        for row in results:

            day = row["_id"]

            document = {
                "day": day,
                "order_count": row["order_count"],
                "total_sales": round(
                    row["total_sales"],
                    2
                ),
                "average_order_value": round(
                    row["average_order_value"],
                    2
                ),
                "refreshed_at": refreshed_at
            }

            operations.append(
                ReplaceOne(
                    {"day": day},
                    document,
                    upsert=True
                )
            )

        if operations:
            target.bulk_write(
                operations,
                ordered=False
            )

        target.create_index(
            [("day", 1)],
            unique=True,
            name="idx_mv_day"
        )

        elapsed = time.perf_counter() - start

        db[MV_STATE].update_one(
            {"view_name": DAILY_MV},
            {
                "$set": {
                    "view_name": DAILY_MV,
                    "refresh_type": "full",
                    "last_refresh_at": refreshed_at,
                    "row_count": len(results),
                    "execution_time_seconds":
                        round(elapsed, 2),
                    "status": "success"
                }
            },
            upsert=True
        )

        print(
            f"daily_sales_summary rows: {len(results)}"
        )
        print(
            f"Execution time: {elapsed:.2f} seconds"
        )

        return {
            "view_name": DAILY_MV,
            "row_count": len(results),
            "execution_time_seconds":
                round(elapsed, 2)
        }

    finally:
        client.close()


# =========================================================
# Materialized View 2
# city_sales_summary
# =========================================================

def full_refresh_city():

    client = MongoClient(MONGO_URI)

    try:
        db = client[DATABASE_NAME]
        source = db[VALIDATED_COLLECTION]
        target = db[CITY_MV]

        print("Building Materialized View: city_sales_summary")

        start = time.perf_counter()

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
                    "amount": _amount_expression()
                }
            },
            {
                "$group": {
                    "_id": "$city",
                    "order_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum": "$amount"
                    },
                    "average_order_value": {
                        "$avg": "$amount"
                    }
                }
            },
            {
                "$sort": {
                    "total_sales": -1
                }
            }
        ]

        results = list(
            source.aggregate(
                pipeline,
                allowDiskUse=True
            )
        )

        target.delete_many({})

        operations = []

        refreshed_at = datetime.now(timezone.utc)

        for row in results:

            city = row["_id"]

            document = {
                "city": city,
                "order_count": row["order_count"],
                "total_sales": round(
                    row["total_sales"],
                    2
                ),
                "average_order_value": round(
                    row["average_order_value"],
                    2
                ),
                "refreshed_at": refreshed_at
            }

            operations.append(
                ReplaceOne(
                    {"city": city},
                    document,
                    upsert=True
                )
            )

        if operations:
            target.bulk_write(
                operations,
                ordered=False
            )

        target.create_index(
            [("city", 1)],
            unique=True,
            name="idx_mv_city"
        )

        elapsed = time.perf_counter() - start

        db[MV_STATE].update_one(
            {"view_name": CITY_MV},
            {
                "$set": {
                    "view_name": CITY_MV,
                    "refresh_type": "full",
                    "last_refresh_at": refreshed_at,
                    "row_count": len(results),
                    "execution_time_seconds":
                        round(elapsed, 2),
                    "status": "success"
                }
            },
            upsert=True
        )

        print(
            f"city_sales_summary rows: {len(results)}"
        )
        print(
            f"Execution time: {elapsed:.2f} seconds"
        )

        return {
            "view_name": CITY_MV,
            "row_count": len(results),
            "execution_time_seconds":
                round(elapsed, 2)
        }

    finally:
        client.close()


# =========================================================
# Incremental Refresh
# daily_sales_summary
# =========================================================

def incremental_refresh_daily(run_id):

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]
        source = db[VALIDATED_COLLECTION]
        target = db[DAILY_MV]

        print(
            f"Incremental refresh daily for run: {run_id}"
        )

        start = time.perf_counter()

        changed_dates = source.distinct(
            "record.order_date",
            {
                "last_run_id": run_id,
                "record.order_date": {
                    "$nin": [None, ""]
                }
            }
        )

        affected_days = sorted({
            value[:10]
            for value in changed_dates
            if isinstance(value, str)
            and len(value) >= 10
        })

        refreshed_at = datetime.now(timezone.utc)

        for day in affected_days:

            start_date = f"{day}T00:00:00"
            end_date = f"{day}T23:59:59"

            pipeline = [
                {
                    "$match": {
                        "record.order_date": {
                            "$gte": start_date,
                            "$lte": end_date
                        }
                    }
                },
                {
                    "$project": {
                        "amount": _amount_expression()
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "order_count": {
                            "$sum": 1
                        },
                        "total_sales": {
                            "$sum": "$amount"
                        },
                        "average_order_value": {
                            "$avg": "$amount"
                        }
                    }
                }
            ]

            rows = list(
                source.aggregate(
                    pipeline,
                    allowDiskUse=True
                )
            )

            if rows:

                row = rows[0]

                target.update_one(
                    {"day": day},
                    {
                        "$set": {
                            "day": day,
                            "order_count":
                                row["order_count"],
                            "total_sales":
                                round(
                                    row["total_sales"],
                                    2
                                ),
                            "average_order_value":
                                round(
                                    row[
                                        "average_order_value"
                                    ],
                                    2
                                ),
                            "refreshed_at":
                                refreshed_at
                        }
                    },
                    upsert=True
                )

        elapsed = time.perf_counter() - start

        db[MV_STATE].update_one(
            {"view_name": DAILY_MV},
            {
                "$set": {
                    "view_name": DAILY_MV,
                    "refresh_type": "incremental",
                    "last_run_id": run_id,
                    "last_refresh_at":
                        refreshed_at,
                    "affected_groups":
                        len(affected_days),
                    "execution_time_seconds":
                        round(elapsed, 2),
                    "status": "success"
                }
            },
            upsert=True
        )

        result = {
            "view_name": DAILY_MV,
            "refresh_type": "incremental",
            "run_id": run_id,
            "affected_days":
                affected_days,
            "affected_count":
                len(affected_days),
            "execution_time_seconds":
                round(elapsed, 2)
        }

        print(result)

        return result

    finally:

        client.close()


# =========================================================
# Incremental Refresh
# city_sales_summary
# =========================================================

def incremental_refresh_city(run_id):

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]
        source = db[VALIDATED_COLLECTION]
        target = db[CITY_MV]

        print(
            f"Incremental refresh city for run: {run_id}"
        )

        start = time.perf_counter()

        affected_cities = source.distinct(
            "record.city",
            {
                "last_run_id": run_id,
                "record.city": {
                    "$nin": [None, ""]
                }
            }
        )

        affected_cities = sorted(
            city
            for city in affected_cities
            if city
        )

        refreshed_at = datetime.now(timezone.utc)

        for city in affected_cities:

            pipeline = [
                {
                    "$match": {
                        "record.city": city
                    }
                },
                {
                    "$project": {
                        "amount": _amount_expression()
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "order_count": {
                            "$sum": 1
                        },
                        "total_sales": {
                            "$sum": "$amount"
                        },
                        "average_order_value": {
                            "$avg": "$amount"
                        }
                    }
                }
            ]

            rows = list(
                source.aggregate(
                    pipeline,
                    allowDiskUse=True
                )
            )

            if rows:

                row = rows[0]

                target.update_one(
                    {"city": city},
                    {
                        "$set": {
                            "city": city,
                            "order_count":
                                row["order_count"],
                            "total_sales":
                                round(
                                    row["total_sales"],
                                    2
                                ),
                            "average_order_value":
                                round(
                                    row[
                                        "average_order_value"
                                    ],
                                    2
                                ),
                            "refreshed_at":
                                refreshed_at
                        }
                    },
                    upsert=True
                )

        elapsed = time.perf_counter() - start

        db[MV_STATE].update_one(
            {"view_name": CITY_MV},
            {
                "$set": {
                    "view_name": CITY_MV,
                    "refresh_type": "incremental",
                    "last_run_id": run_id,
                    "last_refresh_at":
                        refreshed_at,
                    "affected_groups":
                        len(affected_cities),
                    "execution_time_seconds":
                        round(elapsed, 2),
                    "status": "success"
                }
            },
            upsert=True
        )

        result = {
            "view_name": CITY_MV,
            "refresh_type": "incremental",
            "run_id": run_id,
            "affected_cities":
                affected_cities,
            "affected_count":
                len(affected_cities),
            "execution_time_seconds":
                round(elapsed, 2)
        }

        print(result)

        return result

    finally:

        client.close()


# =========================================================
# Refresh both Materialized Views
# =========================================================

def refresh_all_incremental(run_id):

    daily = incremental_refresh_daily(
        run_id
    )

    city = incremental_refresh_city(
        run_id
    )

    return {
        "run_id": run_id,
        "daily_sales_summary": daily,
        "city_sales_summary": city
    }
