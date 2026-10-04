import json
import sys
from pathlib import Path

from pymongo import MongoClient


# =========================================================
# الوصول إلى إعدادات المشروع
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    VALIDATED_COLLECTION
)


# =========================================================
# Query 1
# orders_by_city
# =========================================================

def orders_by_city(city, limit=20):

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]

        collection = db[VALIDATED_COLLECTION]

        query = {
            "record.city": city
        }

        projection = {
            "_id": 0,
            "order_id": 1,
            "quality_status": 1,
            "record.city": 1,
            "record.customer_id": 1,
            "record.customer_name": 1,
            "record.order_date": 1,
            "record.total_amount": 1
        }

        documents = list(
            collection.find(
                query,
                projection
            ).limit(limit)
        )

        return {
            "query_name": "orders_by_city",
            "filter": query,
            "count_returned": len(documents),
            "results": documents
        }

    finally:

        client.close()


# =========================================================
# تشغيل يدوي للاختبار
# =========================================================

if __name__ == "__main__":

    city = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "صنعاء"
    )

    result = orders_by_city(
        city=city,
        limit=10
    )

    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2
        )
    )


# =========================================================
# Query 2
# orders_by_customer
# =========================================================

def orders_by_customer(customer_id, limit=20):

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]
        collection = db[VALIDATED_COLLECTION]

        query = {
            "record.customer_id": customer_id
        }

        projection = {
            "_id": 0,
            "order_id": 1,
            "quality_status": 1,
            "record.customer_id": 1,
            "record.customer_name": 1,
            "record.city": 1,
            "record.order_date": 1,
            "record.status": 1,
            "record.total_amount": 1
        }

        documents = list(
            collection.find(
                query,
                projection
            ).limit(limit)
        )

        return {
            "query_name": "orders_by_customer",
            "filter": query,
            "count_returned": len(documents),
            "results": documents
        }

    finally:
        client.close()


# =========================================================
# Query 3
# orders_by_status_and_date
# =========================================================

def orders_by_status_and_date(
    status,
    start_date,
    end_date,
    limit=20
):

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]
        collection = db[VALIDATED_COLLECTION]

        query = {
            "record.status": status,
            "record.order_date": {
                "$gte": start_date,
                "$lte": end_date
            }
        }

        projection = {
            "_id": 0,
            "order_id": 1,
            "quality_status": 1,
            "record.status": 1,
            "record.order_date": 1,
            "record.customer_id": 1,
            "record.customer_name": 1,
            "record.city": 1,
            "record.total_amount": 1
        }

        documents = list(
            collection.find(
                query,
                projection
            )
            .sort(
                "record.order_date",
                1
            )
            .limit(limit)
        )

        return {
            "query_name": "orders_by_status_and_date",
            "filter": query,
            "count_returned": len(documents),
            "results": documents
        }

    finally:
        client.close()


# =========================================================
# Query 4
# corrected_orders
# =========================================================

def corrected_orders(limit=20):

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]
        collection = db[VALIDATED_COLLECTION]

        query = {
            "quality_status": "corrected"
        }

        projection = {
            "_id": 0,
            "order_id": 1,
            "quality_status": 1,
            "corrections": 1,
            "record.order_date": 1,
            "record.customer_id": 1,
            "record.customer_name": 1,
            "record.city": 1,
            "record.total_amount": 1
        }

        documents = list(
            collection.find(
                query,
                projection
            ).limit(limit)
        )

        return {
            "query_name": "corrected_orders",
            "filter": query,
            "count_returned": len(documents),
            "results": documents
        }

    finally:
        client.close()


# =========================================================
# Query 5
# orders_by_payment_status
# =========================================================

def orders_by_payment_status(payment_status, limit=20):

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]
        collection = db[VALIDATED_COLLECTION]

        query = {
            "record.payment_status": payment_status
        }

        projection = {
            "_id": 0,
            "order_id": 1,
            "quality_status": 1,
            "record.payment_status": 1,
            "record.payment_method": 1,
            "record.payment_amount": 1,
            "record.currency": 1,
            "record.customer_id": 1,
            "record.customer_name": 1,
            "record.city": 1,
            "record.order_date": 1,
            "record.total_amount": 1
        }

        documents = list(
            collection.find(
                query,
                projection
            ).limit(limit)
        )

        return {
            "query_name": "orders_by_payment_status",
            "filter": query,
            "count_returned": len(documents),
            "results": documents
        }

    finally:
        client.close()


# =========================================================
# Query Registry
# =========================================================

def get_query_names():

    return [
        "orders_by_city",
        "orders_by_customer",
        "orders_by_status_and_date",
        "corrected_orders",
        "orders_by_payment_status"
    ]


def run_query(name, params):

    limit = int(
        params.get("limit", 20)
    )

    if name == "orders_by_city":

        return orders_by_city(
            city=params.get("city", "صنعاء"),
            limit=limit
        )

    if name == "orders_by_customer":

        return orders_by_customer(
            customer_id=params.get(
                "customer_id",
                "عميل-51"
            ),
            limit=limit
        )

    if name == "orders_by_status_and_date":

        return orders_by_status_and_date(
            status=params.get(
                "status",
                "قيد الشحن"
            ),
            start_date=params.get(
                "start_date",
                "2025-01-01T00:00:00"
            ),
            end_date=params.get(
                "end_date",
                "2025-12-31T23:59:59"
            ),
            limit=limit
        )

    if name == "corrected_orders":

        return corrected_orders(
            limit=limit
        )

    if name == "orders_by_payment_status":

        return orders_by_payment_status(
            payment_status=params.get(
                "payment_status",
                "تم الدفع"
            ),
            limit=limit
        )

    raise ValueError(
        f"Unknown query: {name}"
    )
