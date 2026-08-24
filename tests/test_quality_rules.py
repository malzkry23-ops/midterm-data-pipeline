import json

from src.quality_rules import clean_row


def make_valid_row():

    return {
        "order_id": "طلب-100001",
        "order_date": "2025-04-11T13:41:00",
        "status": "مؤكد",
        "customer_id": "عميل-1",
        "customer_name": "محمد علي",
        "customer_phone": "777123456",
        "customer_email": "user@example.com",
        "city": "صنعاء",
        "district": "التحرير",
        "delivery_type": "توصيل",
        "delivery_cost": "5000",
        "payment_method": "نقدي",
        "payment_status": "تم الدفع",
        "payment_amount": "135000",
        "currency": "YER",
        "total_amount": "135000",
        "items_json": json.dumps(
            [
                {
                    "sku": "SKU-1",
                    "qty": 1,
                    "unit_price": 130000,
                    "total": 130000
                }
            ],
            ensure_ascii=False
        )
    }


def test_valid_record():

    result = clean_row(
        make_valid_row()
    )

    assert result["quality_status"] == "valid"
    assert result["error_codes"] == []
    assert result["corrections"] == []


def test_automatic_corrections():

    row = make_valid_row()

    row["order_id"] = "  طلب-100001  "
    row["order_date"] = "2025/04/11 13:41:00"
    row["customer_phone"] = "+967 777 123 456"
    row["payment_status"] = "مدفوع"
    row["currency"] = "ريال يمني"
    row["total_amount"] = "135,000.00"

    result = clean_row(row)

    assert result["quality_status"] == "corrected"

    clean = result["clean_record"]

    assert clean["order_id"] == "طلب-100001"
    assert clean["order_date"] == "2025-04-11T13:41:00"
    assert clean["customer_phone"] == "777123456"
    assert clean["payment_status"] == "تم الدفع"
    assert clean["currency"] == "YER"
    assert clean["total_amount"] == "135000.00"

    assert len(result["corrections"]) >= 5


def test_missing_order_id_goes_to_quarantine():

    row = make_valid_row()

    row["order_id"] = ""

    result = clean_row(row)

    assert result["quality_status"] == "quarantined"

    assert (
        "MISSING_ORDER_ID"
        in result["error_codes"]
    )


def test_corrupted_items_json_goes_to_quarantine():

    row = make_valid_row()

    row["items_json"] = (
        '[{"sku":"SKU-1","qty":2'
    )

    result = clean_row(row)

    assert result["quality_status"] == "quarantined"

    assert (
        "CORRUPTED_ITEMS_JSON"
        in result["error_codes"]
    )
