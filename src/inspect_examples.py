import csv
import json

FILE = r"D:\midterm-data-pipeline\data\orders_small_sample.csv"

shown = {
    "missing_order_id": 0,
    "missing_customer_id": 0,
    "unknown_status": 0,
    "unknown_currency": 0,
    "bad_json": 0
}

with open(FILE, "r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)

    for row in reader:

        if not row["order_id"].strip() and shown["missing_order_id"] < 3:
            print("\nMISSING ORDER ID:")
            print(row)
            shown["missing_order_id"] += 1

        if not row["customer_id"].strip() and shown["missing_customer_id"] < 3:
            print("\nMISSING CUSTOMER ID:")
            print(row)
            shown["missing_customer_id"] += 1

        if row["status"].strip() == "حالة غير معروفة تمامًا" and shown["unknown_status"] < 3:
            print("\nUNKNOWN STATUS:")
            print(row)
            shown["unknown_status"] += 1

        if row["currency"].strip() == "عملة غير معروفة" and shown["unknown_currency"] < 3:
            print("\nUNKNOWN CURRENCY:")
            print(row)
            shown["unknown_currency"] += 1

        try:
            json.loads(row["items_json"])
        except:
            if shown["bad_json"] < 3:
                print("\nCORRUPTED JSON:")
                print(row)
                shown["bad_json"] += 1

        if all(v >= 3 for v in shown.values()):
            break