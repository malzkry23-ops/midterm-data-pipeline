import csv
import json
from collections import Counter

FILE = r"D:\midterm-data-pipeline\data\orders_small_sample.csv"

missing = Counter()
status_values = Counter()
payment_status_values = Counter()
currency_values = Counter()

bad_json = 0
total = 0

with open(FILE, "r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)

    for row in reader:
        total += 1

        # الحقول المفقودة
        for field in ["order_id", "customer_id", "order_date",
                      "customer_phone", "customer_email",
                      "total_amount", "items_json"]:
            if not row[field].strip():
                missing[field] += 1

        status_values[row["status"].strip()] += 1
        payment_status_values[row["payment_status"].strip()] += 1
        currency_values[row["currency"].strip()] += 1

        # فحص JSON
        try:
            json.loads(row["items_json"])
        except:
            bad_json += 1


print("TOTAL ROWS:", total)

print("\nMISSING VALUES:")
for key, value in missing.items():
    print(key, "=", value)

print("\nSTATUS VALUES:")
print(status_values.most_common(20))

print("\nPAYMENT STATUS VALUES:")
print(payment_status_values.most_common(20))

print("\nCURRENCY VALUES:")
print(currency_values.most_common(20))

print("\nCORRUPTED ITEMS JSON:", bad_json)