import json
from pymongo import MongoClient

RUN_ID = "793867cd-f655-4e3b-8362-aa3bdcadbd5f"

client = MongoClient("mongodb://127.0.0.1:27017")
collection = client["midterm_pipeline"]["orders_raw"]

total = 0
bad = 0

cursor = collection.find(
    {"run_id": RUN_ID},
    {"raw_record.items_json": 1}
).limit(10000)

for document in cursor:
    total += 1
    value = document.get("raw_record", {}).get("items_json", "")

    try:
        json.loads(value)
    except Exception:
        bad += 1

print("TESTED:", total)
print("CORRUPTED JSON:", bad)
print("VALID JSON:", total - bad)

client.close()
