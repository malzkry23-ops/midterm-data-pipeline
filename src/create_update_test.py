import csv
from pymongo import MongoClient


# إعدادات MongoDB
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "midterm_pipeline"
VALIDATED_COLLECTION = "orders_validated"

# ملف الاختبار الجديد
OUTPUT_FILE = r"D:\midterm-data-pipeline\data\update_test.csv"


# ترتيب أعمدة ملف الدكتور
FIELDS = [
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "delivery_cost",
    "payment_method",
    "payment_status",
    "payment_amount",
    "currency",
    "total_amount",
    "items_json"
]


client = MongoClient(MONGO_URI)

db = client[DATABASE_NAME]
validated = db[VALIDATED_COLLECTION]


try:

    # نأخذ سجل Valid حتى يكون اختبار التحديث واضحًا
    document = validated.find_one({
        "quality_status": "valid"
    })

    if not document:
        print("ERROR: No valid record found")
        raise SystemExit

    record = document["record"].copy()

    # حفظ القيمة القديمة
    old_name = record["customer_name"]

    # تعديل حقل غير جوهري مع بقاء نفس order_id
    record["customer_name"] = old_name + " - معدل"

    # إنشاء CSV يحتوي على السجل المعدل فقط
    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=FIELDS
        )

        writer.writeheader()

        writer.writerow({
            field: record.get(field, "")
            for field in FIELDS
        })

    print("UPDATE TEST FILE CREATED")
    print("Order ID:", record["order_id"])
    print("Old Name:", old_name)
    print("New Name:", record["customer_name"])
    print("File:", OUTPUT_FILE)

finally:

    client.close()