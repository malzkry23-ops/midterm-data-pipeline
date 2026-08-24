import csv
import json
import re
from collections import Counter

FILE = r"D:\midterm-data-pipeline\data\orders_small_sample.csv"

counts = Counter()
examples = {}

arabic_digits = "٠١٢٣٤٥٦٧٨٩"

def save_example(name, value):
    if name not in examples:
        examples[name] = value

with open(FILE, "r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)

    for row in reader:

        # 1- الأرقام العربية
        for field in ["customer_phone", "delivery_cost",
                      "payment_amount", "total_amount"]:
            value = row[field]
            if any(ch in arabic_digits for ch in value):
                counts["arabic_digits"] += 1
                save_example("arabic_digits", value)

        # 2- العملة
        if row["currency"].strip() == "ريال يمني":
            counts["currency_yer_text"] += 1
            save_example("currency_yer_text", row["currency"])

        # 3- فواصل الآلاف
        for field in ["delivery_cost", "payment_amount", "total_amount"]:
            value = row[field]
            if "," in value:
                counts["thousands_separator"] += 1
                save_example("thousands_separator", value)

        # 4- الهاتف غير القياسي
        phone = row["customer_phone"]
        if any(ch in phone for ch in [" ", "+", "-", "(", ")"]):
            counts["phone_format"] += 1
            save_example("phone_format", phone)

        # 5- البريد الإلكتروني
        email = row["customer_email"]
        if "@@" in email or ".." in email:
            counts["email_repeated_symbols"] += 1
            save_example("email_repeated_symbols", email)

        # 6- التاريخ بصيغة مختلفة
        date = row["order_date"]
        if "/" in date:
            counts["date_format"] += 1
            save_example("date_format", date)

        # 7- المسافات الزائدة
        for field, value in row.items():
            if value != value.strip():
                counts["extra_spaces"] += 1
                save_example("extra_spaces", value)
                break

        # 8- مرادفات حالة الدفع
        if row["payment_status"].strip() == "مدفوع":
            counts["payment_status_synonym"] += 1
            save_example(
                "payment_status_synonym",
                row["payment_status"]
            )

        # 9- إجمالي الطلب غير المطابق
        try:
            items = json.loads(row["items_json"])
            delivery = float(row["delivery_cost"])
            total = float(row["total_amount"])

            expected = sum(float(x["total"]) for x in items) + delivery

            if abs(expected - total) > 0.01:
                counts["total_mismatch"] += 1
                save_example(
                    "total_mismatch",
                    f"stored={total}, expected={expected}"
                )
        except:
            pass


print("\nQUALITY PATTERNS FOUND:")

for name, count in counts.items():
    print(f"{name}: {count}")

print("\nEXAMPLES:")

for name, value in examples.items():
    print(f"{name}: {value}")