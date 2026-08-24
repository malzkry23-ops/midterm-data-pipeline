import csv
from quality_rules import clean_row

FILE = r"D:\midterm-data-pipeline\data\orders_small_sample.csv"

with open(FILE, "r", encoding="utf-8-sig", newline="") as f:
    reader = csv.DictReader(f)

    for i, row in enumerate(reader, start=1):
        result = clean_row(row)

        if result["quality_status"] != "valid":
            print("\nROW:", i)
            print("STATUS:", result["quality_status"])
            print("CORRECTIONS:", result["corrections"])
            print("ERRORS:", result["error_codes"])

        if i >= 100:
            break