# Hybrid Big Data ELT Pipeline

مشروع عملي لمعالجة بيانات الطلبات الضخمة باستخدام:

- Python Batch
- Apache PySpark
- MongoDB
- Data Quality
- ELT
- Quarantine
- Audit Trail
- Upsert
- Idempotency

---

## Project Architecture

CSV File
    |
    v
File Router
    |
    +----------------------+
    |                      |
    v                      v
Python Batch            PySpark
Small Files             Large Files
    |                      |
    +----------+-----------+
               |
               v
          orders_raw
               |
               v
       Data Quality Rules
               |
       +-------+-------+
       |               |
       v               v
orders_validated   quarantine_orders

---

## Dataset

Large dataset:

- File: orders_huge_mixed_quality.csv
- Size: 12650.32 MB
- Approximate Size: 12.35 GB
- Rows: 30,000,000

Small sample:

- File: orders_small_sample.csv
- Rows: 100,000
- Size: approximately 41.77 MB

Large datasets are not uploaded to GitHub.

---

## Automatic Engine Router

Threshold:

200 MB

Routing rule:

- File <= 200 MB -> Python Batch
- File > 200 MB -> PySpark

Actual test:

- 41.77 MB -> python_batch
- 12650.32 MB -> pyspark

---

## Technologies

- Python 3.13.5
- PySpark 4.2.0
- Java JDK 17
- MongoDB Community Server
- MongoDB Compass
- PyMongo
- Pytest

---

## MongoDB

Database:

midterm_pipeline

Main collections:

1. orders_raw
2. orders_validated
3. quarantine_orders

### orders_raw

Stores the original records before cleaning.

Metadata includes:

- run_id
- source_file
- ingested_at
- engine_used
- raw_record
- source_partition_id

### orders_validated

Stores valid and automatically corrected records.

Business Key:

order_id

Uses:

- Unique Index
- Upsert

### quarantine_orders

Stores records that cannot be safely corrected.

Includes:

- error_codes
- error_details
- raw_record
- clean_record
- corrections

---

## Python Batch

Used for small files.

Sample configuration:

- Rows: 100,000
- Batch Size: 5,000
- Number of Batches: 20

CSV is processed using streaming batches without loading the entire file into memory.

---

## PySpark

Used for large files.

The Spark path uses:

- SparkSession
- DataFrame API
- Fixed Schema
- MongoDB Spark Connector
- Parallel Processing
- Spark Partitions

Large Raw Load Results:

- Rows: 30,000,000
- Partitions: 99
- Elapsed Time: 382.23 seconds
- Throughput: 78,487.73 rows/sec

Main Run ID:

793867cd-f655-4e3b-8362-aa3bdcadbd5f

---

## Data Quality Rules

The project includes multiple automatic quality rules:

1. Trim extra spaces.
2. Convert Arabic digits.
3. Remove thousands separators.
4. Convert known number words.
5. Normalize currency to YER.
6. Normalize payment status.
7. Clean phone numbers.
8. Normalize date formats.
9. Repair supported email problems.
10. Validate items_json.
11. Recalculate total_amount when possible.
12. Detect missing order IDs.
13. Detect missing customer IDs.
14. Detect duplicate order IDs.
15. Quarantine unsafe records.

Automatic corrections are recorded in an Audit Trail.

---

## Final Large Dataset Results

RAW: 30,000,000

VALID: 22,406,175

CORRECTED: 5,090,034

QUARANTINE: 2,503,791

TOTAL CLASSIFIED: 30,000,000

CONSISTENCY: True

Consistency equation:

VALID + CORRECTED + QUARANTINE = RAW

22,406,175 + 5,090,034 + 2,503,791 = 30,000,000

---

## Transformation Performance

- Input Partitions: 214
- Elapsed Time: 7282.04 seconds
- Throughput: 4119.73 rows/sec

---

## Error Statistics

- CORRUPTED_ITEMS_JSON: 419,906
- MISSING_CUSTOMER_ID: 419,474
- INVALID_EMAIL: 418,709
- DUPLICATE_ORDER_ID: 417,584
- INVALID_DATE: 210,524
- UNKNOWN_STATUS: 210,194
- UNKNOWN_CURRENCY: 210,190
- TOTAL_CANNOT_BE_CALCULATED: 210,018
- EMPTY_ITEMS: 209,934
- MISSING_ORDER_ID: 209,392

---

## Upsert Test

A record was modified and processed again.

Result:

- INSERTED: 0
- UPDATED: 1

This proves that an existing business record can be updated without creating a duplicate.

---

## Idempotency Test

The same sample data was processed again.

Result:

- INSERTED: 0
- UPDATED: 0
- UNCHANGED: 92,350

This proves that processing the same input again does not create duplicate business records.

---

## Automated Tests

Run:

python -m pytest tests -v

Result:

6 passed

Tests include:

- Small File -> Python Batch
- Large File -> PySpark
- Valid Record
- Automatic Corrections
- Missing Order ID -> Quarantine
- Corrupted items_json -> Quarantine

---

## Installation

Install Python dependencies:

python -m pip install -r requirements.txt

Required software:

- Python
- Java JDK 17
- MongoDB Community Server
- MongoDB Compass
- PySpark
- MongoDB Spark Connector

---

## Running the Project

Main entry point:

python src\main.py "PATH_TO_FILE.csv"

The router automatically chooses:

python_batch

or:

pyspark

depending on the file size.

---

## Create Small Sample

Default:

python src\create_small_sample.py

Custom:

python src\create_small_sample.py INPUT.csv OUTPUT.csv 100000

---

## Project Structure

midterm-data-pipeline/
|
+-- config/
|   +-- settings.py
|
+-- data/
|
+-- reports/
|
+-- src/
|   +-- main.py
|   +-- file_router.py
|   +-- create_small_sample.py
|   +-- batch_loader.py
|   +-- spark_loader.py
|   +-- quality_rules.py
|   +-- elt_pipeline.py
|   +-- spark_transform.py
|   +-- spark_transform_fast.py
|   +-- rebuild_quarantine.py
|
+-- tests/
|   +-- test_router.py
|   +-- test_quality_rules.py
|
+-- requirements.txt
+-- README.md
+-- .gitignore

---

## Reports

Important result files are stored inside reports/:

- spark_raw_results.json
- spark_transform_fast_results.json
- rebuild_quarantine_results.json
- results.json

---

## Project Status

- File Router: PASS
- Python Batch: PASS
- PySpark: PASS
- MongoDB: PASS
- Raw Layer: PASS
- Data Quality: PASS
- Automatic Correction: PASS
- Quarantine: PASS
- Audit Trail: PASS
- Upsert: PASS
- Idempotency: PASS
- Consistency: PASS
- Automated Tests: 6 PASSED

---

## GitHub Note

Large CSV files, MongoDB database files, Spark temporary files and cache files must not be uploaded to GitHub.

The repository contains source code, configuration, tests, reports, documentation and screenshots.
