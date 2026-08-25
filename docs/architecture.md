# System Architecture

## Hybrid Big Data ELT Pipeline

هذا المشروع يستخدم معمارية هجينة لمعالجة ملفات CSV حسب حجم الملف.

## Processing Flow

CSV Input
    |
    v
File Router
    |
    +---------------------------+
    |                           |
    v                           v
Python Batch                 PySpark
<= 200 MB                    > 200 MB
    |                           |
    +-------------+-------------+
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

## File Router

يتم قياس حجم الملف قبل المعالجة:

- إذا كان الحجم <= 200 MB يتم استخدام Python Batch.
- إذا كان الحجم > 200 MB يتم استخدام PySpark.

## Raw Layer

يتم تحميل البيانات أولاً إلى:

orders_raw

بدون تنظيف مسبق.

يتم حفظ Metadata مثل:

- run_id
- source_file
- ingested_at
- engine_used
- source_partition_id
- raw_record

## Data Quality Layer

بعد Raw Load يتم تطبيق قواعد الجودة، ومنها:

- Trim spaces
- Arabic digits normalization
- Currency normalization
- Date normalization
- Phone normalization
- Email validation
- items_json validation
- total_amount recalculation
- Missing ID detection
- Duplicate order detection

## Classification

كل سجل يتم تصنيفه إلى:

- Valid
- Corrected
- Quarantined

## Validated Layer

السجلات الصحيحة والمصححة تذهب إلى:

orders_validated

ويتم استخدام:

order_id

كمفتاح Business Key مع:

- Unique Index
- Upsert
- Idempotency

## Quarantine Layer

السجلات التي لا يمكن تصحيحها بأمان تذهب إلى:

quarantine_orders

مع معلومات مثل:

- error_codes
- error_details
- corrections
- raw_record
- clean_record

## Large Dataset Execution

تم اختبار المشروع على ملف بحجم:

12.35 GB

وعدد:

30,000,000 rows

نتائج Raw Load:

- Partitions: 99
- Time: 382.23 seconds
- Throughput: 78,487.73 rows/sec

نتائج التصنيف النهائي:

- Valid: 22,406,175
- Corrected: 5,090,034
- Quarantine: 2,503,791
- Total: 30,000,000
- Consistency: True

## MongoDB

Database:

midterm_pipeline

Collections:

1. orders_raw
2. orders_validated
3. quarantine_orders

## Technologies

- Python
- PySpark
- Apache Spark
- MongoDB
- MongoDB Spark Connector
- PyMongo
- Pytest
