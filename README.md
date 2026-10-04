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

---

## Presentation GUI

A desktop GUI is included for practical demonstration.

Run:

python src\gui.py

The interface allows the examiner to:

- Select any CSV file.
- View file size.
- See the automatically selected engine.
- Start the full ELT pipeline.
- View RAW, VALID, CORRECTED and QUARANTINE counts.
- View INSERTED, UPDATED and UNCHANGED counts.
- View Consistency, Run ID and execution time.

Demo execution results are written to:

reports/latest_run.json

The official full-project results remain stored in:

reports/results.json

---

# Phase 2 - Final Requirements

This section documents the final-project additions implemented on top of the original Phase 1 pipeline.

## 1. MongoDB Queries

Five named and independently executable queries are implemented in:

`src/phase2/queries.py`

Queries:

1. `orders_by_city`
2. `orders_by_customer`
3. `orders_by_status_and_date`
4. `corrected_orders`
5. `orders_by_payment_status`

List available queries:

```powershell
python -c "from src.phase2.queries import get_query_names; print(get_query_names())"
```

---

## 2. MongoDB Indexes

The project includes multiple MongoDB indexes, including a compound index.

Main Phase 2 indexes:

- `idx_city`
- `idx_customer_id`
- `idx_status_order_date`
- `idx_last_run_id`
- `idx_order_date`

The compound index is `idx_status_order_date` on:

- `record.status`
- `record.order_date`

Indexes are implemented in `src/phase2/indexes.py`.

---

## 3. Explain Before and After Indexes

Three important queries were analyzed using MongoDB execution statistics.

### Query 1 - City

Before:
- Execution Time: 8 ms
- Keys Examined: 0
- Docs Examined: 114
- Stage: `COLLSCAN`

After:
- Execution Time: 22 ms
- Keys Examined: 10
- Docs Examined: 10
- Stage: `IXSCAN`

The execution plan changed from a collection scan to an index scan.

### Query 2 - Customer

Before:
- Execution Time: 55,458 ms
- Docs Examined: 27,497,147
- Stage: `COLLSCAN`

After:
- Execution Time: 1 ms
- Docs Examined: 1
- Stage: `IXSCAN`

### Query 3 - Status and Date

Before:
- Execution Time: 62,221 ms
- Docs Examined: 27,497,147
- Stage: `COLLSCAN`

After:
- Execution Time: 10 ms
- Docs Examined: 10
- Stage: `IXSCAN`

Explain reports are stored under `reports/phase2/`.

---

## 4. Aggregation Pipelines

Five named and independently executable aggregation pipelines are implemented in:

`src/phase2/aggregations.py`

Aggregations:

1. `sales_by_city`
2. `top_customers`
3. `orders_by_status`
4. `sales_by_month`
5. `payment_methods_summary`

List available aggregations:

```powershell
python -c "from src.phase2.aggregations import get_aggregation_names; print(get_aggregation_names())"
```

Aggregation reports are stored under:

`reports/phase2/aggregations/`

---

## 5. Materialized Views

Two MongoDB materialized views are implemented:

1. `daily_sales_summary`
2. `city_sales_summary`

Implementation:

`src/phase2/materialized_views.py`

Materialized view state is stored in:

`materialized_view_state`

---

## 6. Incremental Refresh

The materialized views support incremental refresh using `last_run_id`.

Only affected groups are recalculated instead of rebuilding the complete summaries every time.

Tested incremental refresh result:

- `daily_sales_summary` -> 1 affected group
- `city_sales_summary` -> 1 affected group

This proves that the refresh mechanism can update only changed data.


---

## 7. Scheduled Jobs

Two scheduled jobs are implemented using APScheduler.

Implementation:

`src/phase2/jobs.py`

### Job 1 - refresh_materialized_views

Purpose:

Incrementally refresh the materialized views.

Schedule:

Every 30 minutes.

### Job 2 - generate_summary_report

Purpose:

Generate the latest summary report.

Schedule:

Daily at 23:55 UTC.

### Job Logging

Every job execution is logged in MongoDB collection:

`scheduled_job_runs`

Each job log contains:

- job name
- start time
- finish time
- status
- result
- error

Start the scheduler:

```powershell
python src\phase2\jobs.py
```

A scheduled job can also be executed manually.

Example:

```powershell
python -c "from src.phase2.jobs import run_job; print(run_job('generate_summary_report'))"
```


---

## 8. FastAPI REST API

Phase 2 provides a unified REST API implemented using FastAPI.

Implementation:

`src/phase2/api.py`

Start the API:

```powershell
uvicorn src.phase2.api:app --host 127.0.0.1 --port 8000
```

Swagger documentation:

`http://127.0.0.1:8000/docs`

All API responses use JSON.

### Required API Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/health` | Check API and MongoDB connection |
| POST | `/ingest` | Run the existing Phase 1 ingestion pipeline |
| POST | `/indexes` | Create Phase 2 indexes |
| GET | `/queries` | List available named queries |
| GET | `/queries/{name}` | Execute a named query |
| GET | `/aggregations` | List available aggregations |
| GET | `/aggregations/{name}` | Execute a named aggregation |
| POST | `/refresh-mv` | Incrementally refresh materialized views |
| GET | `/jobs` | List scheduled jobs |
| POST | `/jobs/{name}/run` | Execute a scheduled job manually |

### Ingest Endpoint

The `/ingest` endpoint does not create a second ingestion implementation.

It reuses the original Phase 1 pipeline:

```text
Uploaded CSV
    |
    v
FastAPI
    |
    v
src/main.py
    |
    v
Automatic File Router
    |
    +--> Python Batch
    |
    +--> PySpark
```

This preserves the same routing, data-quality, ELT, MongoDB, quarantine and idempotency logic used in Phase 1.


---

## 9. Installation and Environment

Install Python dependencies:

```powershell
python -m pip install -r requirements.txt
```

Required software:

- Python
- Java JDK 17
- MongoDB Community Server
- PySpark

MongoDB must be running before starting the pipeline or API.

Default MongoDB URI:

`mongodb://127.0.0.1:27017`

Environment configuration templates:

- `.env.example`
- `example.env`

Supported environment variables:

- `MONGO_URI`
- `DATABASE_NAME`
- `SMALL_FILE_THRESHOLD_MB`
- `BATCH_SIZE`
- `DEFAULT_SAMPLE_ROWS`
- `JAVA_HOME`
- `SPARK_HOME`
- `PYTHON_EXECUTABLE`
- `SPARK_TEMP`

The source code does not depend on hardcoded machine-specific Python, Java or Spark paths.

On Windows PowerShell, Java 17 can be configured for the current terminal using:

```powershell
$env:JAVA_HOME="PATH_TO_JDK_17"
$env:Path="$env:JAVA_HOME\bin;$env:Path"
```

---

## 10. Portability Verification

The project was tested after removing machine-specific runtime paths.

Python Batch test result:

- RAW: 1
- VALID: 1
- INSERTED: 0
- UPDATED: 0
- UNCHANGED: 1
- CONSISTENCY: True

PySpark smoke test result:

- `SPARK_SMOKE_TEST_OK`
- Count: 10
- Spark Version: 4.2.0
- Java Version: 17

---

## 11. Final Project Status

### Phase 1

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
- Automated Tests: PASS

### Phase 2

- 5 Named Queries: PASS
- 3+ Indexes: PASS
- Compound Index: PASS
- Explain Before/After: PASS
- 5 Aggregations: PASS
- 2 Materialized Views: PASS
- Incremental Refresh: PASS
- 2 Scheduled Jobs: PASS
- Scheduled Job Logging: PASS
- FastAPI: PASS
- Swagger `/docs`: PASS
- Required API Endpoints: PASS
- Portable Configuration: PASS

---

## 12. Evaluation Notes

The evaluator can provide a different CSV file and run it through the same pipeline.

The project does not depend on a fixed CSV filename, fixed row count or developer-specific runtime path.

Run Phase 1 directly:

```powershell
python src\main.py "PATH_TO_FILE.csv"
```

Or use FastAPI `/ingest` through Swagger.

Large CSV files, MongoDB database files, Spark temporary files, private environment files and cache files are not committed to GitHub.
