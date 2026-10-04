import json
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from pymongo import MongoClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import (
    MONGO_URI,
    DATABASE_NAME,
    VALIDATED_COLLECTION
)

from src.phase2.materialized_views import (
    refresh_all_incremental
)


JOB_LOG_COLLECTION = "scheduled_job_runs"

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "phase2"
    / "scheduled_jobs"
)

REPORT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# =========================================================
# تسجيل تنفيذ Job
# =========================================================

def log_job_run(
    job_name,
    started_at,
    finished_at,
    status,
    result=None,
    error=None
):

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]

        document = {
            "job_name": job_name,
            "started_at": started_at,
            "finished_at": finished_at,
            "status": status,
            "result": result,
            "error": error
        }

        db[JOB_LOG_COLLECTION].insert_one(
            document
        )

    finally:
        client.close()


# =========================================================
# الحصول على أحدث Run ID
# =========================================================

def get_latest_run_id():

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]

        collection = db[VALIDATED_COLLECTION]

        document = collection.find_one(
            {
                "last_run_id": {
                    "$exists": True,
                    "$nin": [None, ""]
                }
            },
            {
                "_id": 0,
                "last_run_id": 1
            },
            sort=[
                ("updated_at", -1)
            ]
        )

        if not document:
            return None

        return document.get(
            "last_run_id"
        )

    finally:
        client.close()


# =========================================================
# Job 1
# Refresh Materialized Views
# =========================================================

def refresh_materialized_views_job(
    run_id=None
):

    job_name = "refresh_materialized_views"

    started_at = datetime.now(
        timezone.utc
    )

    try:

        if run_id is None:
            run_id = get_latest_run_id()

        if not run_id:

            raise RuntimeError(
                "No last_run_id was found."
            )

        result = refresh_all_incremental(
            run_id
        )

        finished_at = datetime.now(
            timezone.utc
        )

        log_job_run(
            job_name=job_name,
            started_at=started_at,
            finished_at=finished_at,
            status="success",
            result=result
        )

        return {
            "job_name": job_name,
            "status": "success",
            "started_at":
                started_at.isoformat(),
            "finished_at":
                finished_at.isoformat(),
            "result": result
        }

    except Exception as exc:

        finished_at = datetime.now(
            timezone.utc
        )

        error_text = (
            f"{type(exc).__name__}: {exc}"
        )

        log_job_run(
            job_name=job_name,
            started_at=started_at,
            finished_at=finished_at,
            status="failed",
            error=error_text
        )

        return {
            "job_name": job_name,
            "status": "failed",
            "started_at":
                started_at.isoformat(),
            "finished_at":
                finished_at.isoformat(),
            "error": error_text
        }


# =========================================================
# Job 2
# Generate Summary Report
# =========================================================

def generate_summary_report_job():

    job_name = "generate_summary_report"

    started_at = datetime.now(
        timezone.utc
    )

    client = MongoClient(MONGO_URI)

    try:

        db = client[DATABASE_NAME]

        daily_rows = list(
            db["daily_sales_summary"].find(
                {},
                {"_id": 0}
            ).sort(
                "day",
                1
            )
        )

        city_rows = list(
            db["city_sales_summary"].find(
                {},
                {"_id": 0}
            ).sort(
                "total_sales",
                -1
            )
        )

        generated_at = datetime.now(
            timezone.utc
        )

        report = {
            "report_name":
                "scheduled_summary_report",

            "generated_at":
                generated_at.isoformat(),

            "daily_summary_count":
                len(daily_rows),

            "city_summary_count":
                len(city_rows),

            "daily_sales_summary":
                daily_rows,

            "city_sales_summary":
                city_rows
        }

        output_path = (
            REPORT_DIR
            / "latest_summary_report.json"
        )

        output_path.write_text(
            json.dumps(
                report,
                ensure_ascii=False,
                indent=2,
                default=str
            ),
            encoding="utf-8"
        )

        finished_at = datetime.now(
            timezone.utc
        )

        result = {
            "output_file":
                str(output_path),

            "daily_summary_count":
                len(daily_rows),

            "city_summary_count":
                len(city_rows)
        }

        log_job_run(
            job_name=job_name,
            started_at=started_at,
            finished_at=finished_at,
            status="success",
            result=result
        )

        return {
            "job_name": job_name,
            "status": "success",
            "started_at":
                started_at.isoformat(),
            "finished_at":
                finished_at.isoformat(),
            "result": result
        }

    except Exception as exc:

        finished_at = datetime.now(
            timezone.utc
        )

        error_text = (
            f"{type(exc).__name__}: {exc}"
        )

        log_job_run(
            job_name=job_name,
            started_at=started_at,
            finished_at=finished_at,
            status="failed",
            error=error_text
        )

        return {
            "job_name": job_name,
            "status": "failed",
            "started_at":
                started_at.isoformat(),
            "finished_at":
                finished_at.isoformat(),
            "error": error_text,
            "traceback":
                traceback.format_exc()
        }

    finally:
        client.close()


# =========================================================
# قائمة الـJobs
# =========================================================

def get_jobs():

    return [
        {
            "name":
                "refresh_materialized_views",
            "schedule":
                "every 30 minutes",
            "manual_run": True
        },
        {
            "name":
                "generate_summary_report",
            "schedule":
                "daily at 23:55",
            "manual_run": True
        }
    ]


# =========================================================
# تشغيل Job يدويًا
# =========================================================

def run_job(job_name):

    if job_name == "refresh_materialized_views":
        return refresh_materialized_views_job()

    if job_name == "generate_summary_report":
        return generate_summary_report_job()

    raise ValueError(
        f"Unknown job: {job_name}"
    )


# =========================================================
# Scheduler
# =========================================================

def start_scheduler():

    scheduler = BlockingScheduler(
        timezone="UTC"
    )

    scheduler.add_job(
        refresh_materialized_views_job,
        "interval",
        minutes=30,
        id="refresh_materialized_views",
        replace_existing=True
    )

    scheduler.add_job(
        generate_summary_report_job,
        "cron",
        hour=23,
        minute=55,
        id="generate_summary_report",
        replace_existing=True
    )

    print(
        "Scheduled Jobs started."
    )

    print(
        "- refresh_materialized_views: every 30 minutes"
    )

    print(
        "- generate_summary_report: daily at 23:55 UTC"
    )

    scheduler.start()


if __name__ == "__main__":

    if (
        len(sys.argv) >= 3
        and
        sys.argv[1] == "--run"
    ):

        result = run_job(
            sys.argv[2]
        )

        print(
            json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
                default=str
            )
        )

    else:

        start_scheduler()
