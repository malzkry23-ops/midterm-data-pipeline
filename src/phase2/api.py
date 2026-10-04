import sys
import re
import subprocess
import uuid
from pathlib import Path

from fastapi import (
    FastAPI,
    HTTPException,
    Request,
    UploadFile,
    File
)
from pymongo import MongoClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import (
    MONGO_URI,
    DATABASE_NAME
)

from src.phase2.queries import (
    get_query_names,
    run_query
)

from src.phase2.indexes import (
    create_phase2_indexes
)

from src.phase2.aggregations import (
    get_aggregation_names,
    run_aggregation
)

from src.phase2.materialized_views import (
    refresh_all_incremental
)

from src.phase2.jobs import (
    get_latest_run_id,
    get_jobs,
    run_job
)


app = FastAPI(
    title="Hybrid Big Data ELT Pipeline API",
    description=(
        "Unified API for Big Data Phase 2 "
        "project execution and testing."
    ),
    version="2.0.0"
)


# =========================================================
# Health Check
# GET /health
# =========================================================

@app.get("/health")
def health():

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=3000
    )

    try:

        client.admin.command("ping")

        return {
            "status": "ok",
            "api": "running",
            "mongodb": "connected",
            "database": DATABASE_NAME
        }

    except Exception as exc:

        return {
            "status": "error",
            "api": "running",
            "mongodb": "disconnected",
            "error": str(exc)
        }

    finally:
        client.close()



# =========================================================
# Queries
# GET /queries
# =========================================================

@app.get("/queries")
def list_queries():

    return {
        "count": len(get_query_names()),
        "queries": get_query_names()
    }


# =========================================================
# Run Query
# GET /queries/{name}
# =========================================================

@app.get("/queries/{name}")
def execute_query(
    name: str,
    request: Request
):

    if name not in get_query_names():

        raise HTTPException(
            status_code=404,
            detail=f"Unknown query: {name}"
        )

    params = dict(
        request.query_params
    )

    try:

        return run_query(
            name,
            params
        )

    except Exception as exc:

        raise HTTPException(
            status_code=400,
            detail=str(exc)
        )



# =========================================================
# Indexes
# POST /indexes
# =========================================================

@app.post("/indexes")
def create_indexes():

    try:

        results = create_phase2_indexes()

        return {
            "status": "success",
            "index_count": len(results),
            "indexes": results
        }

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )



# =========================================================
# Aggregations
# GET /aggregations
# =========================================================

@app.get("/aggregations")
def list_aggregations():

    names = get_aggregation_names()

    return {
        "count": len(names),
        "aggregations": names
    }


# =========================================================
# Run Aggregation
# GET /aggregations/{name}
# =========================================================

@app.get("/aggregations/{name}")
def execute_aggregation(
    name: str,
    limit: int = 20
):

    if name not in get_aggregation_names():

        raise HTTPException(
            status_code=404,
            detail=f"Unknown aggregation: {name}"
        )

    try:

        return run_aggregation(
            name,
            limit
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )



# =========================================================
# Materialized Views
# POST /refresh-mv
# =========================================================

@app.post("/refresh-mv")
def refresh_materialized_views(
    run_id: str | None = None
):

    try:

        selected_run_id = (
            run_id
            if run_id
            else get_latest_run_id()
        )

        if not selected_run_id:

            raise HTTPException(
                status_code=404,
                detail="No run_id was found."
            )

        result = refresh_all_incremental(
            selected_run_id
        )

        return {
            "status": "success",
            "refresh_type": "incremental",
            "run_id": selected_run_id,
            "result": result
        }

    except HTTPException:
        raise

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )



# =========================================================
# Scheduled Jobs
# GET /jobs
# =========================================================

@app.get("/jobs")
def list_jobs():

    jobs = get_jobs()

    return {
        "count": len(jobs),
        "jobs": jobs
    }


# =========================================================
# Run Scheduled Job Manually
# POST /jobs/{name}/run
# =========================================================

@app.post("/jobs/{name}/run")
def execute_job(name: str):

    valid_names = [
        job["name"]
        for job in get_jobs()
    ]

    if name not in valid_names:

        raise HTTPException(
            status_code=404,
            detail=f"Unknown job: {name}"
        )

    try:

        result = run_job(name)

        return result

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )



# =========================================================
# Ingest
# POST /ingest
# Uses the same Phase 1 pipeline
# =========================================================

@app.post("/ingest")
async def ingest_file(
    file: UploadFile = File(...)
):

    original_name = (
        Path(file.filename or "uploaded.csv").name
    )

    if not original_name.lower().endswith(".csv"):

        raise HTTPException(
            status_code=400,
            detail="Only CSV files are supported."
        )

    incoming_dir = PROJECT_ROOT / "incoming"

    incoming_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    unique_name = (
        f"api_{uuid.uuid4().hex}_"
        f"{original_name}"
    )

    saved_path = (
        incoming_dir /
        unique_name
    )

    try:

        # ??? ????? ???? Streaming
        # ???? ????? ????? ?????? ?? ???????
        with saved_path.open("wb") as output:

            while True:

                chunk = await file.read(
                    1024 * 1024
                )

                if not chunk:
                    break

                output.write(chunk)

        await file.close()

        command = [
            sys.executable,
            str(
                PROJECT_ROOT /
                "src" /
                "main.py"
            ),
            str(saved_path)
        ]

        process = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace"
        )

        stdout = process.stdout or ""
        stderr = process.stderr or ""

        if process.returncode != 0:

            raise HTTPException(
                status_code=500,
                detail={
                    "message":
                        "Pipeline execution failed.",
                    "return_code":
                        process.returncode,
                    "stderr":
                        stderr[-4000:],
                    "stdout":
                        stdout[-4000:]
                }
            )

        run_matches = re.findall(
            r"Run ID:\s*([A-Za-z0-9\-]+)",
            stdout
        )

        engine_matches = re.findall(
            r"(?:Selected Engine|Engine):\s*([A-Za-z0-9_]+)",
            stdout
        )

        run_id = (
            run_matches[-1]
            if run_matches
            else None
        )

        engine = (
            engine_matches[-1]
            if engine_matches
            else None
        )

        return {
            "status": "success",
            "filename": original_name,
            "saved_as": unique_name,
            "run_id": run_id,
            "engine": engine,
            "pipeline": "existing_phase1_pipeline",
            "message":
                "File processed successfully.",
            "output_tail":
                stdout[-3000:]
        }

    finally:

        try:
            await file.close()
        except Exception:
            pass
