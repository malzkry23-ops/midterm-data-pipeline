import os
import re
import sys
import shutil
import subprocess
from pathlib import Path


# =========================================================
# مسار المشروع
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import (
    SMALL_FILE_THRESHOLD_MB,
    JAVA_HOME,
    SPARK_HOME,
    PYTHON_EXECUTABLE,
)


# =========================================================
# اختيار المحرك
# =========================================================

def choose_engine(file_path):

    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"File not found: {file_path}"
        )

    size_mb = (
        os.path.getsize(file_path)
        / (1024 * 1024)
    )

    if size_mb <= SMALL_FILE_THRESHOLD_MB:

        return (
            "python_batch",
            size_mb,
            f"File size <= {SMALL_FILE_THRESHOLD_MB} MB"
        )

    return (
        "pyspark",
        size_mb,
        f"File size > {SMALL_FILE_THRESHOLD_MB} MB"
    )


# =========================================================
# تجهيز بيئة Spark
# =========================================================

def build_spark_env():

    env = os.environ.copy()

    env["JAVA_HOME"] = JAVA_HOME
    env["SPARK_HOME"] = SPARK_HOME

    env["PYSPARK_PYTHON"] = (
        PYTHON_EXECUTABLE
    )

    env["PYSPARK_DRIVER_PYTHON"] = (
        PYTHON_EXECUTABLE
    )

    env["PATH"] = (
        str(Path(JAVA_HOME) / "bin")
        + os.pathsep
        + str(Path(SPARK_HOME) / "bin")
        + os.pathsep
        + str(Path(PYTHON_EXECUTABLE).parent)
        + os.pathsep
        + env.get("PATH", "")
    )

    return env


# =========================================================
# إيجاد spark-submit
# =========================================================

def get_spark_submit(env):

    candidate = (
        Path(SPARK_HOME)
        / "bin"
        / "spark-submit.cmd"
    )

    if candidate.exists():
        return str(candidate)

    current_path = os.environ.get(
        "PATH",
        ""
    )

    os.environ["PATH"] = env["PATH"]

    try:
        found = shutil.which(
            "spark-submit"
        )

    finally:
        os.environ["PATH"] = current_path

    if not found:

        raise RuntimeError(
            "spark-submit not found"
        )

    return found


# =========================================================
# تشغيل Loader وقراءة Run ID
# =========================================================

def run_loader_and_get_id(
    command,
    env=None
):

    run_id = None

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        bufsize=1
    )

    for line in process.stdout:

        print(
            line,
            end=""
        )

        match = re.search(
            r"Run ID:\s*"
            r"([0-9a-fA-F-]{36})",
            line
        )

        if match:
            run_id = match.group(1)

    return_code = process.wait()

    if return_code != 0:

        raise RuntimeError(
            "Raw loading failed"
        )

    if not run_id:

        raise RuntimeError(
            "Run ID was not detected"
        )

    return run_id


# =========================================================
# تشغيل أمر عادي
# =========================================================

def run_command(
    command,
    env=None
):

    result = subprocess.run(
        command,
        env=env
    )

    if result.returncode != 0:

        raise RuntimeError(
            "Pipeline stage failed"
        )


# =========================================================
# Pipeline كاملة
# =========================================================

def run_pipeline(file_path):

    file_path = str(
        Path(file_path).resolve()
    )

    engine, size_mb, reason = (
        choose_engine(file_path)
    )

    print(
        "\n===================================="
    )

    print(
        "HYBRID BIG DATA ELT PIPELINE"
    )

    print(
        "===================================="
    )

    print(
        "File:",
        file_path
    )

    print(
        "Size:",
        f"{size_mb:.2f} MB"
    )

    print(
        "Selected Engine:",
        engine
    )

    print(
        "Reason:",
        reason
    )

    print(
        "====================================\n"
    )


    # =====================================================
    # Python Batch
    # =====================================================

    if engine == "python_batch":

        print(
            "[1/2] RAW LOAD -> Python Batch\n"
        )

        loader = (
            PROJECT_ROOT
            / "src"
            / "batch_loader.py"
        )

        run_id = run_loader_and_get_id(
            [
                PYTHON_EXECUTABLE,
                str(loader),
                file_path
            ]
        )

        print(
            "\n[2/2] DATA QUALITY + ELT\n"
        )

        elt = (
            PROJECT_ROOT
            / "src"
            / "elt_pipeline.py"
        )

        run_command(
            [
                PYTHON_EXECUTABLE,
                str(elt),
                run_id
            ]
        )


    # =====================================================
    # PySpark
    # =====================================================

    else:

        spark_env = build_spark_env()

        spark_submit = (
            get_spark_submit(
                spark_env
            )
        )

        print(
            "[1/2] RAW LOAD -> PySpark\n"
        )

        loader = (
            PROJECT_ROOT
            / "src"
            / "spark_loader.py"
        )

        run_id = run_loader_and_get_id(
            [
                spark_submit,
                str(loader),
                file_path
            ],
            env=spark_env
        )

        print(
            "\n[2/2] DATA QUALITY + "
            "PYSPARK TRANSFORM\n"
        )

        transform = (
            PROJECT_ROOT
            / "src"
            / "spark_transform_entry.py"
        )

        run_command(
            [
                spark_submit,
                str(transform),
                run_id
            ],
            env=spark_env
        )


    # =====================================================
    # النهاية
    # =====================================================

    print(
        "\n===================================="
    )

    print(
        "PIPELINE COMPLETED SUCCESSFULLY"
    )

    print(
        "===================================="
    )

    print(
        "Run ID:",
        run_id
    )

    print(
        "Engine:",
        engine
    )

    print(
        "Check MongoDB collections:"
    )

    print(
        "- orders_raw"
    )

    print(
        "- orders_validated"
    )

    print(
        "- quarantine_orders"
    )


# =========================================================
# نقطة التشغيل
# =========================================================

if __name__ == "__main__":

    if len(sys.argv) < 2:

        print(
            "Usage:"
        )

        print(
            'python src\\main.py "FILE.csv"'
        )

        sys.exit(1)

    try:

        run_pipeline(
            sys.argv[1]
        )

    except Exception as error:

        print(
            "\nPIPELINE FAILED"
        )

        print(
            "ERROR:",
            error
        )

        sys.exit(1)
