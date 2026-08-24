import os
import sys
import subprocess
from pathlib import Path


# =========================================================
# إضافة مسار المشروع حتى نستطيع قراءة config
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import SMALL_FILE_THRESHOLD_MB


# =========================================================
# اختيار المحرك حسب حجم الملف
# =========================================================

def choose_engine(file_path):

    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"File not found: {file_path}"
        )

    file_size_mb = (
        os.path.getsize(file_path)
        / (1024 * 1024)
    )

    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:

        engine = "python_batch"

        reason = (
            f"File size <= "
            f"{SMALL_FILE_THRESHOLD_MB} MB"
        )

    else:

        engine = "pyspark"

        reason = (
            f"File size > "
            f"{SMALL_FILE_THRESHOLD_MB} MB"
        )

    return (
        engine,
        file_size_mb,
        reason
    )


# =========================================================
# تشغيل المسار المناسب
# =========================================================

def run_pipeline(file_path):

    engine, size_mb, reason = (
        choose_engine(file_path)
    )

    print("\n====================================")
    print("HYBRID PIPELINE ROUTER")
    print("====================================")

    print(f"File: {file_path}")
    print(f"Size: {size_mb:.2f} MB")
    print(f"Selected Engine: {engine}")
    print(f"Reason: {reason}")

    print("====================================\n")


    # =====================================================
    # الملف الصغير → Python Batch
    # =====================================================

    if engine == "python_batch":

        script = (
            PROJECT_ROOT
            / "src"
            / "batch_loader.py"
        )

        command = [
            sys.executable,
            str(script),
            file_path
        ]


    # =====================================================
    # الملف الكبير → PySpark
    # =====================================================

    else:

        script = (
            PROJECT_ROOT
            / "src"
            / "spark_loader.py"
        )

        command = [
            "spark-submit",
            "--driver-memory",
            "4g",
            str(script),
            file_path
        ]


    print(
        "Starting selected engine...\n"
    )

    result = subprocess.run(
        command
    )

    if result.returncode == 0:

        print(
            "\nPipeline loading stage completed successfully."
        )

    else:

        print(
            "\nPipeline failed."
        )

        sys.exit(
            result.returncode
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
            "python src/main.py FILE_PATH"
        )

        sys.exit(1)

    run_pipeline(
        sys.argv[1]
    )
