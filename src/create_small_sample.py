import csv
import sys
from pathlib import Path


# =========================================================
# القيم الافتراضية
# =========================================================

DEFAULT_INPUT = r"D:\orders_huge_mixed_quality.csv"

DEFAULT_OUTPUT = (
    r"D:\midterm-data-pipeline\data\orders_small_sample.csv"
)

DEFAULT_ROWS = 100000


# =========================================================
# إنشاء عينة صغيرة
# =========================================================

def create_sample(
    input_file,
    output_file,
    rows_to_copy
):

    output_path = Path(
        output_file
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    copied_rows = 0

    with open(
        input_file,
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as src:

        reader = csv.reader(
            src
        )

        with open(
            output_file,
            "w",
            encoding="utf-8-sig",
            newline=""
        ) as dst:

            writer = csv.writer(
                dst
            )

            # كتابة Header
            header = next(
                reader
            )

            writer.writerow(
                header
            )

            # كتابة العدد المطلوب من السجلات
            for row in reader:

                writer.writerow(
                    row
                )

                copied_rows += 1

                if copied_rows >= rows_to_copy:
                    break


    print(
        "\n===================================="
    )

    print(
        "SMALL SAMPLE CREATED"
    )

    print(
        "===================================="
    )

    print(
        "Input:",
        input_file
    )

    print(
        "Output:",
        output_file
    )

    print(
        "Rows:",
        copied_rows
    )


# =========================================================
# نقطة التشغيل
#
# الاستخدام:
# python src/create_small_sample.py
#
# أو:
# python src/create_small_sample.py INPUT OUTPUT ROWS
# =========================================================

if __name__ == "__main__":

    input_file = (
        sys.argv[1]
        if len(sys.argv) > 1
        else DEFAULT_INPUT
    )

    output_file = (
        sys.argv[2]
        if len(sys.argv) > 2
        else DEFAULT_OUTPUT
    )

    rows_to_copy = (
        int(sys.argv[3])
        if len(sys.argv) > 3
        else DEFAULT_ROWS
    )

    create_sample(
        input_file,
        output_file,
        rows_to_copy
    )
