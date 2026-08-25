import sys

from spark_transform_fast import run_fast_transform


if __name__ == "__main__":

    if len(sys.argv) < 2:

        print(
            "Usage: "
            "spark-submit spark_transform_entry.py RUN_ID"
        )

        sys.exit(1)

    run_fast_transform(
        sys.argv[1]
    )
