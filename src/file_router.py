import os
import sys

# الحد الفاصل بين الملف الصغير والكبير
SMALL_FILE_THRESHOLD_MB = 200


def choose_engine(file_path):
    # التأكد أن الملف موجود
    if not os.path.exists(file_path):
        print("Error: File not found")
        return

    # حساب حجم الملف بالميجابايت
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

    # اختيار المحرك حسب الحجم
    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = "File size is <= 200 MB"
    else:
        engine = "pyspark"
        reason = "File size is > 200 MB"

    # عرض النتيجة
    print(f"File: {file_path}")
    print(f"Size: {file_size_mb:.2f} MB")
    print(f"Selected Engine: {engine}")
    print(f"Reason: {reason}")


if __name__ == "__main__":
    choose_engine(sys.argv[1])