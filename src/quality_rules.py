import re
import json
from datetime import datetime


# =========================================================
# تحويل الأرقام العربية إلى أرقام إنجليزية
# مثال: ٥٠٠٠٫٠  →  5000.0
# =========================================================
ARABIC_DIGITS = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩٫",
    "0123456789."
)


# =========================================================
# الأرقام المكتوبة بالكلمات والتي يمكن تحويلها بدون تخمين
# =========================================================
KNOWN_NUMBER_WORDS = {
    "ألفان": "2000",
    "خمسة آلاف": "5000"
}


# =========================================================
# إضافة أي تعديل إلى Audit Trail
# =========================================================
def add_correction(corrections, field, old, new, rule):

    if old != new:

        corrections.append({
            "field": field,
            "original_value": old,
            "corrected_value": new,
            "rule_code": rule
        })


# =========================================================
# الدالة الرئيسية لتنظيف وتصنيف السجل
# =========================================================
def clean_row(row):

    # نسخة من السجل حتى لا نعدل البيانات الخام Raw
    data = row.copy()

    corrections = []
    errors = []

    # =====================================================
    # 1- إزالة المسافات الزائدة
    # =====================================================
    for field, value in data.items():

        if isinstance(value, str):

            new_value = value.strip()

            add_correction(
                corrections,
                field,
                value,
                new_value,
                "TRIM_SPACES"
            )

            data[field] = new_value


    # =====================================================
    # فحص المعرفات الأساسية
    # =====================================================

    if not data["order_id"]:
        errors.append("MISSING_ORDER_ID")

    if not data["customer_id"]:
        errors.append("MISSING_CUSTOMER_ID")


    # =====================================================
    # 2- تحويل الأرقام المكتوبة بالكلمات
    # مثال: ألفان → 2000
    # =====================================================

    for field in [
        "delivery_cost",
        "payment_amount",
        "total_amount"
    ]:

        old = data[field]

        if old in KNOWN_NUMBER_WORDS:

            new = KNOWN_NUMBER_WORDS[old]

            add_correction(
                corrections,
                field,
                old,
                new,
                "KNOWN_NUMBER_WORD_TO_NUMBER"
            )

            data[field] = new


    # =====================================================
    # 3- تحويل الأرقام العربية إلى إنجليزية
    # مثال: ٥٠٠٠٫٠ → 5000.0
    # =====================================================

    numeric_fields = [
        "customer_phone",
        "delivery_cost",
        "payment_amount",
        "total_amount"
    ]

    for field in numeric_fields:

        old = data[field]

        new = old.translate(
            ARABIC_DIGITS
        )

        add_correction(
            corrections,
            field,
            old,
            new,
            "ARABIC_DIGITS_TO_LATIN"
        )

        data[field] = new


    # =====================================================
    # 4- إزالة فواصل الآلاف
    # مثال: 135,000.00 → 135000.00
    # =====================================================

    for field in [
        "delivery_cost",
        "payment_amount",
        "total_amount"
    ]:

        old = data[field]

        new = old.replace(",", "")

        add_correction(
            corrections,
            field,
            old,
            new,
            "REMOVE_THOUSANDS_SEPARATOR"
        )

        data[field] = new


    # =====================================================
    # 5- توحيد العملة
    # ريال يمني → YER
    # =====================================================

    if data["currency"] == "ريال يمني":

        old = data["currency"]

        data["currency"] = "YER"

        add_correction(
            corrections,
            "currency",
            old,
            "YER",
            "NORMALIZE_CURRENCY_YER"
        )

    elif data["currency"] != "YER":

        errors.append(
            "UNKNOWN_CURRENCY"
        )


    # =====================================================
    # 6- توحيد حالة الدفع
    # مدفوع → تم الدفع
    # =====================================================

    if data["payment_status"] == "مدفوع":

        old = data["payment_status"]

        data["payment_status"] = "تم الدفع"

        add_correction(
            corrections,
            "payment_status",
            old,
            "تم الدفع",
            "NORMALIZE_PAYMENT_STATUS"
        )


    # =====================================================
    # 7- تنظيف رقم الهاتف
    # =====================================================

    phone = data["customer_phone"]

    # حذف المسافات والشرطات والأقواس
    new_phone = re.sub(
        r"[\s\-\(\)]",
        "",
        phone
    )

    # +967777123456 → 777123456
    if new_phone.startswith("+967"):

        new_phone = new_phone[4:]

    # 967777123456 → 777123456
    elif (
        new_phone.startswith("967")
        and len(new_phone) > 9
    ):

        new_phone = new_phone[3:]

    add_correction(
        corrections,
        "customer_phone",
        phone,
        new_phone,
        "NORMALIZE_PHONE"
    )

    data["customer_phone"] = new_phone


    # =====================================================
    # 8- توحيد التاريخ
    # =====================================================

    old_date = data["order_date"]

    # الصيغ التي وجدناها في بيانات الدكتور
    date_formats = [

        # 2025-01-31T13:54:00
        "%Y-%m-%dT%H:%M:%S",

        # 2025/04/11 13:41:00
        "%Y/%m/%d %H:%M:%S",

        # 17-01-2025 04:50:00
        "%d-%m-%Y %H:%M:%S"
    ]

    parsed_date = None

    for date_format in date_formats:

        try:

            parsed_date = datetime.strptime(
                old_date,
                date_format
            )

            break

        except ValueError:

            pass


    # إذا التاريخ صالح
    if parsed_date:

        new_date = parsed_date.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        add_correction(
            corrections,
            "order_date",
            old_date,
            new_date,
            "NORMALIZE_DATE"
        )

        data["order_date"] = new_date

    else:

        # التاريخ غير معروف أو مستحيل
        errors.append(
            "INVALID_DATE"
        )


    # =====================================================
    # 9- إصلاح البريد الإلكتروني
    # =====================================================

    email = data["customer_email"]

    if email:

        # user@@example.com
        # يصبح
        # user@example.com
        new_email = re.sub(
            r"@+",
            "@",
            email
        )

        # example..com
        # يصبح
        # example.com
        new_email = re.sub(
            r"\.+",
            ".",
            new_email
        )

        email_pattern = (
            r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
        )

        # إذا أصبح البريد صالحًا
        if re.match(
            email_pattern,
            new_email
        ):

            add_correction(
                corrections,
                "customer_email",
                email,
                new_email,
                "FIX_EMAIL_REPEATED_SYMBOLS"
            )

            data["customer_email"] = new_email

        else:

            # مثال @@
            # لا يمكن استنتاج البريد الحقيقي
            errors.append(
                "INVALID_EMAIL"
            )

    else:

        errors.append(
            "INVALID_EMAIL"
        )


    # =====================================================
    # 10- فحص items_json
    # =====================================================

    items = None

    try:

        items = json.loads(
            data["items_json"]
        )

        if not items:

            errors.append(
                "EMPTY_ITEMS"
            )

    except Exception:

        errors.append(
            "CORRUPTED_ITEMS_JSON"
        )


    # =====================================================
    # 11- إعادة حساب إجمالي الطلب
    # مجموع العناصر + تكلفة التوصيل
    # =====================================================

    if items:

        try:

            delivery = float(
                data["delivery_cost"]
            )

            items_total = sum(
                float(item["total"])
                for item in items
            )

            expected_total = (
                items_total
                + delivery
            )

            # محاولة قراءة الإجمالي الحالي
            try:

                current_total = float(
                    data["total_amount"]
                )

            except ValueError:

                current_total = None


            # إذا الإجمالي مفقود أو غير صحيح
            if (
                current_total is None
                or abs(
                    current_total
                    - expected_total
                ) > 0.01
            ):

                old = data["total_amount"]

                new = str(
                    expected_total
                )

                data["total_amount"] = new

                add_correction(
                    corrections,
                    "total_amount",
                    old,
                    new,
                    "RECALCULATE_TOTAL"
                )

        except Exception:

            errors.append(
                "TOTAL_CANNOT_BE_CALCULATED"
            )


    # =====================================================
    # 12- التحقق من حالة الطلب
    # =====================================================

    valid_statuses = [
        "مرتجع",
        "مؤكد",
        "ملغي",
        "قيد الشحن",
        "قيد الانتظار",
        "تم التسليم"
    ]

    if data["status"] not in valid_statuses:

        errors.append(
            "UNKNOWN_STATUS"
        )


    # =====================================================
    # إزالة الأخطاء المكررة
    # =====================================================

    errors = list(
        dict.fromkeys(errors)
    )


    # =====================================================
    # التصنيف النهائي
    # =====================================================

    if errors:

        quality_status = (
            "quarantined"
        )

    elif corrections:

        quality_status = (
            "corrected"
        )

    else:

        quality_status = (
            "valid"
        )


    # =====================================================
    # النتيجة النهائية
    # =====================================================

    return {

        "clean_record": data,

        "quality_status":
            quality_status,

        "corrections":
            corrections,

        "error_codes":
            errors
    }