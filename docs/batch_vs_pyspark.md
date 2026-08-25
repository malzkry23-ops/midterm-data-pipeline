# Python Batch vs PySpark Comparison

## 1. Overview

يستخدم المشروع مسارين مختلفين لمعالجة ملفات CSV حسب حجم الملف:

- Python Batch للملفات الصغيرة.
- PySpark للملفات الكبيرة.

حد الاختيار المستخدم في المشروع:

200 MB

---

## 2. Test Data

### Python Batch Test

تم اختبار Python Batch على عينة صغيرة:

- Rows: 100,000
- File Size: 41.77 MB
- Batch Size: 5,000
- Number of Batches: 20
- Elapsed Time: 4.57 seconds

Observed Throughput:

100,000 / 4.57 = approximately 21,882 rows/sec

### PySpark Test

تم اختبار PySpark على الملف الكبير:

- Rows: 30,000,000
- File Size: 12.35 GB
- Input Partitions: 99
- Elapsed Time: 382.23 seconds
- Throughput: 78,487.73 rows/sec

---

## 3. Performance Comparison

| Metric | Python Batch | PySpark |
|---|---:|---:|
| Tested Rows | 100,000 | 30,000,000 |
| File Size | 41.77 MB | 12.35 GB |
| Batch / Partitions | 5,000 rows per batch | 99 input partitions |
| Elapsed Time | 4.57 sec | 382.23 sec |
| Observed Throughput | ~21,882 rows/sec | 78,487.73 rows/sec |
| Processing Model | Sequential Batches | Parallel DataFrame Processing |
| Best Use | Small Files | Large Files |

Important:

هذه ليست مقارنة Benchmark على نفس حجم الملف، لأن كل محرك تم اختباره في المسار المصمم له.

الأرقام تمثل نتائج التشغيل الفعلية للمشروع.

---

## 4. Memory Usage

### Python Batch

Python Batch لا يقوم بتحميل ملف CSV كاملًا إلى الذاكرة.

يقرأ البيانات على دفعات:

Batch Size = 5,000 rows

وهذا يجعل استهلاك الذاكرة محدودًا ومناسبًا للملفات الصغيرة والمتوسطة.

### PySpark

PySpark يقسم البيانات إلى Partitions ويعالجها بالتوازي.

في الملف الكبير تم استخدام:

99 input partitions

وعند مرحلة التحويل:

214 partitions

هذا يسمح بالتعامل مع البيانات الكبيرة دون الاعتماد على قائمة Python واحدة في الذاكرة.

لم يتم تسجيل قياس رقمي مباشر لاستهلاك RAM أثناء التجربة، لذلك لا يتم عرض أرقام ذاكرة غير مقاسة.

---

## 5. Scalability

### Python Batch

مناسب عندما:

- حجم الملف صغير.
- لا نحتاج معالجة موزعة.
- نريد تنفيذًا بسيطًا.
- استهلاك الموارد منخفض.

لكن عند زيادة حجم البيانات يصبح التنفيذ التسلسلي أبطأ.

### PySpark

مناسب عندما:

- البيانات كبيرة.
- عدد السجلات بالملايين.
- نحتاج Parallel Processing.
- نحتاج DataFrame API.
- نحتاج قابلية توسع مستقبلية إلى Cluster.

PySpark أكثر قابلية للتوسع لأنه يعتمد على تقسيم العمل إلى Partitions.

---

## 6. MongoDB Integration

### Python Batch

يستخدم:

PyMongo

ويتم إدخال البيانات على دفعات باستخدام:

insert_many

### PySpark

تم استخدام:

MongoDB Spark Connector

لتحميل البيانات الكبيرة إلى MongoDB بالتوازي.

وهذا يحقق تكامل Spark مع MongoDB في مسار البيانات الكبيرة.

---

## 7. Router Decision

النظام يختار المحرك تلقائيًا حسب حجم الملف:

File <= 200 MB
-> Python Batch

File > 200 MB
-> PySpark

نتائج الاختبار:

41.77 MB
-> python_batch

12650.32 MB
-> pyspark

---

## 8. Final Conclusion

Python Batch هو الخيار الأنسب للملفات الصغيرة لأنه بسيط ويستخدم Streaming Batches ولا يحتاج Spark overhead.

PySpark هو الخيار الأنسب للملفات الكبيرة لأنه يوفر Parallel Processing وPartitions وقابلية توسع أفضل.

لذلك يستخدم المشروع Hybrid Architecture بدل استخدام محرك واحد لجميع أحجام البيانات.

هذا التصميم يحقق توازنًا بين:

- Simplicity
- Performance
- Memory Efficiency
- Scalability

---

## Why 200 MB Threshold?

تم اختيار 200 MB كحد عملي في هذا المشروع للفصل بين المسار الخفيف والمسار الموزع.

السبب:

- الملفات الصغيرة لا تحتاج Spark overhead.
- Python Batch يستطيع معالجتها بكفاءة باستخدام Streaming Batches.
- الملفات الأكبر تستفيد أكثر من Parallel Processing وSpark Partitions.
- الحد قابل للتعديل من config/settings.py وليس قيمة ثابتة داخل منطق البرنامج.

هذا الحد هو قرار هندسي خاص ببيئة الاختبار الحالية، وليس قاعدة عامة لجميع الأنظمة.

في بيئة Production يمكن تحديده بناءً على:

- Available RAM
- CPU cores
- Dataset structure
- Required processing time
- Cluster resources
