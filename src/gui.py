import re
import sys
import queue
import threading
import subprocess
import tkinter as tk

from pathlib import Path
from tkinter import filedialog, messagebox


# =========================================================
# مسارات المشروع
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import PYTHON_EXECUTABLE
from src.main import choose_engine


# =========================================================
# ألوان الواجهة
# =========================================================

BG = "#F4F6F8"
CARD = "#FFFFFF"
PRIMARY = "#2563EB"
SUCCESS = "#16A34A"
DANGER = "#DC2626"
TEXT = "#111827"
MUTED = "#6B7280"


class PipelineGUI:

    def __init__(self, root):

        self.root = root

        self.root.title(
            "Hybrid Big Data ELT Pipeline"
        )

        self.root.geometry(
            "1200x850"
        )

        self.root.minsize(
            1050,
            750
        )

        self.root.configure(
            bg=BG
        )

        self.selected_file = None

        self.log_queue = queue.Queue()

        self.metrics = {}

        self.build_ui()

        self.clear_results()

        self.root.after(
            100,
            self.process_log_queue
        )


    # =====================================================
    # بناء الواجهة
    # =====================================================

    def build_ui(self):

        header = tk.Frame(
            self.root,
            bg=PRIMARY,
            height=100
        )

        header.pack(
            fill="x"
        )

        tk.Label(
            header,
            text="Hybrid Big Data ELT Pipeline",
            font=("Segoe UI", 24, "bold"),
            fg="white",
            bg=PRIMARY
        ).pack(
            pady=(18, 2)
        )

        tk.Label(
            header,
            text="Python Batch + PySpark + MongoDB",
            font=("Segoe UI", 11),
            fg="white",
            bg=PRIMARY
        ).pack()


        self.container = tk.Frame(
            self.root,
            bg=BG
        )

        self.container.pack(
            fill="both",
            expand=True,
            padx=25,
            pady=18
        )


        # =================================================
        # اختيار الملف
        # =================================================

        file_card = tk.Frame(
            self.container,
            bg=CARD,
            bd=1,
            relief="solid"
        )

        file_card.pack(
            fill="x",
            pady=(0, 12)
        )

        tk.Label(
            file_card,
            text="اختيار ملف البيانات",
            font=("Segoe UI", 15, "bold"),
            bg=CARD,
            fg=TEXT
        ).pack(
            anchor="e",
            padx=20,
            pady=(12, 6)
        )

        select_row = tk.Frame(
            file_card,
            bg=CARD
        )

        select_row.pack(
            fill="x",
            padx=20,
            pady=(0, 12)
        )

        self.file_label = tk.Label(
            select_row,
            text="لم يتم اختيار ملف",
            font=("Segoe UI", 11),
            bg="#F9FAFB",
            fg=MUTED,
            anchor="w",
            padx=12,
            height=2
        )

        self.file_label.pack(
            side="left",
            fill="x",
            expand=True,
            padx=(0, 10)
        )

        tk.Button(
            select_row,
            text="اختيار ملف CSV",
            command=self.select_file,
            bg=PRIMARY,
            fg="white",
            activebackground=PRIMARY,
            activeforeground="white",
            font=("Segoe UI", 11, "bold"),
            relief="flat",
            padx=20,
            pady=10,
            cursor="hand2"
        ).pack(
            side="right"
        )


        # =================================================
        # معلومات الملف
        # =================================================

        info = tk.Frame(
            self.container,
            bg=CARD,
            bd=1,
            relief="solid"
        )

        info.pack(
            fill="x",
            pady=(0, 12)
        )

        self.size_value = self.make_info(
            info,
            "حجم الملف"
        )

        self.engine_value = self.make_info(
            info,
            "المحرك المختار"
        )

        self.reason_value = self.make_info(
            info,
            "سبب الاختيار"
        )


        # =================================================
        # زر التشغيل
        # =================================================

        self.run_button = tk.Button(
            self.container,
            text="▶  بدء معالجة البيانات",
            command=self.start_pipeline,
            bg=SUCCESS,
            fg="white",
            activebackground=SUCCESS,
            activeforeground="white",
            font=("Segoe UI", 14, "bold"),
            relief="flat",
            pady=12,
            cursor="hand2",
            state="disabled"
        )

        self.run_button.pack(
            fill="x",
            pady=(0, 12)
        )


        # =================================================
        # نتائج التصنيف
        # =================================================

        result_frame = tk.Frame(
            self.container,
            bg=BG
        )

        result_frame.pack(
            fill="x",
            pady=(0, 10)
        )

        self.raw_label = self.metric_card(
            result_frame,
            "RAW",
            0
        )

        self.valid_label = self.metric_card(
            result_frame,
            "VALID",
            1
        )

        self.corrected_label = self.metric_card(
            result_frame,
            "CORRECTED",
            2
        )

        self.quarantine_label = self.metric_card(
            result_frame,
            "QUARANTINE",
            3
        )


        # =================================================
        # نتائج Upsert / Idempotency
        # =================================================

        write_frame = tk.Frame(
            self.container,
            bg=BG
        )

        write_frame.pack(
            fill="x",
            pady=(0, 10)
        )

        self.inserted_label = self.metric_card(
            write_frame,
            "INSERTED",
            0
        )

        self.updated_label = self.metric_card(
            write_frame,
            "UPDATED",
            1
        )

        self.unchanged_label = self.metric_card(
            write_frame,
            "UNCHANGED",
            2
        )


        # =================================================
        # الحالة
        # =================================================

        status_frame = tk.Frame(
            self.container,
            bg=CARD,
            bd=1,
            relief="solid"
        )

        status_frame.pack(
            fill="x",
            pady=(0, 10)
        )

        self.consistency_label = tk.Label(
            status_frame,
            text="CONSISTENCY: -",
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 11, "bold")
        )

        self.consistency_label.pack(
            side="left",
            padx=20,
            pady=10
        )

        self.time_label = tk.Label(
            status_frame,
            text="TIME: -",
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 11)
        )

        self.time_label.pack(
            side="left",
            padx=20
        )

        self.run_id_label = tk.Label(
            status_frame,
            text="RUN ID: -",
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 9)
        )

        self.run_id_label.pack(
            side="right",
            padx=20
        )


        # =================================================
        # Logs
        # =================================================

        log_card = tk.Frame(
            self.container,
            bg=CARD,
            bd=1,
            relief="solid"
        )

        log_card.pack(
            fill="both",
            expand=True
        )

        tk.Label(
            log_card,
            text="Execution Logs",
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 12, "bold")
        ).pack(
            anchor="w",
            padx=15,
            pady=(8, 4)
        )

        self.log_box = tk.Text(
            log_card,
            bg="#111827",
            fg="#E5E7EB",
            insertbackground="white",
            font=("Consolas", 10),
            relief="flat",
            height=8
        )

        self.log_box.pack(
            fill="both",
            expand=True,
            padx=15,
            pady=(0, 12)
        )


    # =====================================================
    # مربع معلومات
    # =====================================================

    def make_info(
        self,
        parent,
        title
    ):

        frame = tk.Frame(
            parent,
            bg=CARD
        )

        frame.pack(
            side="left",
            fill="x",
            expand=True,
            padx=10,
            pady=10
        )

        tk.Label(
            frame,
            text=title,
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 9)
        ).pack()

        label = tk.Label(
            frame,
            text="-",
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 11, "bold")
        )

        label.pack(
            pady=2
        )

        return label


    # =====================================================
    # بطاقة إحصائية
    # =====================================================

    def metric_card(
        self,
        parent,
        title,
        column
    ):

        frame = tk.Frame(
            parent,
            bg=CARD,
            bd=1,
            relief="solid"
        )

        frame.grid(
            row=0,
            column=column,
            sticky="nsew",
            padx=4
        )

        parent.grid_columnconfigure(
            column,
            weight=1
        )

        tk.Label(
            frame,
            text=title,
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 10)
        ).pack(
            pady=(8, 2)
        )

        label = tk.Label(
            frame,
            text="-",
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 18, "bold")
        )

        label.pack(
            pady=(0, 8)
        )

        return label


    # =====================================================
    # اختيار الملف
    # =====================================================

    def select_file(self):

        file_path = filedialog.askopenfilename(
            title="اختر ملف CSV",
            filetypes=[
                ("CSV Files", "*.csv"),
                ("All Files", "*.*")
            ]
        )

        if not file_path:
            return

        try:

            engine, size_mb, reason = (
                choose_engine(
                    file_path
                )
            )

        except Exception as error:

            messagebox.showerror(
                "خطأ",
                str(error)
            )

            return

        self.selected_file = file_path

        self.file_label.config(
            text=file_path,
            fg=TEXT
        )

        self.size_value.config(
            text=f"{size_mb:.2f} MB"
        )

        engine_text = (
            "Python Batch"
            if engine == "python_batch"
            else "PySpark"
        )

        self.engine_value.config(
            text=engine_text
        )

        self.reason_value.config(
            text=reason
        )

        self.run_button.config(
            state="normal"
        )

        self.clear_results()

        self.log_box.delete(
            "1.0",
            tk.END
        )

        self.write_log(
            f"File: {file_path}\n"
        )

        self.write_log(
            f"Size: {size_mb:.2f} MB\n"
        )

        self.write_log(
            f"Engine: {engine_text}\n"
        )


    # =====================================================
    # بدء المعالجة
    # =====================================================

    def start_pipeline(self):

        if not self.selected_file:
            return

        self.run_button.config(
            state="disabled",
            text="جاري المعالجة..."
        )

        self.clear_results()

        self.log_box.delete(
            "1.0",
            tk.END
        )

        threading.Thread(
            target=self.run_pipeline_thread,
            daemon=True
        ).start()


    # =====================================================
    # تشغيل main.py
    # =====================================================

    def run_pipeline_thread(self):

        command = [
            PYTHON_EXECUTABLE,
            str(
                PROJECT_ROOT
                / "src"
                / "main.py"
            ),
            self.selected_file
        ]

        try:

            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1
            )

            for line in process.stdout:

                self.log_queue.put(
                    line
                )

            return_code = process.wait()

            if return_code == 0:

                self.log_queue.put(
                    "__PIPELINE_SUCCESS__\n"
                )

            else:

                self.log_queue.put(
                    "__PIPELINE_FAILED__\n"
                )

        except Exception as error:

            self.log_queue.put(
                f"\nERROR: {error}\n"
            )

            self.log_queue.put(
                "__PIPELINE_FAILED__\n"
            )


    # =====================================================
    # استقبال Logs
    # =====================================================

    def process_log_queue(self):

        try:

            while True:

                line = (
                    self.log_queue
                    .get_nowait()
                )

                if "__PIPELINE_SUCCESS__" in line:

                    self.run_button.config(
                        state="normal",
                        text="▶  بدء معالجة البيانات"
                    )

                    messagebox.showinfo(
                        "نجاح",
                        "تمت معالجة الملف بنجاح"
                    )

                    continue

                if "__PIPELINE_FAILED__" in line:

                    self.run_button.config(
                        state="normal",
                        text="▶  بدء معالجة البيانات"
                    )

                    messagebox.showerror(
                        "فشل",
                        "حدث خطأ أثناء المعالجة"
                    )

                    continue

                self.write_log(
                    line
                )

                self.parse_metrics(
                    line
                )

        except queue.Empty:
            pass

        self.root.after(
            100,
            self.process_log_queue
        )


    # =====================================================
    # قراءة النتائج
    # =====================================================

    def parse_metrics(
        self,
        line
    ):

        patterns = {
            "raw": r"^RAW:\s*([\d,]+)",
            "valid": r"^VALID:\s*([\d,]+)",
            "corrected": r"^CORRECTED:\s*([\d,]+)",
            "quarantine": r"^QUARANTINE:\s*([\d,]+)",
            "inserted": r"^INSERTED:\s*([\d,]+)",
            "updated": r"^UPDATED:\s*([\d,]+)",
            "unchanged": r"^UNCHANGED:\s*([\d,]+)",
            "time": r"^TIME:\s*([\d.]+)",
            "consistency": (
                r"^CONSISTENCY:\s*(True|False)"
            ),
            "run_id": (
                r"Run ID:\s*"
                r"([0-9a-fA-F-]{36})"
            )
        }

        clean_line = line.strip()

        for key, pattern in patterns.items():

            match = re.search(
                pattern,
                clean_line
            )

            if match:

                self.metrics[key] = (
                    match.group(1)
                )

        self.update_metrics()


    # =====================================================
    # تحديث الشاشة
    # =====================================================

    def update_metrics(self):

        self.raw_label.config(
            text=self.metrics["raw"]
        )

        self.valid_label.config(
            text=self.metrics["valid"]
        )

        self.corrected_label.config(
            text=self.metrics["corrected"]
        )

        self.quarantine_label.config(
            text=self.metrics["quarantine"]
        )

        self.inserted_label.config(
            text=self.metrics["inserted"]
        )

        self.updated_label.config(
            text=self.metrics["updated"]
        )

        self.unchanged_label.config(
            text=self.metrics["unchanged"]
        )


        consistency = (
            self.metrics["consistency"]
        )

        if consistency == "True":

            self.consistency_label.config(
                text="CONSISTENCY: TRUE ✓",
                fg=SUCCESS
            )

        elif consistency == "False":

            self.consistency_label.config(
                text="CONSISTENCY: FALSE",
                fg=DANGER
            )

        else:

            self.consistency_label.config(
                text="CONSISTENCY: -",
                fg=TEXT
            )


        time_value = (
            self.metrics["time"]
        )

        if time_value != "-":

            self.time_label.config(
                text=f"TIME: {time_value} sec"
            )

        else:

            self.time_label.config(
                text="TIME: -"
            )


        run_id = (
            self.metrics["run_id"]
        )

        if run_id != "-":

            self.run_id_label.config(
                text=f"RUN ID: {run_id}"
            )

        else:

            self.run_id_label.config(
                text="RUN ID: -"
            )


    # =====================================================
    # إعادة النتائج للوضع الافتراضي
    # =====================================================

    def clear_results(self):

        self.metrics = {
            "raw": "-",
            "valid": "-",
            "corrected": "-",
            "quarantine": "-",
            "inserted": "-",
            "updated": "-",
            "unchanged": "-",
            "consistency": "-",
            "time": "-",
            "run_id": "-"
        }

        if hasattr(
            self,
            "raw_label"
        ):
            self.update_metrics()


    # =====================================================
    # إضافة Log
    # =====================================================

    def write_log(
        self,
        text
    ):

        self.log_box.insert(
            tk.END,
            text
        )

        self.log_box.see(
            tk.END
        )


# =========================================================
# تشغيل البرنامج
# =========================================================

if __name__ == "__main__":

    root = tk.Tk()

    app = PipelineGUI(
        root
    )

    root.mainloop()
