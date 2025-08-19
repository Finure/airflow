from __future__ import annotations
import os
import io
import csv
from datetime import datetime, timezone

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

from google.cloud import storage

# Config
SHARED_DIR = os.environ.get("SHARED_DIR", "/shared")  
GCS_BUCKET = os.environ.get('GCS_BUCKET', 'finure-airflow')
GCS_INPUT_BLOB = os.environ.get('INPUT_FILE_PATH', 'datasets/in/dataset.csv')
GCS_OUTPUT_PREFIX = os.environ.get('OUTPUT_FILE_PREFIX', 'datasets/out')
GCS_REJECT_PREFIX = os.environ.get("REJECT_FILE_PREFIX", 'datasets/reject')
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL')  

# Validation
CREDIT_SCORE_MIN = 0
CREDIT_SCORE_MAX = 850

default_args = {
    "owner": "finure-data-platform",
    "retries": 0,
}

with DAG(
    dag_id="dataset_clean_task",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["gcs", "validation", "slack"],
) as dag:

    def _download_from_gcs(**context):
        """Download input CSV to tmp file and pass the file path via XCom"""
        client = storage.Client()  # ADC via GKE Workload Identity
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(GCS_INPUT_BLOB)
        data = blob.download_as_bytes()

        in_path = f"{SHARED_DIR}/input.csv"
        os.makedirs(os.path.dirname(in_path), exist_ok=True)
        with open(in_path, "wb") as f:
            f.write(data)
        context["ti"].xcom_push(key="input_csv_path", value=in_path)

    def _validate_and_clean(**context):
        """Read input CSV, validate/clean, write cleaned and rejects to /tmp, push paths via XCom"""
        in_path = context["ti"].xcom_pull(key="input_csv_path") or f"{SHARED_DIR}/input.csv"
        if not os.path.exists(in_path):
            raise FileNotFoundError(f"Input CSV not found at expected shared path: {in_path}")

        with open(in_path, "r", encoding="utf-8", newline="") as f:
            text = f.read()

        REQUIRED_COLS = ["Age", "Income", "Employed", "CreditScore", "LoanAmount"]
        OPTIONAL_COLS = ["Approved"]

        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            raise ValueError("Input CSV is empty")

        raw_header = [h.strip() for h in rows[0]]
        has_header = all(c in raw_header for c in REQUIRED_COLS)

        if has_header:
            header = raw_header
            data_rows = rows[1:]
        else:
            header = REQUIRED_COLS + OPTIONAL_COLS 
            data_rows = rows

        col_index = {c: (header.index(c) if c in header else None) for c in (REQUIRED_COLS + OPTIONAL_COLS)}
        missing_required = [c for c in REQUIRED_COLS if col_index[c] is None]
        if has_header and missing_required:
            raise ValueError(f"Missing required columns: {missing_required}")

        cleaned = []
        rejected = []

        def parse_int(val, allow_any_len=False, min_len=None, max_len=None, allow_negative=True):
            sval = ("" if val is None else str(val).strip())
            if sval == "":
                return None, "missing"
            if sval.startswith("-"):
                if not allow_negative:
                    return None, "negative_not_allowed"
                if not sval[1:].isdigit():
                    return None, "not_integer"
            elif not sval.isdigit():
                return None, "not_integer"
            ival = int(sval)
            if not allow_any_len:
                digits = len(str(abs(ival)))
                if min_len is not None and digits < min_len:
                    return None, f"too_short_len<{min_len}"
                if max_len is not None and digits > max_len:
                    return None, f"too_long_len>{max_len}"
            return ival, None

        def get_val(row, col):
            idx = col_index[col]
            if idx is None or idx >= len(row):
                return None
            return row[idx]

        for r_i, row in enumerate(data_rows, start=1):
            reasons = []

            age_raw = get_val(row, "Age")
            age, err = parse_int(age_raw, allow_any_len=False, min_len=2, max_len=3, allow_negative=False)
            if err:
                reasons.append(f"Age:{err}")

            income_raw = get_val(row, "Income")
            income, err = parse_int(income_raw, allow_any_len=True)
            if err:
                reasons.append(f"Income:{err}")

            emp_raw = get_val(row, "Employed")
            employed, err = parse_int(emp_raw, allow_any_len=True, allow_negative=False)
            if err:
                reasons.append(f"Employed:{err}")
                employed = None
            else:
                if employed not in (0, 1):
                    reasons.append("Employed:not_0_or_1")

            cs_raw = get_val(row, "CreditScore")
            cs, err = parse_int(cs_raw, allow_any_len=True, allow_negative=False)
            if err:
                reasons.append(f"CreditScore:{err}")
            else:
                if not (CREDIT_SCORE_MIN <= cs <= CREDIT_SCORE_MAX):
                    reasons.append("CreditScore:out_of_range")

            la_raw = get_val(row, "LoanAmount")
            loan_amt, err = parse_int(la_raw, allow_any_len=True)
            if err:
                reasons.append(f"LoanAmount:{err}")

            appr_raw = get_val(row, "Approved")
            approved = ""
            if appr_raw is not None and str(appr_raw).strip() != "":
                a_val, a_err = parse_int(appr_raw, allow_any_len=True, allow_negative=False)
                if a_err:
                    reasons.append(f"Approved:{a_err}")
                else:
                    if a_val not in (0, 1):
                        reasons.append("Approved:not_0_or_1")
                    else:
                        approved = a_val

            required_missing = any(v is None for v in [age, income, employed, cs, loan_amt])
            if reasons or required_missing:
                rejected.append(
                    {"row_number": r_i, "row": row, "reasons": reasons or ["required_missing"]}
                )
                continue

            cleaned.append(
                {
                    "Age": age,
                    "Income": income,
                    "Employed": employed,
                    "CreditScore": cs,
                    "LoanAmount": loan_amt,
                    "Approved": approved,
                }
            )

        # Write cleaned back to /tmp
        cleaned_path = f"{SHARED_DIR}/cleaned.csv"
        with open(cleaned_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerow(["Age", "Income", "Employed", "CreditScore", "LoanAmount", "Approved"])
            for rec in cleaned:
                writer.writerow([
                    rec["Age"], rec["Income"], rec["Employed"], rec["CreditScore"], rec["LoanAmount"], rec["Approved"]
                ])

        # Write rejects to /tmp
        rejects_path = f"{SHARED_DIR}/rejected.csv"
        with open(rejects_path, "w", encoding="utf-8", newline="") as f:
            rej_writer = csv.writer(f, lineterminator="\n")
            rej_writer.writerow(["row_number", "reasons", "raw_row"])
            for rej in rejected:
                rej_writer.writerow([
                    rej["row_number"],
                    ";".join(rej["reasons"]),
                    "|".join("" if x is None else str(x) for x in rej["row"])
                ])

        stats = {
            "total_rows_including_header": len(rows),
            "data_rows_evaluated": len(data_rows),
            "clean_rows": len(cleaned),
            "rejected_rows": len(rejected),
        }

        ti = context["ti"]
        in_path = ti.xcom_pull(task_ids="download_from_gcs", key="input_csv_path") or f"{SHARED_DIR}/input.csv"
        ti.xcom_push(key="cleaned_csv_path", value=cleaned_path)
        ti.xcom_push(key="rejected_csv_path", value=rejects_path)
        ti.xcom_push(key="validation_stats", value=stats)

    def _upload_to_gcs(**context):
        ti = context["ti"]
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        cleaned_path = ti.xcom_pull(task_ids="validate_and_clean", key="cleaned_csv_path") or f"{SHARED_DIR}/cleaned.csv"
        rejected_path = ti.xcom_pull(task_ids="validate_and_clean", key="rejected_csv_path") or f"{SHARED_DIR}/rejected.csv"

        if not cleaned_path or not os.path.exists(cleaned_path):
            raise FileNotFoundError(f"Cleaned CSV path missing or not found: {cleaned_path!r}")
        if not rejected_path or not os.path.exists(rejected_path):
            raise FileNotFoundError(f"Rejected CSV path missing or not found: {rejected_path!r}")

        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        base = os.path.basename(GCS_INPUT_BLOB)
        base_noext = base.rsplit(".", 1)[0]
        out_prefix = GCS_OUTPUT_PREFIX.rstrip("/") + "/"
        rej_prefix = GCS_REJECT_PREFIX.rstrip("/") + "/"

        cleaned_name = f"{out_prefix}{base_noext}_clean_{ts}.csv"
        rejects_name = f"{rej_prefix}{base_noext}_rejects_{ts}.csv"

        with open(cleaned_path, "rb") as f:
            bucket.blob(cleaned_name).upload_from_file(f, content_type="text/csv")
        with open(rejected_path, "rb") as f:
            bucket.blob(rejects_name).upload_from_file(f, content_type="text/csv")

        ti.xcom_push(key="cleaned_object_path", value=cleaned_name)
        ti.xcom_push(key="rejects_object_path", value=rejects_name)

    def _slack_report(**context):
        import json, urllib.request
        ti = context["ti"]

        stats = ti.xcom_pull(task_ids="validate_and_clean", key="validation_stats") or {}
        cleaned_obj = ti.xcom_pull(task_ids="upload_to_gcs", key="cleaned_object_path")
        rejects_obj = ti.xcom_pull(task_ids="upload_to_gcs", key="rejects_object_path")

        webhook = SLACK_WEBHOOK_URL  
        if not webhook:
            print("SLACK_WEBHOOK_URL not set, skipping notify")
            return
        lines = []
        if stats:
            lines.append(f"Clean rows: *{stats.get('clean_rows', 0)}* / Evaluated: *{stats.get('data_rows_evaluated', 0)}*")
            lines.append(f"Rejected rows: *{stats.get('rejected_rows', 0)}*")
        else:
            lines.append("No stats found from validate step (task may have failed or been skipped)")
        if cleaned_obj:
            lines.append(f"Cleaned file: `{cleaned_obj}`")
        if rejects_obj:
            lines.append(f"Rejects report: `{rejects_obj}`")

        msg = (
            f"*CSV Validation Complete*\n"
            f"Bucket: `{GCS_BUCKET}`\n"
            f"Input: `{GCS_INPUT_BLOB}`\n"
            + "\n".join(lines)
        )

        data = json.dumps({"text": msg}).encode("utf-8")
        req = urllib.request.Request(webhook, data=data, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                print(f"Slack status: {resp.status}")
        except Exception as e:
            print(f"Slack notify failed: {e}")

    download = PythonOperator(
        task_id="download_from_gcs",
        python_callable=_download_from_gcs,
    )

    validate_clean = PythonOperator(
        task_id="validate_and_clean",
        python_callable=_validate_and_clean,
    )

    upload = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=_upload_to_gcs,
    )

    slack = PythonOperator(
        task_id="slack_report",
        python_callable=_slack_report,
        trigger_rule="all_done",
    )

    download >> validate_clean >> upload >> slack
