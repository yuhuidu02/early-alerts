"""
early_alerts/merge_handler.py

Merges agent results back into the original CSV/Excel.

Triggerd by:
    - attribution_handler.py when the last agent job completes (fast path)
    - EventBridge            as a safety net ~15 min after batch (handles failures)

Flow:
    1. Read manifest.json - find original CSV key + all expected agent jobs
    2. Read Original CSV from S3
    3. Read all agent result JSONs from S3
    4. Replace CAT_NEEDS_AGENT rows with resolved categories
    5. Write alerts_TIMESTAMP_final.csv and alerts_TIMESTAMP_final.xlsx to S3

manifest["mode"] == "research":
    Agent result JSONs additionally carry rationale/category_no_ml/rationale_no_ml/
    at_risk_predicted/at_risk_probability. The .csv is unaffected (CSV_FIELDS still
    only has the base 16 columns, extra keys are dropped). The .xlsx gets 5 extra
    columns via build_xlsx_bytes_research() instead of batch_categorize's
    build_xlsx_bytes() — blank for rows that were never agent-routed.
"""

import json
import io
import csv
import logging
import os
from datetime import datetime
from collections import defaultdict

import boto3
import openpyxl 
import psycopg2
import psycopg2.extras
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

from batch_categorize import CATEGORY_ORDER, build_xlsx_bytes, CAT_NEEDS_AGENT, CSV_FIELDS, XLSX_HEADERS, COL_WIDTHS

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# # --- Category mapping: batch_categorize full strings -> alerts table short codes ---
CATEGORY_MAP = {
    "Neg: Never Attended / No WebCampus Activity":        "never_attended",
    "Neg: Missing Assignment(s)":                         "missing_assignments",
    "Neg: Lack of Engagement or Infrequent Attendance":   "low_engagement",
    "Neg: Exam/Quiz Performance":                         "exam_performance",
    "Positive: Satisfactory Course Performance":          "satisfactory",
    "Positive: Exceptional Course Performance":           "exceptional",
}

# --- S3 Helpers ---
def _read_json(bucket: str, key: str) -> dict:
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())

def _read_csv(bucket: str, key: str) -> list:
    obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    text = obj["Body"].read().decode("utf-8")
    return list(csv.DictReader(io.StringIO(text)))

def _upload(body: bytes, bucket: str, key: str, content_type: str) -> str:
    boto3.client("s3").put_object(
        Bucket=bucket, Key=key, Body=body, ContentType=content_type
    )
    url = f"s3://{bucket}/{key}"
    logger.info("Uploaded %d bytes → %s", len(body), url)
    return url

# --- Collect agent results ---
def _collect_agent_results(bucket: str, manifest: dict) -> dict:
    """
    Returns dict keyed by (canvas_user_id, canvas_course_id) -> full agent result dict.
    Production result JSONs have just {"category": ...}; research mode result JSONs
    additionally have rationale/category_no_ml/rationale_no_ml/at_risk_predicted/
    at_risk_probability — _merge() reads whatever's there via .get().
    Reads only the JSON files that exist - skips any that failed/timed out.
    """

    date_prefix = manifest["date_prefix"]
    results = {}

    for job in manifest["agent_jobs"]:
        key = f"agent_results/{date_prefix}/{job['canvas_user_id']}_{job['canvas_course_id']}.json"
        try:
            result = _read_json(bucket, key)
            results[(result["canvas_user_id"], result["canvas_course_id"])] = result
        except Exception as e:
            # Agent job didn't complete successfully - leave as CAT_NEEDS_AGENT
            logger.warning("Missing agent result: %s", key)

    logger.info("Collected %d/%d agent results", len(results), manifest["total_agent_jobs"])
    return results

# --- Write Chiron results to alerts table ---
def _write_alerts_to_db(mydb_cfg: dict, merged_rows: list, alert_date: str):
    inserts = []
    skipped = 0

    for row in merged_rows:
        category_short = CATEGORY_MAP.get(row["category"])
        if not category_short:
            logger.warning("Skipping row with unmapped category: %s", row["category"])
            skipped += 1
            continue
        inserts.append((
            int(row["canvas_user_id"]),
            int(row["canvas_course_id"]),
            category_short,
            alert_date,
        ))

    if not inserts:
        logger.info("No valid rows to insert into DB.")
        return
    
    try:
        with psycopg2.connect(**mydb_cfg) as conn:
            with conn.cursor() as cur:
                canvas_user_ids = list({r[0] for r in inserts})
                canvas_course_ids = list({r[1] for r in inserts})
                cur.execute("""
                    SELECT canvas_user_id, id FROM students
                    WHERE canvas_user_id = ANY(%s)
                """, (canvas_user_ids,))
                student_map = {r[0]: r[1] for r in cur.fetchall()}

                cur.execute("""
                    SELECT canvas_course_id, id FROM courses
                    WHERE canvas_course_id = ANY(%s)
                """, (canvas_course_ids,))
                course_map = {r[0]: r[1] for r in cur.fetchall()}

                rows_to_insert = []
                for canvas_uid, canvas_cid, category, alert_date in inserts:
                    student_id = student_map.get(canvas_uid)
                    course_id = course_map.get(canvas_cid)
                    if not student_id or not course_id:
                        logger.warning("Skipping row with missing student/course: %s, %s", canvas_uid, canvas_cid)
                        skipped += 1
                        continue
                    rows_to_insert.append((student_id, course_id, alert_date, category))
                
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO alerts (student_id, course_id, alert_date, entry_type, category)
                    VALUES %s
                """, [(sid, cid, ad, "chiron", cat) for sid, cid, ad, cat in rows_to_insert])
            
            conn.commit()

        logger.info("Inserted %d rows into alerts table (%d skipped)", len(rows_to_insert), skipped)

    except Exception as e:
        logger.error("Failed to write alerts to DB: %s", e)
        # non-blocking — S3 write already succeeded

# --- Merge --- 
def _merge(original_rows: list, agent_results: dict) -> list:
    """
    Replaces CAT_NEEDS_AGENT in original_rows with resolved categories from agent_results.
    Rows without agent jobs (i.e., not CAT_NEEDS_AGENT) are left unchanged.
    In research mode, also attaches rationale/category_no_ml/rationale_no_ml/
    at_risk_predicted/at_risk_probability from the agent result onto the row
    (absent -> not set, so downstream .get() calls default to None/blank).
    """
    merged = []
    for row in original_rows:
        if row["category"] == CAT_NEEDS_AGENT:
            key = (int(row["canvas_user_id"]), int(row["canvas_course_id"]))
            resolved = agent_results.get(key)
            if resolved:
                row = dict(row)  # create a copy to avoid mutating original
                row["category"] = resolved
                if "rationale" in resolved:
                    row["rationale"] = resolved.get("rationale")
                    row["category_no_ml"] = resolved.get("category_no_ml")
                    row["rationale_no_ml"] = resolved.get("rationale_no_ml")
                    row["at_risk_predicted"] = resolved.get("at_risk_predicted")
                    row["at_risk_probability"] = resolved.get("at_risk_probability")
        merged.append(row)
    return merged

# --- Research-mode xlsx builder (5 extra columns beyond batch_categorize's build_xlsx_bytes) ---
RESEARCH_XLSX_HEADERS = XLSX_HEADERS + [
    "ML At-Risk Predicted", "ML At-Risk Probability",
    "Rationale", "Category (No ML)", "Rationale (No ML)",
]
RESEARCH_COL_WIDTHS = COL_WIDTHS + [16, 18, 60, 40, 60]

def build_xlsx_bytes_research(records: list) -> bytes:
    """
    Single-sheet workbook for one course's records — mirrors batch_categorize's
    build_xlsx_bytes() row-writing for the first 16 columns, then appends the
    5 research columns. Blank for any row that was never agent-routed.
    """
    wb = openpyxl.Workbook()
    ws = wb.active
 
    ws.append(RESEARCH_XLSX_HEADERS)
    for cell in ws[1]:
        cell.font = Font(name="Arial", bold=True)
        cell.alignment = Alignment(horizontal="center")
 
    sorted_recs = sorted(
        records,
        key=lambda r: (
            CATEGORY_ORDER.index(r["category"]) if r["category"] in CATEGORY_ORDER else 99,
            r["student_name"],
        )
    )
    for rec in sorted_recs:
        fired_str = " | ".join(rec.get("fired_rules", []))
        ws.append([
            rec["canvas_user_id"],
            rec["student_name"],
            rec["nshe_id"],
            rec["course_name"],
            rec["canvas_course_id"],
            rec["section_number"],
            rec["category"],
            fired_str,
            rec.get("current_grade"),
            rec.get("quiz_score"),
            rec.get("missing_assignments"),
            rec.get("total_clicks"),
            rec.get("click_coverage_z"),
            rec.get("click_intensity_z"),
            rec.get("click_coherence_z"),
            rec.get("last_active"),
            rec.get("at_risk_predicted"),
            rec.get("at_risk_probability"),
            rec.get("rationale"),
            rec.get("category_no_ml"),
            rec.get("rationale_no_ml"),
        ])
        ri = ws.max_row
        ws.cell(ri, 9).number_format = "0.0"     # current_grade
        ws.cell(ri, 10).number_format = "0.0"    # quiz_score
        ws.cell(ri, 13).number_format = "0.00"   # click_coverage_z
        ws.cell(ri, 14).number_format = "0.00"   # click_intensity_z
        ws.cell(ri, 15).number_format = "0.00"   # click_coherence_z
        ws.cell(ri, 18).number_format = "0.000"  # at_risk_probability
        for col in (19, 21):  # Rationale, Rationale (No ML) — wrap for readability
            ws.cell(ri, col).alignment = Alignment(wrap_text=True, vertical="top")
 
    for i, w in enumerate(RESEARCH_COL_WIDTHS, 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
 
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
 
# --- Lambda handler ---
def lambda_handler(event, context):
    manifest_key = event["manifest_key"]
    bucket = event["s3_bucket"]

    logger.info("Merge handler triggered with manifest_key=%s", manifest_key)

    # 1. Read manifest
    try:
        manifest = _read_json(bucket, manifest_key)
        logger.info("Manifest read successfully: %s", manifest_key)
    except Exception as e:
        logger.error("Failed to read manifest: %s", e)
        return {"statusCode": 500, "body": "Failed to read manifest"}
    
    batch_timestamp = manifest["batch_timestamp"]
    date_prefix = manifest["date_prefix"]
    csv_key = manifest["csv_key"]

    # 2. Read original CSV
    try:
        original_rows = _read_csv(bucket, csv_key)
    except Exception as e:
        logger.error("Failed to read original CSV: %s", e)
        return {"statusCode": 500, "body": "Failed to read original CSV"}

    logger.info("Original CSV has %d rows", len(original_rows))

    # 3. Collect agent results
    agent_results = _collect_agent_results(bucket, manifest)

    # 4. Merge
    merged_rows = _merge(original_rows, agent_results)

    try:
        from batch_handler import _build_db_configs
        mydb_cfg, _ = _build_db_configs()
        _write_alerts_to_db(mydb_cfg, merged_rows, manifest["as_of_date"])
    except Exception as e:
        logger.error("alert DB write failed (non-blocking): %s", e)

    resolved_count = sum(
        1 for row in merged_rows 
        if row["category"] != CAT_NEEDS_AGENT
        and any(row["canvas_user_id"] == job["canvas_user_id"] for job in manifest["agent_jobs"])
    )
    logger.info("Resolved %d/%d agent rows", resolved_count, manifest["total_agent_jobs"])

    # 5. Convert merged CSV to XLSX
    records = []
    for row in merged_rows:
        records.append({
            "canvas_user_id":      int(row["canvas_user_id"]),
            "student_name":        row["student_name"],
            "nshe_id":             row["nshe_id"],
            "course_name":         row["course_name"],
            "canvas_course_id":    int(row["canvas_course_id"]),
            "section_number":      row["section_number"],
            "category":            row["category"],
            "fired_rules":         [r for r in row.get("fired_rules", "").split(" | ") if r],
            "current_grade":       float(row["current_grade"]) if row.get("current_grade") else None,
            "quiz_score":          float(row["quiz_score"]) if row.get("quiz_score") else None,
            "missing_assignments": int(row["missing_assignments"]) if row.get("missing_assignments") else 0,
            "total_clicks":        int(row["total_clicks"]) if row.get("total_clicks") else 0,
            "click_coverage_z":    float(row["click_coverage_z"]) if row.get("click_coverage_z") else None,
            "click_intensity_z":    float(row["click_intensity_z"]) if row.get("click_intensity_z") else None,
            "click_coherence_z":    float(row["click_coherence_z"]) if row.get("click_coherence_z") else None,
            "last_active":         row.get("last_active"),
            # research-mode-only — absent/None for any row that wasn't agent-routed
            "at_risk_predicted":   row.get("at_risk_predicted"),
            "at_risk_probability": row.get("at_risk_probability"),
            "rationale":           row.get("rationale"),
            "category_no_ml":      row.get("category_no_ml"),
            "rationale_no_ml":     row.get("rationale_no_ml"),
        })

    # 6. Group records by canvas_course_id
    by_course = defaultdict(list)
    for rec in records:
        by_course[rec["canvas_course_id"]].append(rec)

    # 7. Write one CSV + one XLSX per course
    mode = manifest.get("mode", "production")
    s3_files = {}
    for canvas_course_id, course_records in by_course.items():
        final_csv_key   = f"{date_prefix}/alerts_{batch_timestamp}_{canvas_course_id}_final.csv"
        final_excel_key = f"{date_prefix}/alerts_{batch_timestamp}_{canvas_course_id}_final.xlsx"

        # CSV — unaffected by mode: CSV_FIELDS only has the base 16 columns, and
        # DictWriter(extrasaction="ignore") silently drops the 5 research keys.
        buf = io.StringIO()
        flat = []
        for r in course_records:
            row = dict(r)
            row["fired_rules"] = "|".join(r.get("fired_rules") or [])
            flat.append(row)
        writer = csv.DictWriter(buf, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(flat)
        csv_bytes = buf.getvalue().encode("utf-8")

        s3_csv = _upload(csv_bytes, bucket, final_csv_key, "text/csv")
        
        excel_builder = build_xlsx_bytes_research if mode == "research" else build_xlsx_bytes
        s3_excel = _upload(
            excel_builder(course_records), bucket, final_excel_key,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        s3_files[canvas_course_id] = {"csv": s3_csv, "excel": s3_excel}

    logger.info("Merge complete. Final files written for %d courses.", len(s3_files))

    # update chiron_runs status if this was instructor-triggered
    run_id = manifest.get("run_id")
    if run_id:
        try:
            password = boto3.client("secretsmanager", region_name=os.environ["AWS_DEFAULT_REGION"]) \
                       .get_secret_value(SecretId=os.environ["DB_SECRET_NAME"])["SecretString"]
            mydb_cfg = {
                "host": os.environ["MYDB_HOST"],
                "port": int(os.environ.get("MYDB_PORT", 5432)),
                "dbname": os.environ["MYDB_NAME"],
                "user": "dbuser",
                "password": password,
                "sslmode": "prefer",
                "connect_timeout": 10,
            }
            with psycopg2.connect(**mydb_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE chiron_runs SET status = 'complete' WHERE id = %s",
                        (run_id,)
                    )
                conn.commit()
            logger.info("Updated chiron_runs status to 'complete' for run_id=%s", run_id)
        except Exception as e:
            logger.warning("Could not update chiron_runs: %s", e)  # non-fatal

    return {
        "statusCode": 200,
        "body": json.dumps({
            "total":          len(merged_rows),
            "agent_resolved": len(agent_results),
            "files_by_course": s3_files,
        }),
    }

 
    # # 6. Build final CSV
    # buf = io.StringIO()
    # writer = csv.DictWriter(buf, fieldnames=list(merged_rows[0].keys()))
    # writer.writeheader()
    # writer.writerows(merged_rows)
    # csv_bytes = buf.getvalue().encode("utf-8")
 
    # # 7. Write final files
    # final_csv_key   = f"{date_prefix}/alerts_{batch_timestamp}_final.csv"
    # final_excel_key = f"{date_prefix}/alerts_{batch_timestamp}_final.xlsx"
 
    # s3_csv   = _upload(csv_bytes,               bucket, final_csv_key,
    #                    "text/csv")
    # s3_excel = _upload(build_xlsx_bytes(records), bucket, final_excel_key,
    #                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
 
    # logger.info("Merge complete. Final files written.")
 
    # return {
    #     "statusCode": 200,
    #     "body": json.dumps({
    #         "total":          len(merged_rows),
    #         "agent_resolved": len(agent_results),
    #         "s3_csv":         s3_csv,
    #         "s3_excel":       s3_excel,
    #     }),
    # }