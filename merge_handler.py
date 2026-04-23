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
"""

import json
import io
import csv
import logging
import os
from datetime import datetime

import boto3
import openyxl 
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

from batch_categorize import CATEGORY_ORDER, build_xlsx_bytes, CAT_NEEDS_AGENT

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
    Returns dict keyed by (canvas_user_id, canvas_course_id) -> resolved category.
    Reads only the JSON files that exits - skips any that failed/timed out.
    """

    date_prefix = manifest["date_prefix"]
    results = {}

    for job in manifest["agent_jobs"]:
        key = f"agent_results/{date_prefix}/{job['canvas_user_id']}_{job['canvas_course_id']}.json"
        try:
            result = _read_json(bucket, key)
            results[(result["canvas_user_id"], result["canvas_course_id"])] = result["category"]
        except Exception as e:
            # Agent job didn't complete successfully - leave as CAT_NEEDS_AGENT
            logger.warning("Missing agent result: %s", key)

    logger.info("Collected %d/%d agent results", len(results), manifest["total_agent_jobs"])
    return results

# --- Merge --- 
def _merge(original_rows: list, agent_results: dict) -> list:
    """
    Replaces CAT_NEEDS_AGENT in original_rows with resolved categories from agent_results.
    Rows without agent jobs (i.e., not CAT_NEEDS_AGENT) are left unchanged.
    """
    merged = []
    for row in original_rows:
        if row["category"] == CAT_NEEDS_AGENT:
            key = (int(row["canvas_user_id"]), int(row["canvas_course_id"]))
            resolved = agent_results.get(key)
            if resolved:
                row = dict(row)  # create a copy to avoid mutating original
                row["category"] = resolved
        merged.append(row)
    return merged

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
            "course_name":         row["course_name"],
            "canvas_course_id":    int(row["canvas_course_id"]),
            "category":            row["category"],
            "fired_rules":         [r for r in row.get("fired_rules", "").split(" | ") if r],
            "current_grade":       float(row["current_grade"]) if row.get("current_grade") else None,
            "missing_assignments": int(row["missing_assignments"]) if row.get("missing_assignments") else 0,
            "total_clicks":        int(row["total_clicks"]) if row.get("total_clicks") else 0,
            "breadth_z":           float(row["breadth_z"]) if row.get("breadth_z") else None,
            "last_active":         row.get("last_active"),
        })
 
    # 6. Build final CSV
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(merged_rows[0].keys()))
    writer.writeheader()
    writer.writerows(merged_rows)
    csv_bytes = buf.getvalue().encode("utf-8")
 
    # 7. Write final files
    final_csv_key   = f"{date_prefix}/alerts_{batch_timestamp}_final.csv"
    final_excel_key = f"{date_prefix}/alerts_{batch_timestamp}_final.xlsx"
 
    s3_csv   = _upload(csv_bytes,               bucket, final_csv_key,
                       "text/csv")
    s3_excel = _upload(build_xlsx_bytes(records), bucket, final_excel_key,
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
 
    logger.info("Merge complete. Final files written.")
 
    return {
        "statusCode": 200,
        "body": json.dumps({
            "total":          len(merged_rows),
            "agent_resolved": len(agent_results),
            "s3_csv":         s3_csv,
            "s3_excel":       s3_excel,
        }),
    }