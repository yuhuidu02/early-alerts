"""
early_alerts/batch_handler.py

Lambda entry point for batch student categorization.

Flow: 
1. Run bulk categorization across all active courses
2. Write CSV/Excel to S3 (CAT_NEEDS_AGENT rows are placeholders)
3. Write manifest.json to S3 listing all agent jobs
4. Fan out one async attribution_handler invocation per queued student
5. EventBridge triggers merge_handler as a safety net after a fixed delay

Triggered by:
 - EventBridge on a fixed schedule (e.g., every Sunday at 2am)
 - Direct invoke / POST / batch with optional { "canvas_course_ids": [...]}
"""

import json
import logging
import os
from datetime import datetime 

import boto3
import psycopg2
import psycopg2.extras

import batch_categorize

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Config ---
def _get_secrets(name: str) -> str:
    return boto3.client("secretsmanager", region_name=os.environ["AWS_DEFAULT_REGION"]) \
                .get_secret_value(SecretId=name)["SecretString"]

def _build_db_configs() -> tuple:
    password = _get_secrets(os.environ["DB_SECRET_NAME"])
    base = {"user": "dbuser", "password": password, "sslmode": "prefer", "connect_timeout": 10}
    return (
        {**base, "host": os.environ["MYDB_HOST"], 
                 "port": int(os.environ.get("MYDB_PORT", 5432)),
                 "dbname": os.environ["MYDB_NAME"]},
        {**base, "host": os.environ["TIMESCALE_HOST"], 
                 "port": int(os.environ.get("TIMESCALE_PORT", 5432)), 
                 "dbname": os.environ["TIMESCALE_NAME"]},
    )

def _get_active_course_ids(mydb_cfg: dict) -> list:
    with psycopg2.connect(**mydb_cfg) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT canvas_course_id
                FROM courses
                WHERE status = 'active'
            """)
            return [row["canvas_course_id"] for row in cur.fetchall()]
        
# --- Manifest helpers ---
def _write_manifest(
        bucket: str, 
        batch_timestamp: str,
        date_prefix: str,
        csv_key: str,
        excel_key: str,
        agent_queue: list,
        as_of_date: str,
) -> str:
    """
    Writes a manifest.json to S3 that tracks:
     - the original CSV/Excel keys (for merge to find them)
     - every agent job that needs to complete
     - total count (for merge to know when done)
    """
    manifest = {
        "batch_timestamp": batch_timestamp,
        "date_prefix": date_prefix,
        "as_of_date": as_of_date,
        "csv_key": csv_key,
        "excel_key": excel_key,
        "total_agent_jobs": len(agent_queue),
        "agent_jobs" : [
            {
                "canvas_user_id": job["canvas_user_id"],
                "canvas_course_id": job["canvas_course_id"],
                "fired_rules": job["fired_rules"],
            }
            for job in agent_queue
        ],
    }
    key = f"{date_prefix}/manifest_{batch_timestamp}.json"
    boto3.client("s3").put_object(
        Bucket=bucket, 
        Key=key, 
        Body=json.dumps(manifest, indent=2).encode("utf-8"), 
        ContentType="application/json"
    )
    logger.info("Wrote manifest with %d agent jobs to s3://%s/%s", len(agent_queue), bucket, key)
    return key

# --- Agent fan-out helpers ---
def _invoke_attribution(student: dict, manifest_key: str, bucket: str, as_of_date: str) -> None:
    boto3.client("lambda", region_name=os.environ["AWS_DEFAULT_REGION"]).invoke(
        FunctionName = os.environ["ATTRIBUTION_FUNCTION_NAME"],
        InvocationType = "Event",
        Payload = json.dumps({
            "canvas_user_id": student["canvas_user_id"],
            "canvas_course_id": student["canvas_course_id"],
            "fired_rules": student["fired_rules"],
            "manifest_key": manifest_key,
            "s3_bucket": bucket,
            "as_of_date": as_of_date,
        }),
    )
    logger.info("Invoked attribution for student %d in course %d",
                student["canvas_user_id"], student["canvas_course_id"])
    
# --- Lambda handler ---
def lambda_handler(event, context):
    try:
        body = json.loads(event["body"]) if isinstance(event.get("body"), str) else event.get("body", event)
        canvas_course_ids = body.get("canvas_course_ids") if body else None
        as_of_date = body.get("as_of_date") if body else None
    except Exception:
        canvas_course_ids = None
        as_of_date = None
    
    # Default as_of_date to today if not supplied
    if not as_of_date:
        as_of_date = datetime.utcnow().strftime("%Y-%m-%d")
    
    try:
        mydb_cfg, timescale_cfg = _build_db_configs()
    except Exception as e:
        logger.error("Config error: %s", e)
        return {"statusCode": 500, "body": json.dumps({"error": "Configuration error"})}
    
    if not canvas_course_ids:
        try:
            canvas_course_ids = _get_active_course_ids(mydb_cfg)
            logger.info("No course IDs supplied; defaulting to all active courses: %s", canvas_course_ids)
        except Exception as e:
            logger.error("DB error fetching active courses: %s", e)
            return {"statusCode": 500, "body": json.dumps({"error": "Database error"})}
    
    if not canvas_course_ids:
        logger.warning("No active courses found; exiting without doing anything.")
        return {"statusCode": 200, "body": json.dumps({"message": "No active courses"})}
    
    logger.info("Running batch for %d courses: %s", len(canvas_course_ids), canvas_course_ids)
 
    bucket = os.environ["ALERTS_BUCKET"]

    try:
        result = batch_categorize.run(
            mydb_cfg=mydb_cfg,
            timescale_cfg=timescale_cfg,
            canvas_course_ids=canvas_course_ids,
            s3_bucket=bucket,
            as_of_date=as_of_date,
        )
    except Exception as e:
        logger.exception("Batch job failed: %s", e)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
    
    agent_queue = result.pop("agent_queue", [])
    batch_timestamp = result["batch_timestamp"]
    date_prefix = result["date_prefix"]

    manifest_key = None
    agent_launched = 0

    if agent_queue:
        manifest_key = _write_manifest(
            bucket=bucket,
            batch_timestamp=batch_timestamp,
            date_prefix=date_prefix,
            csv_key=result["csv_key"].replace(f"s3://{bucket}/", ""),
            excel_key=result["excel_key"].replace(f"s3://{bucket}/", ""),
            agent_queue=agent_queue,
            as_of_date=as_of_date,
        )
        for student in agent_queue:
            try:
                _invoke_attribution(student, manifest_key, bucket, as_of_date)
                agent_launched += 1
            except Exception as e:
                logger.error("Failed to invoke attribution for student %d in course %d: %s", 
                             student["canvas_user_id"], student["canvas_course_id"], str(e))

    result["agent_launched"] = agent_launched
    result["manifest_key"] = manifest_key
    logger.info("Done. %d categorized, %d sent to agent.", result["total"], agent_launched)

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(result, indent=2)
    }