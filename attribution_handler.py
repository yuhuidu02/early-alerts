"""
early_alerts/attribution_handler.py

Lambda entry point for single-student agent attribution.
Invoked asychronously by batch_handler.py for each student
whose signals triggers 2+ investigated negative rules.

Flow:
  1. Receive student + fired_rules + manifest_key from batch_handler.
  2. Resolve canvas IDs -> internal DB IDs
  3. Run attribution agent with all 4 tools
  4. Agent picks the single best category from the fired_rules
  5. Write result JSON to S3:
      agent_result/<data>/<canvas_user_id>_<canvas_course_id>.json
  6. check manifest - if all jobs are down, invoke merge_handler
"""

import json
import logging
import os

import boto3
import psycopg2
import psycopg2.extras
from strands import Agent
from strands.models import BedrockModel

from tools.attribution_tools import AttributionToolkit

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- System prompt ---
SYSTEM_PROMPT = """
You are a student alert categorization agent at a university.
 
The deterministic rules have flagged this student under MULTIPLE categories
simultaneously. Your job is to investigate using all 4 tools and decide
which single category best describes the student's primary issue.
 
You will be given a list of fired_rules — the categories whose conditions
were met. You must pick exactly ONE from that list as the final category.
 
RULES:
- Call ALL 4 tools before deciding
- Pick the category that best explains the root cause, not just a symptom
- Return only the category string, exactly as given in fired_rules
 
CONSTRUCT SCORING RULES:
- con (Confidence):  1-6, HIGHER is better.
- sth (Study Habits): 1-6, HIGHER is better.
- mot (Motivation):   1-6, HIGHER is better.
- abur (Stress):      1-6, HIGHER means MORE stressed.
- res (Resilience):   1-6, LOWER means LESS resilient.
 
Click z-scores: 0 = course average. Below -1.0 = warning. Below -2.0 = strong warning.
"""

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

def _q(cfg: dict, sql: str, params=None) -> list:
    with psycopg2.connect(**cfg) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
        
# --- S3 helpers ---
def _write_result(bucket: str, date_prefix: str, canvas_user_id: int, 
                  canvas_course_id: int, category: str) -> str:
    key = f"agent_results/{date_prefix}/{canvas_user_id}_{canvas_course_id}.json"
    body = json.dumps({
        "canvas_user_id": canvas_user_id,
        "canvas_course_id": canvas_course_id,
        "category": category
    }).encode("utf-8")
    boto3.client("s3").put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/json"
    )
    return key

def _read_manifest(bucket: str, manifest_key: str) -> dict:
    obj = boto3.client("s3").get_object(Bucket=bucket, Key=manifest_key)
    return json.loads(obj["Body"].read())

def _all_jobs_done(bucket: str, manifest: dict) -> bool:
    """
    Checks whether every expected agent result JSON exists in S3.
    Uses list_objects_v2 with the agent_results prefix - no read needed.
    """
    s3 = boto3.client("s3")
    date_prefix = manifest["date_prefix"]
    prefix = f"agent_results/{date_prefix}/"

    paginator = s3.get_paginator("list_objects_v2")
    done_keys = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            done_keys.add(obj["Key"])
    
    expected = {
        f"agent_results/{date_prefix}/{job['canvas_user_id']}_{job['canvas_course_id']}.json"
        for job in manifest["jobs"]
    }
    missing = expected - done_keys
    if missing:
        logger.info("%d agent jobs still pending: %s", len(missing), missing)
    
    return len(missing) == 0

def _invoke_merge(bucket: str, manifest_key: str) -> None:
    boto3.client("lambda", region_name=os.environ["AWS_DEFAULT_REGION"]).invoke(
        FunctionName   = os.environ["MERGE_FUNCTION_NAME"],
        InvocationType = "Event",
        Payload        = json.dumps({
            "manifest_key": manifest_key,
            "s3_bucket": bucket,
        }),
    )
    logger.info("Merge triggered for manifest %s", manifest_key)


# --- Lambda handler ---
def lambda_handler(event, context):
    # 1. Parse input
    canvas_user_id = int(event["canvas_user_id"])
    canvas_course_id = int(event["canvas_course_id"])
    fired_rules = event["fired_rules"]
    manifest_key = event["manifest_key"]
    bucket = event["s3_bucket"]
    as_of_date = event["as_of_date"]
    
    logger.info("Attribution for user %d in course %d with rules %s", 
                canvas_user_id, canvas_course_id, as_of_date, fired_rules)
    
    try:
        mydb_cfg, timescale_cfg = _build_db_configs()
    except Exception as e:
        logger.error("DB config error: %s", str(e))
        raise

    # 2. Resolve canvas IDs -> internal DB IDs
    try:
        mydb_rows = _q(mydb_cfg, """
            SELECT s.id AS student_id, s.name, c.id AS course_id, c.name AS course_name
            FROM students s
            JOIN student_courses sc ON sc.student_id = s.id
            JOIN courses c ON c.id = sc.course_id
            WHERE s.canvas_user_id = %s AND c.canvas_course_id = %s
        """, (canvas_user_id, canvas_course_id))

        ts_rows = _q(timescale_cfg, """
            SELECT s.id AS student_id, c.id AS course_id
            FROM students s JOIN courses c ON 1=1
            WHERE s.canvas_user_id = %s AND c.canvas_course_id = %s
        """, (canvas_user_id, canvas_course_id))

    except Exception as e:
      logger.error("ID resolution error: %s", str(e))
      return
    
    if not mydb_rows or not ts_rows:
        logger.warning("No DB records found for user %d in course %d", 
                       canvas_user_id, canvas_course_id)
        return
    
    student = mydb_rows[0]
    ts_student_id = ts_rows[0]["student_id"] if ts_rows else None
    ts_course_id = ts_rows[0]["course_id"] if ts_rows else None

    # 3. Run attribution agent with all 4 tools
    try:
        toolkit = AttributionToolkit(mydb_cfg, timescale_cfg, as_of_date=as_of_date)
        model = BedrockModel(
            model_id = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-haiku-4-5-20051001-v1:0"),
            region_name = os.environ.get("AWS_DEFAULT_REGION", "us-west-2"),
            temperature = 0.0,
        )
        agent = Agent(model=model, tools=toolkit.all(), system_prompt=SYSTEM_PROMPT)

        prompt = f"""
          Student Name: {student['name']}
          Course: {student['course_name']}
          As of Date: {as_of_date}

          The following categories all fired for this student as of {as_of_date}:
          {json.dumps(fired_rules, indent=2)}
          
          Internal IDs for tool calls:
          - Student ID (mydb):      {student['student_id']}
          - Course ID (mydb):       {student['course_id']}
          - Student ID (timescale): {ts_student_id}
          - Course ID (timescale):  {ts_course_id}
          
          Use all 4 tools then return exactly one category string from the fired_rules list above.
        """

        response = agent(prompt)
        response_text = str(response)
        category = None
        for rule in fired_rules:
            if rule in response_text:
                category = rule
                break
            
        # fallback: if agent didn't pick from the list, just assign the first one
        if category is None:
            category = fired_rules[0]
            logger.warning("Agent failed to pick category from list, defaulting to first one: %s", category)
            category = fired_rules[0]
        
        logger.info("Agent resolved: %s -> %s", student["name"], category)
    
    except Exception as e:
        logger.exception("Agent failed for user %d in course %d: %s", canvas_user_id, canvas_course_id, str(e))
        # fallback to first fired rule if agent fails
        category = fired_rules[0]
        logger.warning("Defaulting to first fired rule due to agent failure: %s", category)

    # 4. Write result JSON to S3
    manifest = _read_manifest(bucket, manifest_key)
    date_prefix = manifest["date_prefix"]
    _write_result(bucket, date_prefix, canvas_user_id, canvas_course_id, category)

    # 5. check manifest - if all jobs are down, invoke merge_handler
    if _all_jobs_done(bucket, manifest):
        logger.info("All agent jobs done for manifest %s, invoking merge handler", manifest_key)
        _invoke_merge(bucket, manifest_key)
    else:
        logger.info("Waiting for remaining agent jobs for manifest %s", manifest_key)



