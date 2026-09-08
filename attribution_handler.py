"""
early_alerts/attribution_handler.py

Lambda entry point for single-student agent attribution.
Invoked asychronously by batch_handler.py for each student
whose signals triggers 2+ investigated negative rules.

Flow (mode="production", default — unchanged):: 
  1. Receive student + fired_rules + manifest_key from batch_handler.
  2. Resolve canvas IDs -> internal DB IDs
  3. Run attribution agent with all 4 tools
  4. Agent picks the single best category from the fired_rules
  5. Write result JSON to S3:
      agent_result/<data>/<canvas_user_id>_<canvas_course_id>.json
  6. check manifest - if all jobs are down, invoke merge_handler

Flow (mode="research"):
  Same ID resolution + manifest/merge bookkeeping. Step 3-5 instead run the agent
  TWICE with free choice among all 6 categories (not constrained to fired_rules):
    - AI+ML:   fired_rules (real, threshold-adjusted) + ML at-risk probability -> canonical
    - AI-only: fired_rules_no_ml (unadjusted-threshold context) + no ML info -> comparison
  Result JSON carries both categories + rationales + the ML fields.
"""

import json
import logging
import os
import re

import boto3
import psycopg2
import psycopg2.extras
from strands import Agent
from strands.models import BedrockModel

from batch_categorize import (
    CAT_NEVER_ATTENDED,
    CAT_MISSING_ASSIGNMENTS,
    CAT_LOW_ENGAGEMENT,
    CAT_EXAM_PERFORMANCE,
    CAT_POSITIVE_EXCEPTIONAL,
    CAT_POSITIVE_SATISFACTORY,
)
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

GRADE FIELDS (from get_missing_assignments):
- latest_current_grade: the student's overall course grade (0-100). Provided for context only —
  it does NOT trigger any alert category on its own.
- latest_quiz_score: the student's most recent Chiron quiz score (0-100). This is the
  SOLE trigger for Neg: Exam/Quiz Performance. Below 60 = poor quiz performance.
- latest_missing_assignments: the student's most recent missing assignment count.
- history: full weekly snapshot of missing_assignments and quiz_score ordered oldest to newest.
  Use the trend to distinguish chronic issues from sudden drops or improving trajectories —
  this context is key when choosing the root cause category.
  
Click z-scores: 0 = course average. Below -1.0 = warning. Below -2.0 = strong warning.
"""

# --- System prompt (research mode — free choice among all 6 categories) ---
VALID_CATEGORIES = [
    CAT_NEVER_ATTENDED,
    CAT_MISSING_ASSIGNMENTS,
    CAT_LOW_ENGAGEMENT,
    CAT_EXAM_PERFORMANCE,
    CAT_POSITIVE_EXCEPTIONAL,
    CAT_POSITIVE_SATISFACTORY,
]

SYSTEM_PROMPT_RESEARCH = """
You are a student alert categorization agent at a university, running in RESEARCH MODE.
 
This student is a conflict/ambiguous case: either multiple deterministic rules fired
simultaneously, or the deterministic rules and an ML risk model disagreed. You are NOT
restricted to a pre-filtered list — investigate using all 4 tools, then choose the ONE
category that best describes this student's current status from the full set below.
 
CATEGORIES (choose exactly one):
- "Neg: Never Attended / No WebCampus Activity" — no meaningful Canvas/WebCampus activity recorded.
- "Neg: Missing Assignment(s)" — one or more missing assignments is the primary issue.
- "Neg: Lack of Engagement or Infrequent Attendance" — activity is present but sparse, shallow,
  or infrequent relative to peers (low click breadth/intensity/coherence).
- "Neg: Exam/Quiz Performance" — the primary issue is poor quiz/exam scores, not attendance or engagement.
- "Positive: Satisfactory Course Performance" — no significant negative signal; performing adequately.
- "Positive: Exceptional Course Performance" — no significant negative signal; performing strongly
  (current course grade around 85% or higher is a good rule of thumb, but use overall judgment).
 
You will also be told which categories the deterministic rule engine flagged for this student
("fired_rules"). Treat this as CONTEXT ONLY, not a constraint — you may agree with it, pick a
different category among the 6, or land on a positive category even if a negative rule fired,
if your investigation supports that.
 
RULES:
- Call ALL 4 tools before deciding.
- Pick the category that best explains the root cause, not just a symptom.
- Base your decision on the tool evidence, not just on which rules fired.
 
CONSTRUCT SCORING RULES:
- con (Confidence):  1-6, HIGHER is better.
- sth (Study Habits): 1-6, HIGHER is better.
- mot (Motivation):   1-6, HIGHER is better.
- abur (Stress):      1-6, HIGHER means MORE stressed.
- res (Resilience):   1-6, LOWER means LESS resilient.
 
GRADE FIELDS (from get_missing_assignments):
- latest_current_grade: the student's overall course grade (0-100). Context only.
- latest_quiz_score: the student's most recent Chiron quiz score (0-100).
- latest_missing_assignments: the student's most recent missing assignment count.
- history: full weekly snapshot ordered oldest to newest. Use the trend to distinguish
  chronic issues from sudden drops or improving trajectories.
 
Click z-scores: 0 = course average. Below -1.0 = warning. Below -2.0 = strong warning.
 
You may also be given an ML model's estimated probability of subsequent academic
difficulty. If given, weigh it as ONE signal among the others — not an automatic
override of your own judgment from the tool evidence.
 
OUTPUT FORMAT — return ONLY a JSON object, no markdown fences, no extra text:
{
  "category": "<exact category string from the list above>",
  "rationale": "<2-4 sentences citing the specific evidence that drove this decision>"
}
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

def _write_result_research(bucket: str, date_prefix: str, canvas_user_id: int, canvas_course_id: int,
                            category: str, rationale: str, category_no_ml: str, rationale_no_ml: str,
                            at_risk_predicted, at_risk_probability) -> str:
    key = f"agent_results/{date_prefix}/{canvas_user_id}_{canvas_course_id}.json"
    body = json.dumps({
        "canvas_user_id": canvas_user_id,
        "canvas_course_id": canvas_course_id,
        "category": category,                    # AI+ML — canonical
        "rationale": rationale,
        "category_no_ml": category_no_ml,         # AI-only — comparison
        "rationale_no_ml": rationale_no_ml,
        "at_risk_predicted": at_risk_predicted,
        "at_risk_probability": at_risk_probability,
    }).encode("utf-8")
    boto3.client("s3").put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/json"
    )
    return key

# --- Research-mode agent helpers ---
_JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)

def _parse_agent_output_research(response_text: str, fired_rules: list) -> tuple:
    """
    Returns (category, rationale). Falls back gracefully if the agent didn't
    return valid JSON or returned an unrecognized category.
    """
    match = _JSON_BLOCK_RE.search(response_text)
    if match:
        try:
            parsed = json.loads(match.group(0))
            category = parsed.get("category")
            rationale = parsed.get("rationale", "").strip()
            if category in VALID_CATEGORIES and rationale:
                return category, rationale
        except (json.JSONDecodeError, AttributeError):
            pass
 
    for cat in VALID_CATEGORIES:
        if cat in response_text:
            logger.warning("Could not parse structured JSON; recovered category by substring match: %s", cat)
            return cat, f"[Could not parse structured rationale — raw agent output: {response_text[:500]}]"
 
    default_category = fired_rules[0] if fired_rules else CAT_POSITIVE_SATISFACTORY
    logger.warning("Agent output completely unparseable; defaulting to %s", default_category)
    return default_category, f"[Agent output unparseable — defaulted. Raw output: {response_text[:500]}]"
 
def _build_prompt_research(student: dict, fired_rules: list, as_of_date: str, ids: dict,
                           ml_prediction: dict | None) -> str:
    fired_text = ", ".join(fired_rules) if fired_rules else "(none — no deterministic rule fired)"
 
    ml_block = ""
    if ml_prediction is not None:
        prob = ml_prediction.get("at_risk_probability")
        predicted = ml_prediction.get("at_risk_predicted")
        ml_block = f"""
          ML Model Risk Estimate:
          - Probability of subsequent academic difficulty: {prob if prob is not None else "unknown"}
          - Model's binary at-risk prediction: {predicted}
        """
 
    return f"""
      Student Name: {student['name']}
      Course: {student['course_name']}
      As of Date: {as_of_date}
 
      Deterministic rule engine flagged (context only, not a constraint): {fired_text}
      {ml_block}
      Internal IDs for tool calls:
      - Student ID (mydb):      {ids['mydb_student_id']}
      - Course ID (mydb):       {ids['mydb_course_id']}
      - Student ID (timescale): {ids['ts_student_id']}
      - Course ID (timescale):  {ids['ts_course_id']}
 
      Use all 4 tools, then return your decision as the JSON object described in your instructions.
    """

def _run_agent_condition(toolkit: AttributionToolkit, model: BedrockModel, student: dict,
                          fired_rules: list, as_of_date: str, ids: dict,
                          ml_prediction: dict | None) -> tuple:
    """
    Runs one independent agent instance for one condition (AI-only or AI+ML).
    No shared state between conditions — a fresh Agent() each call.
    """
    agent = Agent(model=model, tools=toolkit.all(), system_prompt=SYSTEM_PROMPT_RESEARCH)
    prompt = _build_prompt_research(student, fired_rules, as_of_date, ids, ml_prediction)
    try:
        response = agent(prompt)
        return _parse_agent_output_research(str(response), fired_rules)
    except Exception as e:
        logger.exception("Agent call failed for %s: %s", student.get("name"), str(e))
        default_category = fired_rules[0] if fired_rules else CAT_POSITIVE_SATISFACTORY
        return default_category, f"[Agent call raised an exception: {e}]"
 
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
        for job in manifest["agent_jobs"]
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
    mode = event.get("mode", "production")
    fired_rules_no_ml = event.get("fired_rules_no_ml")
    at_risk_predicted = event.get("at_risk_predicted")
    at_risk_probability = event.get("at_risk_probability")
    
    logger.info("Attribution for canvas_user_id=%s canvas_course_id=%s as_of=%s fired=%s",
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

    if mode == "research":
        # 3. Run attribution agent TWICE — free choice among all 6 categories
        ids = {
            "mydb_student_id": student["student_id"],
            "mydb_course_id": student["course_id"],
            "ts_student_id": ts_student_id,
            "ts_course_id": ts_course_id,
        }
        try:
            toolkit = AttributionToolkit(mydb_cfg, timescale_cfg, as_of_date=as_of_date)
            model = BedrockModel(
                model_id = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-haiku-4-5-20051001-v1:0"),
                region_name = os.environ.get("AWS_DEFAULT_REGION", "us-west-2"),
                temperature = 0.0,
            )
 
            # AI+ML — canonical. Uses the REAL (threshold-adjusted) fired_rules + ML info.
            ml_prediction = None
            if at_risk_predicted is not None:
                ml_prediction = {"at_risk_predicted": at_risk_predicted, "at_risk_probability": at_risk_probability}
            category, rationale = _run_agent_condition(
                toolkit, model, student, fired_rules, as_of_date, ids, ml_prediction=ml_prediction
            )
 
            # AI-only — comparison. Uses fired_rules_no_ml (unadjusted thresholds) + no ML info,
            # so nothing ML-derived leaks into this condition, not even indirectly via thresholds.
            fired_no_ml = fired_rules_no_ml if fired_rules_no_ml is not None else fired_rules
            category_no_ml, rationale_no_ml = _run_agent_condition(
                toolkit, model, student, fired_no_ml, as_of_date, ids, ml_prediction=None
            )
 
            logger.info("Agent resolved (research): %s -> AI+ML=%s | AI-only=%s",
                        student["name"], category, category_no_ml)
        except Exception as e:
            logger.exception("Research agent failed for user %d in course %d: %s",
                              canvas_user_id, canvas_course_id, str(e))
            default_category = fired_rules[0] if fired_rules else CAT_POSITIVE_SATISFACTORY
            category = default_category
            rationale = f"[Agent call raised an exception: {e}]"
            category_no_ml = default_category
            rationale_no_ml = f"[Agent call raised an exception: {e}]"
        
        # 4. Write result JSON to S3
        manifest = _read_manifest(bucket, manifest_key)
        date_prefix = manifest["date_prefix"]
        _write_result_research(
            bucket, date_prefix, canvas_user_id, canvas_course_id,
            category, rationale, category_no_ml, rationale_no_ml,
            at_risk_predicted, at_risk_probability,
        )

    else:

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



