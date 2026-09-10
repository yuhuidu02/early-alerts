"""
early_alerts/post_hoc_rationale_handler.py

Lambda entry point for post-hoc audit rationale generation.
Invoked fire-and-forget by the Express /alerts/:canvasCourseId/override route
whenever an instructor's category diverges from Chiron's original call.

This agent runs BLIND: it does not see Chiron's original category or the
instructor's category. It re-investigates the student from scratch and
produces its own independent categorization + rationale, as a second audit
opinion. Comparison against chiron_category / instructor_category happens
downstream (by whoever reads alert_rationales), not by the agent itself.

Flow:
  1. Receive canvas_user_id + canvas_course_id + alert_id + chiron/instructor
     categories (stored for reference only, never shown to the agent).
  2. Resolve canvas IDs -> internal DB IDs (mydb + timescale).
  3. Run the same 4-tool toolkit as attribution_handler.py, agent picks its
     own category independently and explains it.
  4. Upsert the result into alert_rationales, keyed to the instructor's alert_id.
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

CATEGORY_LABELS = {
    "never_attended":      "Neg: Never Attended / No WebCampus Activity",
    "missing_assignments": "Neg: Missing Assignment(s)",
    "low_engagement":      "Neg: Lack of Engagement or Infrequent Attendance",
    "exam_performance":    "Neg: Exam/Quiz Performance",
    "satisfactory":        "Positive: Satisfactory Course Performance",
    "exceptional":         "Positive: Exceptional Course Performance",
}

VALID_CATEGORIES = set(CATEGORY_LABELS.keys())

SYSTEM_PROMPT = """
You are a student alert categorization agent at a university, performing an
independent audit review of this student's current status.

Investigate using all 4 tools, then decide which ONE of the following
categories best describes the student right now. You MUST choose one of
these exact codes — do not invent a new label, do not combine categories,
do not use a term that isn't on this list:
{chr(10).join(f"  {code}  ({label})" for code, label in CATEGORY_LABELS.items())}


CONSTRUCT SCORING RULES:
- con (Confidence):  1-6, HIGHER is better.
- sth (Study Habits): 1-6, HIGHER is better.
- mot (Motivation):   1-6, HIGHER is better.
- abur (Stress):      1-6, HIGHER means MORE stressed.
- res (Resilience):   1-6, LOWER means LESS resilient.

GRADE FIELDS (from get_missing_assignments):
- latest_current_grade: overall course grade (0-100), context only.
- latest_quiz_score: SOLE trigger for exam_performance (below 60).
- latest_missing_assignments: most recent missing assignment count.
- history: full weekly snapshot, oldest to newest.

Click z-scores: 0 = course average. Below -1.0 = warning. Below -2.0 = strong warning.

Write a short factual explanation (3-6 sentences) grounded in what the tools
showed, then end your response with exactly one line in this format, using
one of the six codes above verbatim — e.g. "missing_assignments", never a
new term you made up:
Category: <code>
"""

# --- Config (identical to attribution_handler.py) ---
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

def _extract_category(response_text: str) -> str | None:
    for line in reversed(response_text.strip().splitlines()):
        if line.lower().startswith("category:"):
            candidate = line.split(":", 1)[1].strip().lower()
            if candidate in VALID_CATEGORIES:
                return candidate
    return None

def _write_rationale(mydb_cfg: dict, alert_id: int, student_id: int, course_id: int,
                      chiron_category: str, instructor_category: str,
                      audit_category: str | None, rationale: str) -> None:
    with psycopg2.connect(**mydb_cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO alert_rationales
                   (alert_id, student_id, course_id, chiron_category, instructor_category,
                    audit_category, rationale)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (alert_id) DO UPDATE SET
                       rationale = EXCLUDED.rationale,
                       audit_category = EXCLUDED.audit_category,
                       chiron_category = EXCLUDED.chiron_category,
                       instructor_category = EXCLUDED.instructor_category,
                       created_at = NOW()""",
                (alert_id, student_id, course_id, chiron_category, instructor_category,
                 audit_category, rationale),
            )
        conn.commit()


# --- Lambda handler ---
def lambda_handler(event, context):
    canvas_user_id = int(event["canvas_user_id"])
    canvas_course_id = int(event["canvas_course_id"])
    alert_id = int(event["alert_id"])
    chiron_category = event["chiron_category"]
    instructor_category = event["instructor_category"]
    as_of_date = event["as_of_date"]

    logger.info("Post-hoc audit for canvas_user_id=%s canvas_course_id=%s alert_id=%s",
                canvas_user_id, canvas_course_id, alert_id)

    try:
        mydb_cfg, timescale_cfg = _build_db_configs()
    except Exception as e:
        logger.error("DB config error: %s", str(e))
        raise

    # Resolve canvas IDs -> internal DB IDs (same lookup as attribution_handler.py)
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

    audit_category = None
    try:
        toolkit = AttributionToolkit(mydb_cfg, timescale_cfg, as_of_date=as_of_date)
        model = BedrockModel(
            model_id=os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-haiku-4-5-20051001-v1:0"),
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-west-2"),
            temperature=0.0,
        )
        agent = Agent(model=model, tools=toolkit.all(), system_prompt=SYSTEM_PROMPT)

        prompt = f"""
          Student Name: {student['name']}
          Course: {student['course_name']}
          As of Date: {as_of_date}

          Internal IDs for tool calls:
          - Student ID (mydb):      {student['student_id']}
          - Course ID (mydb):       {student['course_id']}
          - Student ID (timescale): {ts_student_id}
          - Course ID (timescale):  {ts_course_id}

          Use all 4 tools, then write your rationale and category as described in your instructions.
        """

        response = agent(prompt)
        response_text = str(response).strip()
        rationale = response_text
        audit_category = _extract_category(response_text)
        if audit_category is None:
            logger.warning("Agent returned an unparseable/invalid category for alert_id=%s — "
                            "response did not use one of the six valid codes", alert_id)
            audit_category = "unclassified"

    except Exception as e:
        logger.exception("Agent failed for alert_id=%s: %s", alert_id, str(e))
        rationale = "failure"
        audit_category = None

    try:
        _write_rationale(mydb_cfg, alert_id, student["student_id"], student["course_id"],
                          chiron_category, instructor_category, audit_category, rationale)
    except Exception as e:
        logger.exception("Failed to write rationale for alert_id=%s: %s", alert_id, str(e))