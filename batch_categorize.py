"""
early_alerts/batch_categorize.py

Bulk categorization across one or more courses.

Categorization logic:
  - Evaluate all rules for each student simultaneously using the determinsitic rules
  - If exactly 1 rule fires -> assign that category directly (no agent)
  - If 2+ rules fire        -> flag for agent to decide best category
  - If 0 rules fire         -> Positive (satisfactory or exceptional)

  research_mode=True (see run()/categorize()) additionally flags for the agent any
  student where the deterministic rules and the ML at-risk prediction disagree
  (0 rules fired but ML flags at-risk, or exactly 1 rule fired but ML doesn't).
  Default (research_mode=False) behavior is unchanged from production.

Output written to S3:
  s3://<ALERTS_BUCKET>/alerts/YYYY/MM/DD/alerts_YYYYMMDD_HHMMSS.csv
  s3://<ALERTS_BUCKET>/alerts/YYYY/MM/DD/alerts_YYYYMMDD_HHMMSS.xlsx

"""

import csv
import io
import logging
import re
import statistics
from collections import Counter, defaultdict
from datetime import datetime

import boto3
import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# --- Category constants ---
CAT_POSITIVE_SATISFACTORY = "Positive: Satisfactory Course Performance"
CAT_POSITIVE_EXCEPTIONAL = "Positive: Exceptional Course Performance"
CAT_MISSING_ASSIGNMENTS = "Neg: Missing Assignment(s)"
CAT_EXAM_PERFORMANCE = "Neg: Exam/Quiz Performance"
CAT_LOW_ENGAGEMENT = "Neg: Lack of Engagement or Infrequent Attendance"
CAT_NEVER_ATTENDED = "Neg: Never Attended / No WebCampus Activity"
CAT_NEEDS_AGENT = "Needs Agent Review" # Internal - never written to output, only used to flag for agent review

CATEGORY_ORDER = [
    CAT_NEVER_ATTENDED,
    CAT_MISSING_ASSIGNMENTS,
    CAT_LOW_ENGAGEMENT,
    CAT_EXAM_PERFORMANCE,
    CAT_POSITIVE_EXCEPTIONAL,
    CAT_POSITIVE_SATISFACTORY,
    CAT_NEEDS_AGENT
]



# --- DB helpers ---
def _query(cfg: dict, sql: str, params=None) -> list:
    with psycopg2.connect(**cfg) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
        
# --- Stage 1a. Student roster (mydb) ---

# def fetch_student_roster(mydb_cfg: dict, canvas_course_ids: list, as_of_date: str) -> list:
#     """
#     Fetches the most recent score snapshot per (student, course) on or before as_of_date.
#     Uses student_score_snapshots instead of students table for historical accuracy.
#     """
#     ph = ",".join(["%s"] * len(canvas_course_ids))
#     rows = _query(mydb_cfg, f"""
#         SELECT DISTINCT ON (s.canvas_user_id, c.canvas_course_id)
#             s.canvas_user_id,
#             s.id            AS student_id,
#             s.name,
#             s.integration_id,
#             ss.missing_assignments,
#             ss.current_score,
#             ss.quiz_score,
#             sc.section_number,
#             c.canvas_course_id,
#             c.id              AS course_id,
#             c.name            AS course_name
#         FROM students s
#         JOIN student_courses sc ON sc.student_id = s.id
#         JOIN courses c          ON c.id = sc.course_id
#         JOIN student_score_snapshots ss ON ss.student_id = s.id AND ss.course_id = c.id
#         WHERE c.canvas_course_id IN ({ph})
#             AND ss.recorded_at <= %s
#             AND sc.status = 'active' 
#         ORDER BY s.canvas_user_id, c.canvas_course_id, ss.recorded_at DESC
#     """, canvas_course_ids + [as_of_date])
#     logger.info("Roster: %d student-course rows across %d courses (as of %s)", 
#                 len(rows), len(canvas_course_ids), as_of_date)
#     return rows

def fetch_student_roster(mydb_cfg: dict, canvas_course_ids: list, as_of_date: str) -> list:
    """
    Fetches the latest score snapshot per (student, course), regardless of when
    Chiron is run. current_score always reflects the most recent data available.
    Uses student_score_snapshots instead of students table for historical accuracy.
    """
    ph = ",".join(["%s"] * len(canvas_course_ids))
    rows = _query(mydb_cfg, f"""
        SELECT DISTINCT ON (s.canvas_user_id, c.canvas_course_id)
            s.canvas_user_id,
            s.id            AS student_id,
            s.name,
            s.integration_id,
            ss.missing_assignments,
            ss.current_score,
            ss.quiz_score,
            sc.section_number,
            c.canvas_course_id,
            c.id              AS course_id,
            c.name            AS course_name
        FROM students s
        JOIN student_courses sc ON sc.student_id = s.id
        JOIN courses c          ON c.id = sc.course_id
        JOIN student_score_snapshots ss ON ss.student_id = s.id AND ss.course_id = c.id
        WHERE c.canvas_course_id IN ({ph})
            AND sc.status = 'active' 
        ORDER BY s.canvas_user_id, c.canvas_course_id, ss.recorded_at DESC
    """, canvas_course_ids)
    logger.info("Roster: %d student-course rows across %d courses (latest snapshot, requested as_of_date=%s)", 
                len(rows), len(canvas_course_ids), as_of_date)
    return rows

# --- Stage 1b. Click signal (mytimescale) ---

def fetch_click_signals(timescale_cfg: dict, canvas_course_ids: list, as_of_date: str) -> dict:
    """
    Reads pre-computed click stats from student_click_stats table.
    Z-score computed against course population from the same table
    Falls back to empty dict if no stats found on or before as_of_date.
    """
    ph = ",".join(["%s"] * len(canvas_course_ids))

    rows = _query(timescale_cfg, f"""
        SELECT
            s.canvas_user_id,
            c.canvas_course_id,
            scs.total_clicks,
            scs.click_slope,
            scs.last_active,
            scs.coverage,
            scs.intensity,
            scs.coherence,
            AVG(scs.coverage)  OVER (PARTITION BY scs.course_id) AS course_avg_coverage,
            STDDEV(scs.coverage) OVER (PARTITION BY scs.course_id) AS course_std_coverage,
            AVG(scs.intensity)  OVER (PARTITION BY scs.course_id) AS course_avg_intensity,
            STDDEV(scs.intensity) OVER (PARTITION BY scs.course_id) AS course_std_intensity,
            AVG(scs.coherence)  OVER (PARTITION BY scs.course_id) AS course_avg_coherence,
            STDDEV(scs.coherence) OVER (PARTITION BY scs.course_id) AS course_std_coherence
        FROM student_click_stats scs
        JOIN students s  ON s.id  = scs.student_id
        JOIN courses c   ON c.id  = scs.course_id
        WHERE c.canvas_course_id IN ({ph})
          AND scs.as_of_date = (
                SELECT MAX(as_of_date)
                FROM student_click_stats
                WHERE course_id = scs.course_id
                AND as_of_date <= %s
            )
    """, canvas_course_ids + [as_of_date])

    if not rows:
        logger.warning("No click stats found for canvas_course_ids: %s as_of_date: %s", canvas_course_ids, as_of_date)
        return {}
    
    logger.info("Fetched %d click stats rows for %d courses (as of %s)", len(rows), len(canvas_course_ids), as_of_date)

    def z_score(value, avg, std):
        if not std or std == 0:
            return 0.0
        return round((value - avg) / std, 2)
    
    result = {}
    for row in rows:
        coverage = row["coverage"] or 0
        intensity = row["intensity"] or 0
        coherence = row["coherence"] or 0

        result[(row["canvas_user_id"], row["canvas_course_id"])] = {
            "total_clicks": row["total_clicks"] or 0,
            "click_slope": row["click_slope"] or 0,
            "last_active": str(row["last_active"])[:10] if row["last_active"] else None,
            "breadth": coverage,
            "breadth_z": z_score(coverage, row["course_avg_coverage"], row["course_std_coverage"]),
            "intensity": intensity,
            "intensity_z": z_score(intensity, row["course_avg_intensity"], row["course_std_intensity"]),
            "coherence": coherence,
            "coherence_z": z_score(coherence, row["course_avg_coherence"], row["course_std_coherence"]),
        }

    logger.info("Processed click signals for %d student-course pairs", len(result))
    return result



# def fetch_click_signals(timescale_cfg: dict, canvas_course_ids: list, as_of_date: str) -> dict:
#     """
#     Returns dict keyed by (canvas_user_id, canvas_course_id).
#     All click data filtered to timestamp <= as_of_date.
#     Z-scores computed against the course population up to that same date
#     for a true historical snapshot.
#     """
#     ph = ",".join(["%s"] * len(canvas_course_ids))

#     course_map = {
#         r["canvas_course_id"]: r["id"]
#         for r in _query(timescale_cfg,
#                         f"SELECT canvas_course_id, id FROM courses WHERE canvas_course_id IN ({ph})",
#                         canvas_course_ids)
#     }
#     if not course_map:
#         logger.warning("No matching courses found in timescale for canvas_course_ids: %s", canvas_course_ids)
#         return {}
    
#     ts_course_ids = list(course_map.values())
#     ph2 = ",".join(["%s"] * len(ts_course_ids))

#     student_map = {
#         r["canvas_user_id"]: r["id"]
#         for r in _query(timescale_cfg, f"""
#             SELECT DISTINCT s.canvas_user_id, s.id
#             FROM students s
#             JOIN click_sequences cs ON cs.user_id = s.id
#             WHERE cs.course_id IN ({ph2})
#             AND cs.timestamp <= %s
#         """, ts_course_ids + [as_of_date])
#     }

#     logger.info("Fetching all click events up to %s for %d courses...", as_of_date, len(ts_course_ids))

#     all_clicks = _query(timescale_cfg, f"""
#         SELECT user_id, course_id, label, timestamp
#         FROM click_sequences
#         WHERE course_id IN ({ph2})
#             AND timestamp <= %s
#         ORDER BY user_id, course_id, timestamp ASC
#     """, ts_course_ids + [as_of_date])
#     logger.info("Fetched %d click events", len(all_clicks))

#     sequences:     dict = defaultdict(list)
#     last_active:   dict = {}
#     weekly_counts: dict = defaultdict(lambda: defaultdict(int)) # (user_id, course_id) -> week -> count

#     for row in all_clicks:
#         key = (row["user_id"], row["course_id"])
#         sequences[key].append(row["label"])
#         ts = row["timestamp"]
#         if key not in last_active or ts > last_active[key]:
#             last_active[key] = ts
#         weekly_counts[key][ts.strftime("%Y-W%W")] += 1
    
#     course_breadths: dict = defaultdict(list)
#     student_breadth: dict = {}

#     for (uid, cid), seq in sequences.items():
#         breadth = len(set(seq))
#         course_breadths[cid].append(breadth)
#         student_breadth[(uid, cid)] = breadth

#     def z_score(value, population):
#         if len(population) < 2:
#             return 0.0
#         std = statistics.stdev(population)
#         return 0.0 if std == 0 else round((value - statistics.mean(population)) / std, 2)
    
#     rev_student = {v: k for k, v in student_map.items()}
#     rev_course  = {v: k for k, v in course_map.items()}

#     result = {}

#     for (uid, cid), breadth in student_breadth.items():
#         canvas_uid = rev_student.get(uid)
#         canvas_cid = rev_course.get(cid)
#         if canvas_uid is None or canvas_cid is None:
#             continue
#         wk_vals = sorted(weekly_counts[(uid, cid)].values())
#         la = last_active.get((uid, cid))
#         result[(canvas_uid, canvas_cid)] = {
#             "total_clicks": sum(wk_vals),
#             "click_slope": (wk_vals[-1] - wk_vals[-2]) if len(wk_vals) >= 2 else 0,
#             "last_active": str(la)[:10] if la else None,
#             "breadth": breadth,
#             "breadth_z": z_score(breadth, course_breadths[cid])
#         }
#     logger.info("Processed click signals for %d student-course pairs", len(result))
#     return result


# --- Stage 1c. Fetch model predictions (mydb) ---
def fetch_model_predictions(mydb_cfg: dict, canvas_course_ids: list, as_of_date: str) -> dict:
    """
    Reads the LATEST at-risk prediction per (student, course), regardless of when
    it was made. as_of_date is accepted for call-site compatibility but no longer
    used as a filter — predictions run hourly, and filtering to "<= as_of_date"
    (a bare date, i.e. midnight) was silently excluding same-day predictions,
    causing Chiron to act on stale data vs. what the dashboard shows.
    Returns dict keyed by (canvas_user_id, canvas_course_id).
    """
    ph = ",".join(["%s"] * len(canvas_course_ids))
    rows = _query(mydb_cfg, f"""
        SELECT DISTINCT ON (mp.student_id, c.canvas_course_id)
            s.canvas_user_id,
            c.canvas_course_id,
            mp.at_risk_predicted,
            mp.at_risk_probability
        FROM model_predictions mp
        JOIN students s ON s.id = mp.student_id
        JOIN courses c  ON c.id = mp.course_id
        WHERE c.canvas_course_id IN ({ph})
        ORDER BY mp.student_id, c.canvas_course_id, mp.prediction_timestamp DESC
    """, canvas_course_ids)
 
    result = {
        (row["canvas_user_id"], row["canvas_course_id"]): {
            "at_risk_predicted": bool(row["at_risk_predicted"]),
            "at_risk_probability": float(row["at_risk_probability"]) if row["at_risk_probability"] is not None else None,
        }
        for row in rows
    }
    logger.info("Fetched %d latest model prediction rows for %d courses (run as_of_date=%s, not used as a filter)",
                len(result), len(canvas_course_ids), as_of_date)
    return result

# --- Stage 2. Rule evaluation ---

# --- Rule thresholds ---
# "flagged" = ML layer already predicted at-risk -> fire the deterministic rules more sensitively. 
# Fixed/deterministic for now; RL-tuned later.

THRESHOLDS = {
    "default": {"low_engagement_z": -1.5, "low_engagement_min_dims": 2, "quiz_score_cutoff": 70.0, "missing_cutoff": 2},  # placeholder — tune me
    "flagged": {"low_engagement_z": -1.0, "low_engagement_min_dims": 2, "quiz_score_cutoff": 75.0, "missing_cutoff": 1},  # placeholder — tune me
}

def get_fired_rules(student: dict, click: dict | None, at_risk_flagged: bool = False) -> list:
    """
    Evaluate all rules and returns every category whose conditions are met.

    Deliberately does NOT short-circuit - every rule is checked independently
    so we can detect when multiple rules fire simultaneously.

    Return a list of 0-4 category strings 
    """
    t = THRESHOLDS["flagged"] if at_risk_flagged else THRESHOLDS["default"]

    grade = float(student["current_score"] or 0)
    quiz_score = float(student["quiz_score"]) if student["quiz_score"] is not None else None
    missing = int(student["missing_assignments"] or 0)
    bz = click["breadth_z"] if click else 0
    iz = click["intensity_z"] if click else 0
    cz = click["coherence_z"] if click else 0
    fired = []

    if click is None or click["total_clicks"] == 0:
        fired.append(CAT_NEVER_ATTENDED)

    if missing >= t["missing_cutoff"]:
        fired.append(CAT_MISSING_ASSIGNMENTS)
    
    # if bz is not None and bz <= -1.5:
    #     fired.append(CAT_LOW_ENGAGEMENT)
    low_dimensions = sum([bz <= t["low_engagement_z"], iz <= t["low_engagement_z"], cz <= t["low_engagement_z"]])
    if low_dimensions >= t["low_engagement_min_dims"]:
        fired.append(CAT_LOW_ENGAGEMENT)

    if quiz_score is not None and quiz_score < t["quiz_score_cutoff"]:
        fired.append(CAT_EXAM_PERFORMANCE)
    
    return fired

# The three "investigable" negative categories - only these trigger the agent
INVESTIGABLE = [CAT_MISSING_ASSIGNMENTS, CAT_LOW_ENGAGEMENT, CAT_EXAM_PERFORMANCE]

def categorize(student: dict, click: dict | None, at_risk_flagged: bool = False, research_mode: bool = False) -> str:
    """
    Returns (final category, all fired rules).

    Agent routing rules (research_mode=False, default — production, unchanged)::
      - CAT_NEVER_ATTENDED fires -> always deterministic
      - Both positives fire -> always deterministic
      - 2+ of the investigable negatives -> CAT_NEEDS_AGENT
      - anything else -> deterministic (1 rule fired)

    Additional conflict/ambiguous cases routed to the agent when research_mode=True
    (rules and the ML at-risk prediction disagree):
      - 0 investigable rules fired, but ML flagged the student at-risk
      - exactly 1 investigable rule fired, but ML did NOT flag the student at-risk
    NEVER_ATTENDED still always takes priority and is never a conflict case, even
    in research_mode.
    """

    fired = get_fired_rules(student, click, at_risk_flagged)
    grade = float(student["current_score"] or 0)

    # Never attended is always top priority if it fires
    if CAT_NEVER_ATTENDED in fired:
        return CAT_NEVER_ATTENDED, fired
    
    if len(fired) == 0:
        if research_mode and at_risk_flagged:
            return CAT_NEEDS_AGENT, fired # ML says at-risk but no deterministic rules fired # added 090826
        category = CAT_POSITIVE_EXCEPTIONAL if grade >= 85.0 else CAT_POSITIVE_SATISFACTORY
        return category, fired
    
    # count how many of the investigable negatives fired
    investigable_fired = [c for c in fired if c in INVESTIGABLE]

    if len(investigable_fired) >= 2:
        return CAT_NEEDS_AGENT, fired
    
    if research_mode and len(investigable_fired) == 1 and not at_risk_flagged:
        return CAT_NEEDS_AGENT, fired  # ML says not at-risk but 1 deterministic rule fired # added 090826
    
    # exactly one investigable rule fired -> deterministic category
    return fired[0], fired

# -------------- Output builders ----------------
CSV_FIELDS = [
    "canvas_user_id", "student_name", "nshe_id", "course_name", "canvas_course_id", "section_number",
    "category", "fired_rules", "current_grade", "quiz_score", "missing_assignments",
    "total_clicks", "click_coverage_z", "click_intensity_z", "click_coherence_z", "last_active",
]

XLSX_HEADERS = [
    "Canvas User ID", "Student Name", "NSHE ID", "Course Name", "Canvas Course ID", "Section Number",
    "Category", "Fired Rules", "Current Grade (%)", "Quiz Score (%)", "Missing Assignments",
    "Total Clicks", "Click Coverage Z-Score", "Click Intensity Z-Score", "Click Coherence Z-Score", "Last Active",
]

COL_WIDTHS = [16, 28, 14, 36, 18, 48, 40, 18, 18, 14, 12, 20, 20, 20, 14]

def build_csv_bytes(records: list) -> bytes:
    buf = io.StringIO()
    # serialize fired_rules as a pipe-separated string for csv
    flat = []
    for r in records:
        row = dict(r)
        row["fired_rules"] = "|".join(r.get("fired_rules") or [])
        flat.append(row)
    writer = csv.DictWriter(buf, fieldnames=CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(flat)
    return buf.getvalue().encode("utf-8")

def build_xlsx_bytes(records: list) -> bytes:
    wb = openpyxl.Workbook()

    def _write_sheet(ws, sheet_records):
        ws.append(XLSX_HEADERS)
        for cell in ws[1]:
            cell.font = Font(name="Arial", bold=True)
            cell.alignment = Alignment(horizontal="center")
        
        sorted_recs = sorted(
            sheet_records,
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
            ])
            ri = ws.max_row
            ws.cell(ri, 9).number_format = "0.0" # current_grade
            ws.cell(ri, 10).number_format = "0.0" # quiz_score
            ws.cell(ri, 13).number_format = "0.00" # click_coverage_z
            ws.cell(ri, 14).number_format = "0.00" # click_intensity_z
            ws.cell(ri, 15).number_format = "0.00" # click_coherence_z

        for i, w in enumerate(COL_WIDTHS, 1):
            ws.column_dimensions[get_column_letter(i)].width = w
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
    
    wb.remove(wb.active) # remove default sheet

    by_course = defaultdict(list)
    for rec in records:
        by_course[rec["course_name"]].append(rec)
    INVALID_SHEET_CHARS = r'[]:*?/\\'

    def _safe_sheet_title(name: str) -> str:
        cleaned = re.sub(f"[{re.escape(INVALID_SHEET_CHARS)}]", "-", name)
        return cleaned[:31] or "Sheet"

    for course_name in sorted(by_course):
        _write_sheet(wb.create_sheet(_safe_sheet_title(course_name)), by_course[course_name])
    
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

# --- S3 upload ---

def _upload(body: bytes, bucket: str, key: str, content_type: str) -> str:
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )
    url = f"s3://{bucket}/{key}"
    logger.info("Uploaded %d byte file to %s", len(body), url)
    return url

# --- Main entry point ---

def run(
    mydb_cfg: dict,
    timescale_cfg: dict,
    canvas_course_ids: list,
    s3_bucket: str,
    as_of_date: str,  # e.g., "2026-03-12"
    research_mode: bool = False
) -> dict:
    """
    Returns:
    {   
        "total": 123,
        "needs_agent": 45,
        "by_category": { ... },
        "agent_queue": [ { canvas_user_id, canvas_course_id, fired_rules  fired_rules_no_ml (research_mode only), at_risk_predicted, at_risk_probability }, ... ],
        "s3_csv": "s3://...",
        "s3_excel": "s3://...",
        "research_mode": false,
    }

    research_mode=False (default) is byte-identical to today's production behavior.
    research_mode=True widens agent-routing (see categorize()) to include conflict/
    ambiguous cases where the deterministic rules and the ML at-risk prediction
    disagree, and attaches each queued student's ML prediction to their agent_queue
    entry so attribution_handler.py doesn't need to re-fetch it.
    """
    now = datetime.utcnow()
    ts = now.strftime("%Y%m%d_%H%M%S")
    prefix = f"alerts/{now.strftime('%Y/%m/%d')}"

    roster = fetch_student_roster(mydb_cfg, canvas_course_ids, as_of_date)
    click_signals = fetch_click_signals(timescale_cfg, canvas_course_ids, as_of_date)
    model_predictions = fetch_model_predictions(mydb_cfg, canvas_course_ids, as_of_date)

    records = []
    agent_queue = []

    for student in roster:
        click = click_signals.get((student["canvas_user_id"], student["canvas_course_id"]))
        prediction = model_predictions.get((student["canvas_user_id"], student["canvas_course_id"]))
        at_risk_flagged = prediction["at_risk_predicted"] if prediction else False
        category, fired = categorize(student, click, at_risk_flagged, research_mode=research_mode)
        record = {
            "canvas_user_id": student["canvas_user_id"],
            "student_name": student["name"],
            "nshe_id": student["integration_id"],
            "course_name": student["course_name"],
            "canvas_course_id": student["canvas_course_id"],
            "section_number": student["section_number"],
            "category": category,
            "fired_rules": fired,
            "current_grade": float(student["current_score"]) if student["current_score"] else None,
            "quiz_score": float(student["quiz_score"]) if student.get("quiz_score") is not None else None,
            "missing_assignments": int(student["missing_assignments"] or 0),
            "total_clicks": click["total_clicks"] if click else 0,
            "click_coverage_z": click["breadth_z"] if click else None,
            "click_intensity_z": click["intensity_z"] if click else None,
            "click_coherence_z": click["coherence_z"] if click else None,
            "last_active": click["last_active"] if click else None,
        }
        records.append(record)

        if category == CAT_NEEDS_AGENT:
            entry ={
                "canvas_user_id": student["canvas_user_id"],
                "canvas_course_id": student["canvas_course_id"],
                "student_name": student["name"],
                "course_name": student["course_name"],
                "fired_rules": fired,
                "at_risk_predicted": prediction["at_risk_predicted"] if prediction else None,
                "at_risk_probability": prediction["at_risk_probability"] if prediction else None,
            }
            if research_mode:
                # Recompute with thresholds forced to "default" (unadjusted) — the AI-only
                # condition must never see a fired_rules list that was itself already
                # influenced by the ML flag via THRESHOLDS["flagged"]. Only relevant when
                # at_risk_flagged was actually True; otherwise this is identical to `fired`
                entry["fired_rules_no_ml"] = get_fired_rules(student, click, at_risk_flagged=False)
            agent_queue.append(entry)

    counts = Counter(r["category"] for r in records)
    logger.info("--- Summary: %d students ---", len(records))

    for cat in CATEGORY_ORDER:
        logger.info("Category '%-50s %d", cat, counts.get(cat, 0))
    
    s3_csv = _upload(build_csv_bytes(records), s3_bucket, 
                        f"{prefix}/alerts_{ts}.csv", "text/csv")
    s3_excel = _upload(build_xlsx_bytes(records), s3_bucket, 
                        f"{prefix}/alerts_{ts}.xlsx", 
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    return {
        "total": len(records),
        "needs_agent": len(agent_queue),
        "by_category": dict(counts),
        "agent_queue": agent_queue,
        "batch_timestamp": ts,
        "date_prefix": prefix,
        "s3_csv": s3_csv,
        "s3_excel": s3_excel,
        "research_mode": research_mode,
    }

