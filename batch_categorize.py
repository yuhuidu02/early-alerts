"""
early_alerts/batch_categorize.py

Bulk categorization across one or more courses.

Categorization logic:
  - Evaluate all rules for each student simultaneously using the determinsitic rules
  - If exactly 1 rule fires -> assign that category directly (no agent)
  - If 2+ rules fire        -> flag for agent to decide best category
  - If 0 rules fire         -> Positive (satisfactory or exceptional)

Output written to S3:
  s3://<ALERTS_BUCKET>/alerts/YYYY/MM/DD/alerts_YYYYMMDD_HHMMSS.csv
  s3://<ALERTS_BUCKET>/alerts/YYYY/MM/DD/alerts_YYYYMMDD_HHMMSS.xlsx

"""

import csv
import io
import logging
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

def fetch_student_roster(mydb_cfg: dict, canvas_course_ids: list, as_of_date: str) -> list:
    """
    Fetches the most recent score snapshot per (student, course) on or before as_of_date.
    Uses student_score_snapshots instead of students table for historical accuracy.
    """
    ph = ",".join(["%s"] * len(canvas_course_ids))
    rows = _query(mydb_cfg, f"""
        SELECT DISTINCT ON (s.canvas_user_id, c.canvas_course_id)
            s.canvas_user_id,
            s.id            AS student_id,
            s.name,
            ss.missing_assignments,
            ss.current_score,
            c.canvas_course_id,
            c.id              AS course_id,
            c.name            AS course_name
        FROM students s
        JOIN student_courses sc ON sc.student_id = s.id
        JOIN courses c          ON c.id = sc.course_id
        JOIN student_score_snapshots ss ON ss.student_id = s.id AND ss.course_id = c.id
        WHERE c.canvas_course_id IN ({ph})
            AND ss.recorded_at <= %s
        ORDER BY s.canvas_user_id, c.canvas_course_id, ss.recorded_at DESC
    """, canvas_course_ids + [as_of_date])
    logger.info("Roster: %d student-course rows across %d courses (as of %s)", 
                len(rows), len(canvas_course_ids), as_of_date)
    return rows

# --- Stage 1b. Click signal (mytimescale) ---

def fetch_click_signals(timescale_cfg: dict, canvas_course_ids: list, as_of_date: str) -> dict:
    """
    Returns dict keyed by (canvas_user_id, canvas_course_id).
    All click data filtered to timestamp <= as_of_date.
    Z-scores computed against the course population up to that same date
    for a true historical snapshot.
    """
    ph = ",".join(["%s"] * len(canvas_course_ids))

    course_map = {
        r["canvas_course_id"]: r["id"]
        for r in _query(timescale_cfg,
                        f"SELECT canvas_course_id, id FROM courses WHERE canvas_course_id IN ({ph})",
                        canvas_course_ids)
    }
    if not course_map:
        logger.warning("No matching courses found in timescale for canvas_course_ids: %s", canvas_course_ids)
        return {}
    
    ts_course_ids = list(course_map.values())
    ph2 = ",".join(["%s"] * len(ts_course_ids))

    student_map = {
        r["canvas_user_id"]: r["id"]
        for r in _query(timescale_cfg, f"""
            SELECT DISTINCT s.canvas_user_id, s.id
            FROM students s
            JOIN click_sequences cs ON cs.user_id = s.id
            WHERE cs.course_id IN ({ph2})
            AND cs.timestamp <= %s
        """, ts_course_ids + [as_of_date])
    }
    
    logger.info("Fetching all click events up to %s for %d courses...", as_of_date, len(ts_course_ids))

    all_clicks = _query(timescale_cfg, f"""
        SELECT user_id, course_id, label, timestamp
        FROM click_sequences
        WHERE course_id IN ({ph2})
            AND timestamp <= %s
        ORDER BY user_id, course_id, timestamp ASC
    """, ts_course_ids + [as_of_date])
    logger.info("Fetched %d click events", len(all_clicks))

    sequences:     dict = defaultdict(list)
    last_active:   dict = {}
    weekly_counts: dict = defaultdict(lambda: defaultdict(int)) # (user_id, course_id) -> week -> count

    for row in all_clicks:
        key = (row["user_id"], row["course_id"])
        sequences[key].append(row["label"])
        ts = row["timestamp"]
        if key not in last_active or ts > last_active[key]:
            last_active[key] = ts
        weekly_counts[key][ts.strftime("%Y-W%W")] += 1
    
    course_breadths: dict = defaultdict(list)
    student_breadth: dict = {}

    for (uid, cid), seq in sequences.items():
        breadth = len(set(seq))
        course_breadths[cid].append(breadth)
        student_breadth[(uid, cid)] = breadth

    def z_score(value, population):
        if len(population) < 2:
            return 0.0
        std = statistics.stdev(population)
        return 0.0 if std == 0 else round((value - statistics.mean(population)) / std, 2)
    
    rev_student = {v: k for k, v in student_map.items()}
    rev_course  = {v: k for k, v in course_map.items()}

    result = {}

    for (uid, cid), breadth in student_breadth.items():
        canvas_uid = rev_student.get(uid)
        canvas_cid = rev_course.get(cid)
        if canvas_uid is None or canvas_cid is None:
            continue
        wk_vals = sorted(weekly_counts[(uid, cid)].values())
        la = last_active.get((uid, cid))
        result[(canvas_uid, canvas_cid)] = {
            "total_clicks": sum(wk_vals),
            "click_slope": (wk_vals[-1] - wk_vals[-2]) if len(wk_vals) >= 2 else 0,
            "last_active": str(la)[:10] if la else None,
            "breadth": breadth,
            "breadth_z": z_score(breadth, course_breadths[cid])
        }
    logger.info("Processed click signals for %d student-course pairs", len(result))
    return result

# --- Stage 2. Rule evaluation ---

def get_fired_rules(student: dict, click: dict | None) -> list:
    """
    Evaluate all rules and returns every category whose conditions are met.

    Deliberately does NOT short-circuit - every rule is checked independently
    so we can detect when multiple rules fire simultaneously.

    Return a list of 0-4 category strings 
    """
    grade = float(student["current_score"] or 0)
    missing = int(student["missing_assignments"] or 0)
    bz = click["breadth_z"] if click else 0
    fired = []
    if click is None or click["total_clicks"] == 0:
        fired.append(CAT_NEVER_ATTENDED)

    if missing >= 1:
        fired.append(CAT_MISSING_ASSIGNMENTS)
    
    if bz is not None and bz <= -1.5:
        fired.append(CAT_LOW_ENGAGEMENT)

    if grade < 60:
        fired.append(CAT_EXAM_PERFORMANCE)
    
    return fired

# The three "investigable" negative categories - only these trigger the agent
INVESTIGABLE = [CAT_MISSING_ASSIGNMENTS, CAT_LOW_ENGAGEMENT, CAT_EXAM_PERFORMANCE]

def categorize(student: dict, click: dict | None) -> str:
    """
    Returns (final category, all fired rules).

    Agent routing rules:
      - CAT_NEVER_ATTENDED fires -> always deterministic
      - Both positives fire -> always deterministic
      - 2+ of the investigable negatives -> CAT_NEEDS_AGENT
      - anything else -> deterministic (1 rule fired)
    """

    fired = get_fired_rules(student, click)
    grade = float(student["current_score"] or 0)

    # Never attended is always top priority if it fires
    if CAT_NEVER_ATTENDED in fired:
        return CAT_NEVER_ATTENDED, fired
    
    if len(fired) == 0:
        category = CAT_POSITIVE_EXCEPTIONAL if grade >= 85.0 else CAT_POSITIVE_SATISFACTORY
        return category, fired
    
    # count how many of the investigable negatives fired
    investigable_fired = [c for c in fired if c in INVESTIGABLE]

    if len(investigable_fired) >= 2:
        return CAT_NEEDS_AGENT, fired
    
    # exactly one investigable rule fired -> deterministic category
    return fired[0], fired

# -------------- Output builders ----------------
CSV_FIELDS = [
    "canvas_user_id", "student_name", "course_name", "canvas_course_id",
    "category", "fired_rules", "current_grade", "missing_assignments",
    "total_clicks", "breadth_z", "last_active",
]

XLSX_HEADERS = [
    "Canvas User ID", "Student Name", "Course Name", "Canvas Course ID",
    "Category", "Fired Rules", "Current Grade (%)", "Missing Assignments",
    "Total Clicks", "Breadth Z-Score", "Last Active",
]

COL_WIDTHS = [16, 28, 36, 18, 48, 40, 18, 22, 14, 16, 14]

def build_csv_bytes(records: list) -> bytes:
    buf = io.StringIO()
    # serialize fired_rules as a pipe-separated string for csv
    flat = []
    for r in records:
        row = dict(r)
        row["fired_rules"] = "|".join(r.get("fired_rules"), [])
        flat.append(row)
    writer = csv.DictWriter(buf, fieldnames=CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(flat)
    return buf.getvalue().encode("utf-8")

def build_xlsx_bytes(records: list) -> bytes:
    wb = openpyxl.Workbook()
    now = datetime.utcnow()

    # --- Summary sheet ---
    ws_sum = wb.active
    ws_sum.title = "Summary"    
    ws_sum["A1"] = "Early Alert Summary"
    ws_sum["A1"].font = Font(size=18, bold=True, name="Arial")
    ws_sum["A2"] = f"Generated on {now.strftime('%Y-%m-%d at %H:%M UTC')}"
    ws_sum["A2"].font = Font(size=10, italic=True, name="Arial")

    for col, label in enumerate(["Category", "Count", "% of Total"], 1):
        ws_sum.cell(4, col, label).font = Font(name="Arial", bold=True)

    counts = Counter(r["category"] for r in records)
    total = len(records)
    for i, cat in enumerate(CATEGORY_ORDER, 5):
        count = counts.get(cat, 0)
        ws_sum.cell(i, 1, cat)
        ws_sum.cell(i, 2, count)
        pct = ws_sum.cell(i, 3, count / total if total > 0 else 0)
        pct.number_format = "0.00%"
    
    ws_sum.column_dimensions["A"].width = 50
    ws_sum.column_dimensions["B"].width = 10
    ws_sum.column_dimensions["C"].width = 12

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
                rec["course_name"],
                rec["canvas_course_id"],
                rec["category"],
                fired_str,
                rec.get("current_score"),
                rec.get("missing_assignments"),
                rec.get("total_clicks"),
                rec.get("breadth_z"),
                rec.get("last_active"),
            ])
            ri = ws.max_row
            ws.cell(ri, 7).number_format = "0.0"
            ws.cell(ri, 10).number_format = "0.00"
        
        for i, w in enumerate(COL_WIDTHS, 1):
            ws.column_dimensions[get_column_letter(i)].width = w
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions
    
    _write_sheet(wb.create_sheet("All Students"), records)

    by_course = defaultdict(list)
    for rec in records:
        by_course[rec["course_name"]].append(rec)
    for course_name in sorted(by_course):
        _write_sheet(wb.create_sheet(course_name[:31]), by_course[course_name])
    
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
    as_of_date: str # e.g., "2026-03-12"
) -> dict:
    """
    Returns:
    {   
        "total": 123,
        "needs_agent": 45,
        "by_category": { ... },
        "agent_queue": [ { canvas_user_id, canvas_course_id, fired_rules }, ... ],
        "s3_csv": "s3://...",
        "s3_excel": "s3://..."
    }
    """
    now = datetime.utcnow()
    ts = now.strftime("%Y%m%d_%H%M%S")
    prefix = f"alerts/{now.strftime('%Y/%m/%d')}"

    roster = fetch_student_roster(mydb_cfg, canvas_course_ids, as_of_date)
    click_signals = fetch_click_signals(timescale_cfg, canvas_course_ids, as_of_date)

    records = []
    agent_queue = []

    for student in roster:
        click = click_signals.get((student["canvas_user_id"], student["canvas_course_id"]))
        category, fired = categorize(student, click)
        record = {
            "canvas_user_id": student["canvas_user_id"],
            "student_name": student["name"],
            "course_name": student["course_name"],
            "canvas_course_id": student["canvas_course_id"],
            "category": category,
            "fired_rules": fired,
            "current_grade": float(student["current_score"]) if student["current_score"] else None,
            "missing_assignments": int(student["missing_assignments"] or 0),
            "total_clicks": click["total_clicks"] if click else 0,
            "breadth_z": click["breadth_z"] if click else None,
            "last_active": click["last_active"] if click else None,
        }
        records.append(record)

        if category == CAT_NEEDS_AGENT:
            agent_queue.append({
                "canvas_user_id": student["canvas_user_id"],
                "canvas_course_id": student["canvas_course_id"],
                "student_name": student["name"],
                "course_name": student["course_name"],
                "fired_rules": fired,
            })

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
    }

