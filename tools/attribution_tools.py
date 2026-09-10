"""
tools/attribution_tools.py

A self-contained toolkit for the four Downstream attribution tools.

Usage - attach to any Strand agent:
    
    from tools.attribution_tools import AttributionToolkit

    toolkit = AttributionToolkit(mydb_cfg, timescale_cfg, as_of_date="2026-02-12")

    agent = Agent(
        model=my_model,
        tools=toolkit.all(),   # provides all 4 tools   
        system_prompt=SYSTEM_PROMPT,  # provides system prompt for context
    )

    # Or pick individual tools:
    agent = Agent(model=model, tools=[toolkit.get_missing_assignments])

    # Or call a tool directly (no agent needed):
    result = toolkit.get_missing_assignments(student_id=2564)

"""

import json
import statistics
from collections import defaultdict
from typing import Any

import psycopg2
import psycopg2.extras
from strands import tool
from datetime import date, timedelta

class AttributionToolkit:
    """
    Wraps the four Downstream attribution tools

    Each tool method is decorated with @tool so Strands can register it
    automatically. Because they live on an instance, they close over
    `self` (and therefore the DB configs) instead of free variables - 
    making them injectable, testable, and reusable across agents.
    """

    def __init__(self, mydb_cfg: dict, timescale_cfg: dict, as_of_date: str):
        self.mydb_cfg = mydb_cfg
        self.timescale_cfg = timescale_cfg
        self.as_of_date = as_of_date

        # Bind the @tool-decorated methods to this instance so Strands see plain callables.
        # (Strands inspects __name__ and __doc__, both of which are preserved 
        # by the @toll decorator on the inner functions below.)
        self.get_construct_scores_and_slopes = self._make_construct_tool()
        self.get_missing_assignments = self._make_missing_assignments_tool()
        self.get_withdrawal_and_support = self._make_withdrawal_tool()
        self.get_click_activity = self._make_click_tool()

    # --- Public API ---
    
    def all(self) -> list:
        """Return all tools as a list - pass directly to Agent(tools=...)"""
        return [
            self.get_construct_scores_and_slopes,
            self.get_missing_assignments,
            self.get_withdrawal_and_support,
            self.get_click_activity,
        ]
    

    # --- Private DB helpers ---

    def _query(self, cfg: dict, sql: str, params=None) -> list[dict]:
         with psycopg2.connect(**cfg) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(r) for r in cur.fetchall()]
    
    def _query_mydb(self, sql: str, params=None) -> list[dict]:
        return self._query(self.mydb_cfg, sql, params)
    
    def _query_timescale(self, sql: str, params=None) -> list[dict]:
        return self._query(self.timescale_cfg, sql, params)
    
    # --- Tool factories ---
    # Each _make_*() method defines and returns one @tool-decorated function.
    # The function closes over `self`, giving it DB access without globals.

    def _make_construct_tool(self):

        toolkit = self

        @tool
        def get_construct_scores_and_slopes(student_id: int, course_id: int) -> str:
            """
            Retrieves survey construct score history for a student in a course,
            plus the slope (latest minus previous) for each construct.
 
            Constructs and their scoring:
            - con   (Confidence):  1-6, higher is better. Low score or downward slope = warning.
            - sth  (Study Habits): 1-6, higher is better. Low score or downward slope = warning.
            - mot (Motivation):    1-6, higher is better. Low score or downward slope = warning.
            - abur  (Stress):      1-6, higher means MORE stressed. High score or upward slope = warning.
            - res  (Resilience):   1-6, lower means LESS resilient. Low score or downward slope = warning.
 
            Args:
                student_id (int): The student's internal mydb id.
                course_id (int): The course's internal mydb id.
 
            Returns:
                str: JSON with score history per submission and slope per construct.
            """
            rows = toolkit._query_mydb("""
                SELECT
                    qs.submitted_at,
                    qz.title AS quiz_title,
                    c.code AS construct_code,
                    c.name AS construct_name,
                    ROUND(AVG(qsc.score), 2) AS avg_score
                FROM quiz_submissions qs
                JOIN quizzes qz ON qs.quiz_id = qz.id
                JOIN question_scores qsc ON qsc.submission_id = qs.id
                JOIN questions q ON qsc.question_id = q.id
                JOIN constructs c ON q.construct_id = c.id
                WHERE qs.user_id = %s
                  AND qz.course_id = %s
                  AND qs.submitted_at IS NOT NULL
                  AND qs.submitted_at <= %s
                  AND c.code IN ('con', 'sth', 'mot', 'abur', 'res')
                GROUP BY qs.submitted_at, qz.title, c.code, c.name
                ORDER BY qs.submitted_at ASC, c.code
            """, (student_id, course_id, toolkit.as_of_date))

            if not rows:
                return json.dumps({"error": "No quiz submissions found for this student and course."})
            
            submissions: dict[str, Any] = defaultdict(dict)
            for row in rows:
                key = str(row["submitted_at"])[:10]  # date only
                submissions[key]["submitted_at"] = key
                submissions[key]["quiz"] = row["quiz_title"]
                submissions[key][row["construct_code"]] = float(row["avg_score"])

            history = list(submissions.values())

            construct_codes = ['con', 'sth', 'mot', 'abur', 'res']
            if len(history) >= 2:
                latest, previous = history[-1], history[-2]
                slopes = {
                    code: {
                        "previous": previous[code],
                        "latest": latest[code],
                        "slope": round(latest[code] - previous[code], 2),
                        "direction": ("up" if latest[code] > previous[code] else
                                      "down" if latest[code] < previous[code] else "flat")
                    }
                    for code in construct_codes
                    if code in previous and code in latest
                }
            else:
                slopes = {"note": "Not enough submissions to calculate slopes."}
            
            return json.dumps({
                "history": history,
                "slopes": slopes,
                "scoring_guide": {
                    "con":  {"name": "Confidence",   "direction": "higher_is_better", "warning": "low score or downward slope"},
                    "sth":  {"name": "Study Habits", "direction": "higher_is_better", "warning": "low score or downward slope"},
                    "mot":  {"name": "Motivation",   "direction": "higher_is_better", "warning": "low score or downward slope"},
                    "abur": {"name": "Stress",       "direction": "higher_is_worse",  "warning": "high score or upward slope"},
                    "res":  {"name": "Resilience",   "direction": "lower_is_worse",   "warning": "low score or downward slope"},
                },
            }, indent=2)
        
        return get_construct_scores_and_slopes
    
    def _make_missing_assignments_tool(self):

        toolkit = self

        @tool
        def get_missing_assignments(student_id: int) -> str:
            """
            Retrieves the full snapshot history of a student's missing assignment count
            and quiz score (Chiron survey performance) up to as_of_date, ordered oldest
            to newest. Use the trend — not just the latest value — to assess whether
            the issue is worsening, improving, or chronic.
 
            Args:
                student_id (int): The student's internal mydb id.
 
            Returns:
                str: JSON with student name, latest values, and full weekly history
                     of missing_assignments and quiz_score.
            """
            rows = toolkit._query_mydb("""
                SELECT
                    s.name,
                    ss.missing_assignments,
                    ss.current_score,
                    ss.quiz_score,
                    ss.recorded_at
                FROM students s
                JOIN student_score_snapshots ss ON ss.student_id = s.id
                WHERE s.id = %s
                  AND ss.recorded_at <= %s
                ORDER BY ss.recorded_at ASC
            """, (student_id, toolkit.as_of_date))

            if not rows:
                return json.dumps({"error": "Student not found"})
            
            history = [
                {
                    "recorded_at": str(row["recorded_at"])[:10],
                    "missing_assignments": row["missing_assignments"],
                    "quiz_score": float(row["quiz_score"]) if row["quiz_score"] is not None else None,
                }
                for row in rows
            ]
 
            latest = rows[-1]  # oldest record is last due to DESC order
            return json.dumps({
                "student_name":        latest["name"],
                "latest_missing_assignments": latest["missing_assignments"],
                "latest_quiz_score":          float(latest["quiz_score"]) if latest["quiz_score"] is not None else None,
                "latest_current_grade":       float(latest["current_score"]) if latest["current_score"] is not None else None,
                "history":                    history,
            }, indent=2)

        return get_missing_assignments

    def _make_withdrawal_tool(self):
        toolkit = self

        @tool
        def get_withdrawal_and_support(student_id: int, course_id: int) -> str:
            """
            Retrieves the student's withdrawal intention (with_v2) and support
            requested (supp) responses across survey submissions in the course.
            Score interpretation: 1 = No, 2 = Sometimes, 3 = Yes.
 
            Args:
                student_id (int): The student's internal mydb id.
                course_id (int): The course's internal mydb id.
 
            Returns:
                str: JSON with withdrawal and support flag history.
            """
            rows = toolkit._query_mydb("""
                SELECT
                    qs.submitted_at,
                    qz.title AS quiz_title,
                    q.code   AS question_code,
                    qsc.score
                FROM quiz_submissions qs
                JOIN quizzes qz ON qs.quiz_id = qz.id
                JOIN question_scores qsc ON qsc.submission_id = qs.id
                JOIN questions q ON qsc.question_id = q.id
                WHERE qs.user_id = %s
                  AND qz.course_id = %s
                  AND qs.submitted_at IS NOT NULL
                  AND qs.submitted_at <= %s
                  AND q.code IN ('with_v2', 'supp')
                ORDER BY qs.submitted_at ASC, q.code
            """, (student_id, course_id, toolkit.as_of_date))

            if not rows:
                return json.dumps({"error": "No quiz submissions found for this student and course."})
            
            label_map = {1: "No", 2: "Sometimes", 3: "Yes"}
            history = [
                {
                    "submitted_at":  str(row["submitted_at"])[:10],
                    "quiz":          row["quiz_title"],
                    "question_code": row["question_code"],
                    "response":      label_map.get(row["score"], str(row["score"])),
                    "raw_score":     row["score"],
                }
                for row in rows
            ]

            latest_with = [r for r in history if r["question_code"] == "with_v2"]
            latest_supp = [r for r in history if r["question_code"] == "supp"]

            flags: dict[str, Any] = {}
            if latest_with:
                flags["withdrawal_latest"] = latest_with[-1]["response"]
                flags["withdrawal_risk"] = latest_with[-1]["raw_score"] == 3
            if latest_supp:
                flags["support_latest"] = latest_supp[-1]["response"]
                flags["support_requested"] = latest_supp[-1]["raw_score"] == 3

            return json.dumps({"flags": flags, "history": history}, indent=2)

        return get_withdrawal_and_support
    
    def _make_click_tool(self):
        toolkit = self

        @tool
        def get_click_activity(click_student_id: int, click_course_id: int) -> str:
            """
            Retrieves click activity summary for a student in a course from TimescaleDB.
            Returns weekly click volume, last active date, and behavioral features
            (breadth and depth) with z-scores relative to the course population.
 
            breadth: number of unique pages visited.
            depth: proportion of possible page-to-page transitions observed.
            z-score > 0 = above course average; < 0 = below; < -1.0 = warning sign.
 
            Args:
                click_student_id (int): The student's internal timescale id.
                click_course_id (int): The course's internal timescale id.
 
            Returns:
                str: JSON with weekly clicks, behavioral features, and z-scores.
            """
            weekly = toolkit._query_timescale("""
                SELECT DATE_TRUNC('week', timestamp) AS week_start, COUNT(*) AS click_count
                FROM click_sequences
                WHERE user_id = %s AND course_id = %s AND timestamp <= %s
                GROUP BY week_start
                ORDER BY week_start ASC
            """, (click_student_id, click_course_id, toolkit.as_of_date))

            last_active_row = toolkit._query_timescale("""
                SELECT MAX(timestamp) AS last_active
                FROM click_sequences
                WHERE user_id = %s AND course_id = %s AND timestamp <= %s
            """, (click_student_id, click_course_id, toolkit.as_of_date))

            sequence_rows = toolkit._query_timescale("""
                SELECT label FROM click_sequences
                WHERE user_id = %s AND course_id = %s AND timestamp <= %s
                ORDER BY timestamp ASC
            """, (click_student_id, click_course_id, toolkit.as_of_date))

            if not sequence_rows:
                return json.dumps({"error": "No click data found for this student and course."})
            
            def compute_features(seq: list[str]) -> tuple[int, float]:
                unique = list(set(seq))
                n = len(unique)
                if n == 0:
                    return 0, 0.0
                idx = {s: i for i, s in enumerate(unique)}
                adj = [[0] * n for _ in range(n)]
                for i in range(len(seq) - 1):
                    adj[idx[seq[i]]][idx[seq[i + 1]]] = 1 # or += 1
                edges = sum(adj[i][j] for i in range(n) for j in range(n))
                return n, round(edges / (n * n), 4)
            
            def z_score(value: float, population: list[float]) -> float | None:
                if len(population) < 2:
                    return None
                std = statistics.stdev(population)
                return 0.0 if std == 0 else round((value - statistics.mean(population)) / std, 2)
            
            student_seq = [r["label"] for r in sequence_rows]
            s_breadth, s_depth = compute_features(student_seq)

            all_students = toolkit._query_timescale("""
                SELECT DISTINCT user_id FROM click_sequences
                WHERE course_id = %s AND timestamp <= %s
            """, (click_course_id, toolkit.as_of_date))

            all_breadths, all_depths = [], []
            for row in all_students:
                rows = toolkit._query_timescale("""
                    SELECT label FROM click_sequences
                    WHERE user_id = %s AND course_id = %s AND timestamp <= %s
                    ORDER BY timestamp ASC
                """, (row["user_id"], click_course_id, toolkit.as_of_date))
                if rows:
                    b, d = compute_features([r["label"] for r in rows])
                    all_breadths.append(b)
                    all_depths.append(d)
            
            weekly_data = [
                {"week": str(r["week_start"])[:10], "clicks": r["click_count"]}
                for r in weekly
            ]

            # Flag the current (possibly still-in-progress) week so it's never
            # silently compared to a full prior week.
            as_of = date.fromisoformat(str(toolkit.as_of_date)[:10])
            current_week_start = as_of - timedelta(days=as_of.weekday())
            for w in weekly_data:
                week_start_date = date.fromisoformat(w["week"])
                if week_start_date == current_week_start:
                    w["partial_week"] = True
                    w["days_elapsed"] = (as_of - week_start_date).days + 1
                else:
                    w["partial_week"] = False

            # Slope computed only between the two most recent COMPLETE weeks —
            # a partial in-progress week is excluded, since it would always
            # look like a decline regardless of actual behavior.
            complete_weeks = [w for w in weekly_data if not w["partial_week"]]
            click_slope = (
                complete_weeks[-1]["clicks"] - complete_weeks[-2]["clicks"]
                if len(complete_weeks) >= 2 else None
            )
            last_active_val = str(last_active_row[0]["last_active"]) if last_active_row else None

            return json.dumps({
                "weekly_clicks": weekly_data,
                "click_slope": click_slope,
                "click_slope_note": "Computed between the two most recent COMPLETE weeks only. A week marked partial_week=true is excluded — don't compare its click count to a full week's.",
                "last_active": str(last_active_val)[:10] if last_active_val else None,
                "behavioral_features": {
                    "breadth": {
                        "value": s_breadth,
                        "course_mean": round(statistics.mean(all_breadths), 2),
                        "course_stdev": round(statistics.stdev(all_breadths), 2) if len(all_breadths) >= 2 else None,
                        "z_score": z_score(s_breadth, all_breadths),
                        "interpretation": "unique pages visited — negative z-score means below course average",
                    },
                    "depth": {
                        "value": s_depth,
                        "course_mean": round(statistics.mean(all_depths), 2),
                        "course_stdev": round(statistics.stdev(all_depths), 2) if len(all_depths) >= 2 else None,   
                        "z_score": z_score(s_depth, all_depths),
                        "interpretation": "navigation variety — negative z-score means more repetitive than peers",
                    }
                }
            }, indent=2)
        return get_click_activity
