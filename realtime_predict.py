"""
early_alerts/realtime_predict.py

Calls predict-at-risk-realtime once per (student, course) in parallel and returns
predictions in the same shape as batch_categorize.fetch_model_predictions().
"""
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config

logger = logging.getLogger(__name__)

FUNCTION_NAME = "predict-at-risk-realtime"
MAX_WORKERS = 10  # each invoke opens its own DB connections; keep modest

# No retries: the Lambda inserts a model_predictions row per call, so a retry = duplicate row
_lambda = boto3.client(
    "lambda",
    config=Config(read_timeout=120, connect_timeout=10, retries={"max_attempts": 0}),
)


def _predict_one(canvas_user_id, canvas_course_id, event_id) -> dict:
    resp = _lambda.invoke(
        FunctionName=FUNCTION_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps({
            "canvas_user_id": int(canvas_user_id),
            "canvas_course_id": int(canvas_course_id),
            "event_id": event_id,
        }).encode(),
    )
    body = json.loads(resp["Payload"].read())
    if resp.get("FunctionError"):
        raise RuntimeError(body)
    if isinstance(body.get("body"), str):  # Function-URL-style envelope
        body = json.loads(body["body"])
    return {
        "at_risk_predicted": bool(body["at_risk_predicted"]),
        "at_risk_probability": float(body["at_risk_probability"])
            if body.get("at_risk_probability") is not None else None,
    }


def predict_roster(roster: list, run_ts: str) -> dict:
    """
    Returns {(canvas_user_id, canvas_course_id): {at_risk_predicted, at_risk_probability}}.
    Keys use the roster's own values so lookups in run() match exactly.
    Failed students are omitted (caller falls back to the DB).
    """
    keys = {(s["canvas_user_id"], s["canvas_course_id"]) for s in roster}
    results, failed = {}, 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_predict_one, uid, cid, f"chiron-{run_ts}-{cid}-{uid}"): (uid, cid)
            for uid, cid in keys
        }
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                results[key] = fut.result()
            except Exception as e:
                failed += 1
                logger.warning("Realtime prediction failed for %s: %s", key, e)

    logger.info("Realtime predictions: %d ok, %d failed of %d", len(results), failed, len(keys))
    return results