# shared/utils.py
# Reusable helpers — imported by any Lambda function via a Lambda Layer

import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ── Response Helpers ──────────────────────────────────────────────────────────

def success_response(data: dict, status_code: int = 200) -> dict:
    """Standard success envelope returned by every Lambda."""
    return {
        "statusCode": status_code,
        "body": json.dumps(data)
    }


def error_response(message: str, status_code: int = 500) -> dict:
    """Standard error envelope returned by every Lambda."""
    logger.error(message)
    return {
        "statusCode": status_code,
        "body": json.dumps({"error": message})
    }


# ── Event Parsing ─────────────────────────────────────────────────────────────

def parse_body(event: dict) -> dict:
    """
    Safely parse the body from an API Gateway event or a direct Lambda invoke.
    Returns an empty dict if body is missing or malformed.
    """
    body = event.get("body", event)   # direct invoke passes dict; API GW passes string
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}
    return body if isinstance(body, dict) else {}


# ── Logging ───────────────────────────────────────────────────────────────────

def log_event(fn_name: str, event: dict):
    """Log the incoming event (omit sensitive keys in production)."""
    safe = {k: v for k, v in event.items() if k not in ("password", "secret", "token")}
    logger.info(f"[{fn_name}] Received event: {json.dumps(safe)}")