import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def success_response(data: dict, status_code: int = 200) -> dict:
    return {"statusCode": status_code, "body": json.dumps(data)}


def error_response(message: str, status_code: int = 500) -> dict:
    logger.error(message)
    return {"statusCode": status_code, "body": json.dumps({"error": message})}


def parse_body(event: dict) -> dict:
    body = event.get("body", event)
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}
    return body if isinstance(body, dict) else {}


def log_event(fn_name: str, event: dict):
    safe = {k: v for k, v in event.items() if k not in ("password", "secret", "token")}
    logger.info(f"[{fn_name}] Received event: {json.dumps(safe)}")
