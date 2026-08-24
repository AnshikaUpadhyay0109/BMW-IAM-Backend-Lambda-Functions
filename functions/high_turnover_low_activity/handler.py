import boto3
import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))
from shared.config import (
    ATHENA_DATABASE, ATHENA_REGION,
    S3_OUTPUT_LOCATION, POLL_INTERVAL_SEC, MAX_POLL_ATTEMPTS,
)
from shared.utils import success_response, error_response, log_event

SQL_QUERY = """
WITH base AS (
    SELECT
        country,
        dealer_code AS entity_id,
        SUM(net_amount)                AS turnover,
        COUNT(DISTINCT invoice_number) AS invoice_count,
        DATE_DIFF('day', MAX(invoice_date), DATE '2026-06-30') AS recency_days
    FROM "dibmw-dev-sellout"."sellout_view"
    WHERE year = 2026
      AND month IN (4, 5, 6)
      AND (
            (country = 'FR' AND dealer_code IN ('21125','11380','35955','33400')) OR
            (country = 'GR' AND dealer_code IN ('40477','6057','30864','9118'))   OR
            (country = 'PL' AND dealer_code IN ('28965','33160'))
          )
    GROUP BY country, dealer_code
),
market_avg AS (
    SELECT country, AVG(turnover) AS avg_turnover
    FROM base
    GROUP BY country
)
SELECT
    b.*,
    ma.avg_turnover,
    (b.turnover > ma.avg_turnover)                     AS high_turnover,
    (b.invoice_count < 2 OR b.recency_days > 30)       AS low_activity,
    (b.turnover > ma.avg_turnover
     AND (b.invoice_count < 2 OR b.recency_days > 30)) AS high_turnover_low_activity_flag
FROM base b
JOIN market_avg ma ON b.country = ma.country
ORDER BY b.country, b.turnover DESC
"""

athena = boto3.client("athena", region_name=ATHENA_REGION)


def _run_athena_query(sql: str) -> str:
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": S3_OUTPUT_LOCATION},
        WorkGroup=os.environ.get("ATHENA_WORKGROUP", "primary"),
    )
    execution_id = response["QueryExecutionId"]
    print(f"[high_turnover_low_activity] Query submitted → {execution_id}")

    for attempt in range(MAX_POLL_ATTEMPTS):
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        print(f"[high_turnover_low_activity] Attempt {attempt + 1}: state={state}, waiting…")
        time.sleep(POLL_INTERVAL_SEC)
    else:
        raise TimeoutError(f"Query {execution_id} did not complete within the poll limit.")

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena {state}: {reason}")

    return execution_id


def lambda_handler(event, context):
    log_event("high_turnover_low_activity", event)
    try:
        execution_id   = _run_athena_query(SQL_QUERY)
        output_s3_path = f"{S3_OUTPUT_LOCATION}{execution_id}.csv"
        print(f"[high_turnover_low_activity] SUCCESS → {output_s3_path}")
        return success_response({
            "query_name":         "high_turnover_low_activity",
            "query_execution_id": execution_id,
            "output_s3_path":     output_s3_path,
        })
    except Exception as exc:
        return error_response(f"high_turnover_low_activity failed: {exc}")
