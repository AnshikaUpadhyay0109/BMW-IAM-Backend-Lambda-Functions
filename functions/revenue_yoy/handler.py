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
WITH latest_month AS (
    SELECT MAX("month") AS cy_month
    FROM "dibmw-dev-sellout"."sellout_agg_dealer"
    WHERE year = EXTRACT(YEAR FROM current_date)
      AND sales_actual_MTD IS NOT NULL
)

SELECT
    sd.dealer_code,
    MAX(dm.dealer_name) AS dealer_name,

    ROUND(
        SUM(
            CASE
                WHEN sd.year = EXTRACT(YEAR FROM current_date)
                 AND sd."month" = lm.cy_month
                THEN COALESCE(sd.sales_actual_MTD, 0)
                ELSE 0
            END
        ),
        0
    ) AS cy_revenue_eur,

    ROUND(
        SUM(
            CASE
                WHEN sd.year = EXTRACT(YEAR FROM current_date) - 1
                 AND sd."month" = lm.cy_month
                THEN COALESCE(sd.sales_actual_MTD, 0)
                ELSE 0
            END
        ),
        0
    ) AS ly_revenue_eur,

    lm.cy_month AS lat_month

FROM "dibmw-dev-sellout"."sellout_agg_dealer" sd
LEFT JOIN "dibmw-dev-sellout"."dealer_master" dm
    ON sd.dealer_code = dm.dealer_code
CROSS JOIN latest_month lm
WHERE sd.dealer_code IN (
    '21125','11380','35955','33400',
    '40477','6057','30864','9118',
    '28965','33160'
)
GROUP BY
    sd.dealer_code,
    lm.cy_month

ORDER BY cy_revenue_eur DESC;
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
    print(f"[revenue_yoy] Query submitted → {execution_id}")

    for attempt in range(MAX_POLL_ATTEMPTS):
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        print(f"[revenue_yoy] Attempt {attempt + 1}: state={state}, waiting…")
        time.sleep(POLL_INTERVAL_SEC)
    else:
        raise TimeoutError(f"Query {execution_id} did not complete within the poll limit.")

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena {state}: {reason}")

    return execution_id


def lambda_handler(event, context):
    log_event("revenue_yoy", event)
    try:
        execution_id   = _run_athena_query(SQL_QUERY)
        output_s3_path = f"{S3_OUTPUT_LOCATION}{execution_id}.csv"
        print(f"[revenue_yoy] SUCCESS → {output_s3_path}")
        return success_response({
            "query_name":         "revenue_yoy",
            "query_execution_id": execution_id,
            "output_s3_path":     output_s3_path,
        })
    except Exception as exc:
        return error_response(f"revenue_yoy failed: {exc}")
