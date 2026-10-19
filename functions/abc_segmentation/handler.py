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
WITH ytd_sales AS (
    SELECT
        sd.dealer_code,
        max(dm.region) as region,
        max(dm.dealer_city) as dealer_city,
        MAX(dm.dealer_name) AS dealer_name,
        MAX(dm.country) AS country,
        MAX(sd.sales_actual_YTD) AS ytd_sales
    FROM "dibmw-dev-sellout"."sellout_agg_dealer" sd
    LEFT JOIN "dibmw-dev-sellout"."dealer_master" dm
        ON sd.dealer_code = dm.dealer_code
    WHERE sd.year = YEAR(current_date)
      AND sd.sales_actual_YTD IS NOT NULL
    GROUP BY sd.dealer_code
)
SELECT
    dealer_code,
    dealer_name,
    region,
    country,
    dealer_city,
    ROUND(ytd_sales, 0) AS ytd_sales_eur,
    NTILE(3) OVER (ORDER BY ytd_sales DESC) AS quantile,
    CASE NTILE(3) OVER (ORDER BY ytd_sales DESC)
        WHEN 1 THEN 'A'
        WHEN 2 THEN 'B'
        WHEN 3 THEN 'C'
    END AS segment
FROM ytd_sales
WHERE region IN ('UK IR')
ORDER BY ytd_sales DESC;
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
    print(f"[abc_segmentation] Query submitted → {execution_id}")

    for attempt in range(MAX_POLL_ATTEMPTS):
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        print(f"[abc_segmentation] Attempt {attempt + 1}: state={state}, waiting…")
        time.sleep(POLL_INTERVAL_SEC)
    else:
        raise TimeoutError(f"Query {execution_id} did not complete within the poll limit.")

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena {state}: {reason}")

    return execution_id


def lambda_handler(event, context):
    log_event("abc_segmentation", event)
    try:
        execution_id   = _run_athena_query(SQL_QUERY)
        output_s3_path = f"{S3_OUTPUT_LOCATION}{execution_id}.csv"
        print(f"[abc_segmentation] SUCCESS → {output_s3_path}")
        return success_response({
            "query_name":         "abc_segmentation",
            "query_execution_id": execution_id,
            "output_s3_path":     output_s3_path,
        })
    except Exception as exc:
        return error_response(f"abc_segmentation failed: {exc}")
