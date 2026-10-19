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
SELECT
    country,
    dealer_code,
    dealer_master_name,
    dealer_city,
    district_area,
    region,
    antecedent_category,
    below_benchmark_category,
    country_confidence_pct,
    dealer_confidence_pct,
    confidence_gap_pct,
    country_antecedent_baskets,
    country_joint_baskets,
    dealer_antecedent_baskets,
    dealer_joint_baskets,
    analysis_period_start,
    analysis_period_end_exclusive
FROM "dibmw-dev-sellout".market_basket_below_benchmark_uk_ir
ORDER BY dealer_code, confidence_gap_pct DESC
"""

athena = boto3.client("athena", region_name=ATHENA_REGION)


def _run_athena_query(sql: str) -> str:
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        WorkGroup=os.environ.get("ATHENA_WORKGROUP", "primary"),
    )
    execution_id = response["QueryExecutionId"]
    print(f"[market_basket_uk_ir] Query submitted → {execution_id}")

    for attempt in range(MAX_POLL_ATTEMPTS):
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        print(f"[market_basket_uk_ir] Attempt {attempt + 1}: state={state}, waiting…")
        time.sleep(POLL_INTERVAL_SEC)
    else:
        raise TimeoutError(f"Query {execution_id} did not complete within the poll limit.")

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena {state}: {reason}")

    return execution_id


def lambda_handler(event, context):
    log_event("market_basket_uk_ir", event)
    try:
        execution_id   = _run_athena_query(SQL_QUERY)
        output_s3_path = f"{S3_OUTPUT_LOCATION}{execution_id}.csv"
        print(f"[market_basket_uk_ir] SUCCESS → {output_s3_path}")
        return success_response({
            "query_name":         "market_basket_uk_ir",
            "query_execution_id": execution_id,
            "output_s3_path":     output_s3_path,
        })
    except Exception as exc:
        return error_response(f"market_basket_uk_ir failed: {exc}")
