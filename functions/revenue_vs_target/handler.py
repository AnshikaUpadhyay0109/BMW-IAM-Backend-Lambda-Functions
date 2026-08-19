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
WITH target_months AS (
    SELECT YEAR(date_add('month', -2, CURRENT_DATE))  AS yr,
           MONTH(date_add('month', -2, CURRENT_DATE)) AS mo, 1 AS pos
    UNION ALL
    SELECT YEAR(date_add('month', -3, CURRENT_DATE)),
           MONTH(date_add('month', -3, CURRENT_DATE)), 2
    UNION ALL
    SELECT YEAR(date_add('month', -4, CURRENT_DATE)),
           MONTH(date_add('month', -4, CURRENT_DATE)), 3
),

sd_eff AS (
    SELECT
        dealer_code, year, month,
        purchase_actual_MTD, sales_actual_MTD,
        CASE
            WHEN purchase_target_MTD IS NOT NULL AND purchase_target_MTD > 0 THEN 'purchase'
            WHEN sales_target_MTD   IS NOT NULL AND sales_target_MTD   > 0 THEN 'sales'
            ELSE NULL
        END AS target_type,
        CASE
            WHEN purchase_target_MTD IS NOT NULL AND purchase_target_MTD > 0 THEN purchase_target_MTD
            WHEN sales_target_MTD   IS NOT NULL AND sales_target_MTD   > 0 THEN sales_target_MTD
            ELSE NULL
        END AS eff_target,
        CASE
            WHEN purchase_target_MTD IS NOT NULL AND purchase_target_MTD > 0 THEN purchase_actual_MTD
            WHEN sales_target_MTD   IS NOT NULL AND sales_target_MTD   > 0 THEN sales_actual_MTD
            ELSE purchase_actual_MTD
        END AS eff_actual
    FROM "dibmw-dev-sellout"."sellout_agg_dealer"
)
SELECT
    dm.dealer_code,
    dm.dealer_name,
    MAX(sd.target_type)                                        AS target_type,

    MAX(CASE WHEN tm.pos = 3 THEN sd.eff_actual END)           AS M3_Actual,
    MAX(CASE WHEN tm.pos = 3 THEN sd.eff_target END)           AS M3_Target,
    ROUND(
        MAX(CASE WHEN tm.pos = 3 THEN sd.eff_actual END) -
        MAX(CASE WHEN tm.pos = 3 THEN sd.eff_target END), 2
    )                                                           AS M3_Gap_EUR,
    ROUND(
        MAX(CASE WHEN tm.pos = 3 THEN sd.eff_actual END) /
        NULLIF(MAX(CASE WHEN tm.pos = 3 THEN sd.eff_target END), 0)
        * 100, 1
    )                                                           AS M3_AchvPct,

    MAX(CASE WHEN tm.pos = 2 THEN sd.eff_actual END)           AS M2_Actual,
    MAX(CASE WHEN tm.pos = 2 THEN sd.eff_target END)           AS M2_Target,
    ROUND(
        MAX(CASE WHEN tm.pos = 2 THEN sd.eff_actual END) -
        MAX(CASE WHEN tm.pos = 2 THEN sd.eff_target END), 2
    )                                                           AS M2_Gap_EUR,
    ROUND(
        MAX(CASE WHEN tm.pos = 2 THEN sd.eff_actual END) /
        NULLIF(MAX(CASE WHEN tm.pos = 2 THEN sd.eff_target END), 0)
        * 100, 1
    )                                                           AS M2_AchvPct,

    MAX(CASE WHEN tm.pos = 1 THEN sd.eff_actual END)           AS M1_Actual,
    MAX(CASE WHEN tm.pos = 1 THEN sd.eff_target END)           AS M1_Target,
    ROUND(
        MAX(CASE WHEN tm.pos = 1 THEN sd.eff_actual END) -
        MAX(CASE WHEN tm.pos = 1 THEN sd.eff_target END), 2
    )                                                           AS M1_Gap_EUR,
    ROUND(
        MAX(CASE WHEN tm.pos = 1 THEN sd.eff_actual END) /
        NULLIF(MAX(CASE WHEN tm.pos = 1 THEN sd.eff_target END), 0)
        * 100, 1
    )                                                           AS M1_AchvPct

FROM target_months tm
CROSS JOIN "dibmw-dev-sellout"."dealer_master" dm
LEFT JOIN sd_eff sd
    ON sd.dealer_code = dm.dealer_code
   AND sd.year = tm.yr
   AND sd.month = tm.mo
WHERE dm.dealer_code IN (
    '21125','11380','35955','33400',
    '40477','6057','30864','9118',
    '28965','33160'
)
GROUP BY dm.dealer_code, dm.dealer_name
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
    print(f"[revenue_vs_target] Query submitted → {execution_id}")

    for attempt in range(MAX_POLL_ATTEMPTS):
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        print(f"[revenue_vs_target] Attempt {attempt + 1}: state={state}, waiting…")
        time.sleep(POLL_INTERVAL_SEC)
    else:
        raise TimeoutError(f"Query {execution_id} did not complete within the poll limit.")

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena {state}: {reason}")

    return execution_id


def lambda_handler(event, context):
    log_event("revenue_vs_target", event)
    try:
        execution_id   = _run_athena_query(SQL_QUERY)
        output_s3_path = f"{S3_OUTPUT_LOCATION}{execution_id}.csv"
        print(f"[revenue_vs_target] SUCCESS → {output_s3_path}")
        return success_response({
            "query_name":         "revenue_vs_target",
            "query_execution_id": execution_id,
            "output_s3_path":     output_s3_path,
        })
    except Exception as exc:
        return error_response(f"revenue_vs_target failed: {exc}")
