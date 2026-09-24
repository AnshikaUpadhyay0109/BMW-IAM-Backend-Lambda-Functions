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
WITH date_params AS (
    SELECT
          month(date_add('month', -1, current_date)) AS rpt_month
        , year(date_add('month', -1, current_date)) AS rpt_year
),

customer_counts AS (
    SELECT
          s.dealer_code
        , COUNT(
            DISTINCT CASE
                WHEN month(s.invoice_date) = d.rpt_month
                 AND year(s.invoice_date) = d.rpt_year
                THEN s.customer_code
            END
          ) AS current_month_customer_count

        , COUNT(
            DISTINCT CASE
                WHEN month(s.invoice_date) = d.rpt_month
                 AND year(s.invoice_date) = d.rpt_year - 1
                THEN s.customer_code
            END
          ) AS last_year_same_month_customer_count

    FROM "dibmw-dev-sellout"."sellout_view" s
    CROSS JOIN date_params d
    GROUP BY s.dealer_code
),

base_data AS (
    SELECT DISTINCT
          c.dealer_code
        , s.country
        , s.dealer_master_country
        , s.region
        , s.dealer_city
        , s.district_area
        , c.current_month_customer_count
        , c.last_year_same_month_customer_count

        , ROUND(
            CASE
                WHEN c.last_year_same_month_customer_count > 0
                THEN
                    100.0 *
                    (
                        c.current_month_customer_count
                        - c.last_year_same_month_customer_count
                    )
                    / c.last_year_same_month_customer_count
            END
          , 2) AS growth_degrowth_pct

    FROM customer_counts c
    LEFT JOIN "dibmw-dev-sellout"."sellout_view" s
        ON s.dealer_code = c.dealer_code
),

country_ranking AS (
    SELECT
          *
        , NTILE(3) OVER (
              PARTITION BY country
              ORDER BY COALESCE(growth_degrowth_pct, -999999)
          ) AS country_ntile
    FROM base_data
)

SELECT
      dealer_code
    , country
    , dealer_master_country
    , region
    , dealer_city
    , district_area
    , current_month_customer_count
    , last_year_same_month_customer_count
    , growth_degrowth_pct

    , CASE
          WHEN country_ntile = 1 THEN 'Critical'
          WHEN country_ntile = 2 THEN 'Needs Monitoring'
          WHEN country_ntile = 3 THEN 'Stable'
      END AS customer_growth_health_tag

FROM country_ranking

WHERE region IN ('UK IR')

ORDER BY
      country
    , growth_degrowth_pct;
"""

athena = boto3.client("athena", region_name=ATHENA_REGION)


def _run_athena_query(sql: str) -> str:
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        WorkGroup=os.environ.get("ATHENA_WORKGROUP", "primary"),
    )
    execution_id = response["QueryExecutionId"]
    print(f"[comp_customer_count] Query submitted → {execution_id}")

    for attempt in range(MAX_POLL_ATTEMPTS):
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        print(f"[comp_customer_count] Attempt {attempt + 1}: state={state}, waiting…")
        time.sleep(POLL_INTERVAL_SEC)
    else:
        raise TimeoutError(f"Query {execution_id} did not complete within the poll limit.")

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena {state}: {reason}")

    return execution_id


def lambda_handler(event, context):
    log_event("comp_customer_count", event)
    try:
        execution_id   = _run_athena_query(SQL_QUERY)
        output_s3_path = f"{S3_OUTPUT_LOCATION}{execution_id}.csv"
        print(f"[comp_customer_count] SUCCESS → {output_s3_path}")
        return success_response({
            "query_name":         "comp_customer_count",
            "query_execution_id": execution_id,
            "output_s3_path":     output_s3_path,
        })
    except Exception as exc:
        return error_response(f"comp_customer_count failed: {exc}")
