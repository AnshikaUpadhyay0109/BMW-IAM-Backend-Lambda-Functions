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
WITH monthly_counts AS (
    SELECT
          dealer_code
        , month(invoice_date) AS month_no
        , COUNT(DISTINCT customer_code) AS customer_count
    FROM "dibmw-dev-sellout"."sellout_view"
    WHERE dealer_code IN (
        '21125','11380','35955','33400',
        '40477','6057','30864','9118',
        '28965','33160'
    )
    AND year(invoice_date) = 2026
    AND month(invoice_date) IN (3,4,5,6)
    GROUP BY
          dealer_code
        , month(invoice_date)
),

customer_counts AS (
    SELECT
          dealer_code

        , COALESCE(MAX(CASE WHEN month_no = 3 THEN customer_count END),0) AS mar_cnt
        , COALESCE(MAX(CASE WHEN month_no = 4 THEN customer_count END),0) AS apr_cnt
        , COALESCE(MAX(CASE WHEN month_no = 5 THEN customer_count END),0) AS may_cnt
        , COALESCE(MAX(CASE WHEN month_no = 6 THEN customer_count END),0) AS jun_cnt

    FROM monthly_counts
    GROUP BY dealer_code
),

decline_calc AS (
    SELECT
          dealer_code

        , mar_cnt
        , apr_cnt
        , may_cnt
        , jun_cnt

        -- Apr vs Mar
        , CASE
              WHEN apr_cnt < mar_cnt
               AND mar_cnt > 0
              THEN 100.0 * (mar_cnt - apr_cnt) / mar_cnt
          END AS apr_decline_pct

        -- May vs Apr
        , CASE
              WHEN may_cnt < apr_cnt
               AND apr_cnt > 0
              THEN 100.0 * (apr_cnt - may_cnt) / apr_cnt
          END AS may_decline_pct

        -- Jun vs May
        , CASE
              WHEN jun_cnt < may_cnt
               AND may_cnt > 0
              THEN 100.0 * (may_cnt - jun_cnt) / may_cnt
          END AS jun_decline_pct

    FROM customer_counts
)

SELECT
      dealer_code

    , CONCAT(
          CAST(apr_cnt AS VARCHAR)
        , ', '
        , CAST(may_cnt AS VARCHAR)
        , ', '
        , CAST(jun_cnt AS VARCHAR)
      ) AS customer_count_apr_may_jun

    , CASE
          WHEN
              (
                  CASE WHEN apr_decline_pct IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN may_decline_pct IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN jun_decline_pct IS NOT NULL THEN 1 ELSE 0 END
              ) >= 2
          THEN
              'Declining: ' ||

              CAST(
                  ROUND(
                      (
                          COALESCE(apr_decline_pct,0)
                        + COALESCE(may_decline_pct,0)
                        + COALESCE(jun_decline_pct,0)
                      )
                      /
                      (
                          CASE WHEN apr_decline_pct IS NOT NULL THEN 1 ELSE 0 END
                        + CASE WHEN may_decline_pct IS NOT NULL THEN 1 ELSE 0 END
                        + CASE WHEN jun_decline_pct IS NOT NULL THEN 1 ELSE 0 END
                      )
                  ,2
                  )
              AS VARCHAR
              ) || '%'

          ELSE
              'Stable'
      END AS customer_count_trend

FROM decline_calc
ORDER BY dealer_code;
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
    print(f"[customer_trend] Query submitted → {execution_id}")

    for attempt in range(MAX_POLL_ATTEMPTS):
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        print(f"[customer_trend] Attempt {attempt + 1}: state={state}, waiting…")
        time.sleep(POLL_INTERVAL_SEC)
    else:
        raise TimeoutError(f"Query {execution_id} did not complete within the poll limit.")

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena {state}: {reason}")

    return execution_id


def lambda_handler(event, context):
    log_event("customer_trend", event)
    try:
        execution_id   = _run_athena_query(SQL_QUERY)
        output_s3_path = f"{S3_OUTPUT_LOCATION}{execution_id}.csv"
        print(f"[customer_trend] SUCCESS → {output_s3_path}")
        return success_response({
            "query_name":         "customer_trend",
            "query_execution_id": execution_id,
            "output_s3_path":     output_s3_path,
        })
    except Exception as exc:
        return error_response(f"customer_trend failed: {exc}")
