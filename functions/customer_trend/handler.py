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
    SELECT
        YEAR(date_add('month', -4, CURRENT_DATE)) AS yr,
        MONTH(date_add('month', -4, CURRENT_DATE)) AS mo,
        1 AS pos
    UNION ALL
    SELECT
        YEAR(date_add('month', -3, CURRENT_DATE)),
        MONTH(date_add('month', -3, CURRENT_DATE)),
        2
    UNION ALL
    SELECT
        YEAR(date_add('month', -2, CURRENT_DATE)),
        MONTH(date_add('month', -2, CURRENT_DATE)),
        3
    UNION ALL
    SELECT
        YEAR(date_add('month', -1, CURRENT_DATE)),
        MONTH(date_add('month', -1, CURRENT_DATE)),
        4
),

monthly_counts AS (
    SELECT
        sv.dealer_code,
        tm.pos,
        COUNT(DISTINCT sv.customer_code) AS customer_count
    FROM target_months tm
    LEFT JOIN "dibmw-dev-sellout"."sellout_view" sv
        ON YEAR(sv.invoice_date) = tm.yr
       AND MONTH(sv.invoice_date) = tm.mo
    GROUP BY
        sv.dealer_code,
        tm.pos
),

customer_counts AS (
    SELECT
        dealer_code,
        COALESCE(MAX(CASE WHEN pos = 1 THEN customer_count END), 0) AS m4_cnt,
        COALESCE(MAX(CASE WHEN pos = 2 THEN customer_count END), 0) AS m3_cnt,
        COALESCE(MAX(CASE WHEN pos = 3 THEN customer_count END), 0) AS m2_cnt,
        COALESCE(MAX(CASE WHEN pos = 4 THEN customer_count END), 0) AS m1_cnt
    FROM monthly_counts
    GROUP BY dealer_code
),

decline_calc AS (
    SELECT
        dealer_code,
        m4_cnt,
        m3_cnt,
        m2_cnt,
        m1_cnt,
        CASE
            WHEN m3_cnt < m4_cnt AND m4_cnt > 0
            THEN 100.0 * (m4_cnt - m3_cnt) / m4_cnt
        END AS m3_decline_pct,
        CASE
            WHEN m2_cnt < m3_cnt AND m3_cnt > 0
            THEN 100.0 * (m3_cnt - m2_cnt) / m3_cnt
        END AS m2_decline_pct,
        CASE
            WHEN m1_cnt < m2_cnt AND m2_cnt > 0
            THEN 100.0 * (m2_cnt - m1_cnt) / m2_cnt
        END AS m1_decline_pct
    FROM customer_counts
),

finaldata AS (
    SELECT
        dealer_code,
        CONCAT(
            CAST(m3_cnt AS VARCHAR), ', ',
            CAST(m2_cnt AS VARCHAR), ', ',
            CAST(m1_cnt AS VARCHAR)
        ) AS customer_count_last_3_months,
        CASE
            WHEN (
                CASE WHEN m3_decline_pct IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN m2_decline_pct IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN m1_decline_pct IS NOT NULL THEN 1 ELSE 0 END
            ) >= 2
            THEN
                'Declining: ' ||
                CAST(
                    ROUND(
                        (
                            COALESCE(m3_decline_pct, 0) +
                            COALESCE(m2_decline_pct, 0) +
                            COALESCE(m1_decline_pct, 0)
                        )
                        /
                        (
                            CASE WHEN m3_decline_pct IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN m2_decline_pct IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN m1_decline_pct IS NOT NULL THEN 1 ELSE 0 END
                        ),
                        2
                    ) AS VARCHAR
                ) || '%'
            ELSE 'Stable'
        END AS customer_count_trend
    FROM decline_calc
)

SELECT DISTINCT
      f.dealer_code
    , f.customer_count_last_3_months
    , f.customer_count_trend
    , s.country
    , s.dealer_master_country
    , s.region
    , s.dealer_city
    , s.district_area
FROM finaldata f
LEFT JOIN "dibmw-dev-sellout"."sellout_view" s
    ON s.dealer_code = f.dealer_code
WHERE s.region IN ('UK IR')
ORDER BY f.dealer_code;
"""

athena = boto3.client("athena", region_name=ATHENA_REGION)


def _run_athena_query(sql: str) -> str:
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
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
