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
WITH last_month AS (
    SELECT
        YEAR(date_add('month', -1, CURRENT_DATE)) AS cy_year,
        MONTH(date_add('month', -1, CURRENT_DATE)) AS cy_month
),

dealer_sales AS (

    SELECT
        sd.dealer_code,
        MAX(dm.dealer_name) AS dealer_name,
        MAX(dm.country) AS country,
        MAX(dm.region) AS region,
        MAX(dm.dealer_city) AS dealer_city,

        ROUND(
            SUM(
                CASE
                    WHEN sd.year = lm.cy_year
                     AND sd.month = lm.cy_month
                    THEN COALESCE(sd.sales_actual_MTD, 0)
                    ELSE 0
                END
            ),
            0
        ) AS current_year_sales_eur,

        ROUND(
            SUM(
                CASE
                    WHEN sd.year = lm.cy_year - 1
                     AND sd.month = lm.cy_month
                    THEN COALESCE(sd.sales_actual_MTD, 0)
                    ELSE 0
                END
            ),
            0
        ) AS last_year_sales_eur,

        ROUND(
            SUM(
                CASE
                    WHEN sd.year = lm.cy_year
                     AND sd.month = lm.cy_month
                    THEN COALESCE(sd.sales_actual_MTD, 0)
                    ELSE 0
                END
            )
            -
            SUM(
                CASE
                    WHEN sd.year = lm.cy_year - 1
                     AND sd.month = lm.cy_month
                    THEN COALESCE(sd.sales_actual_MTD, 0)
                    ELSE 0
                END
            ),
            0
        ) AS sales_gap_eur,

        ROUND(
            (
                SUM(
                    CASE
                        WHEN sd.year = lm.cy_year
                         AND sd.month = lm.cy_month
                        THEN COALESCE(sd.sales_actual_MTD, 0)
                        ELSE 0
                    END
                )
                -
                SUM(
                    CASE
                        WHEN sd.year = lm.cy_year - 1
                         AND sd.month = lm.cy_month
                        THEN COALESCE(sd.sales_actual_MTD, 0)
                        ELSE 0
                    END
                )
            )
            /
            NULLIF(
                SUM(
                    CASE
                        WHEN sd.year = lm.cy_year - 1
                         AND sd.month = lm.cy_month
                        THEN COALESCE(sd.sales_actual_MTD, 0)
                        ELSE 0
                    END
                ),
                0
            ) * 100,
            1
        ) AS sales_growth_decline_pct,

        CASE
            WHEN
                SUM(
                    CASE
                        WHEN sd.year = lm.cy_year
                         AND sd.month = lm.cy_month
                        THEN COALESCE(sd.sales_actual_MTD, 0)
                        ELSE 0
                    END
                )
                >
                SUM(
                    CASE
                        WHEN sd.year = lm.cy_year - 1
                         AND sd.month = lm.cy_month
                        THEN COALESCE(sd.sales_actual_MTD, 0)
                        ELSE 0
                    END
                )
            THEN 'Growth'

            WHEN
                SUM(
                    CASE
                        WHEN sd.year = lm.cy_year
                         AND sd.month = lm.cy_month
                        THEN COALESCE(sd.sales_actual_MTD, 0)
                        ELSE 0
                    END
                )
                <
                SUM(
                    CASE
                        WHEN sd.year = lm.cy_year - 1
                         AND sd.month = lm.cy_month
                        THEN COALESCE(sd.sales_actual_MTD, 0)
                        ELSE 0
                    END
                )
            THEN 'Decline'

            ELSE 'Flat'
        END AS sales_performance_tag,

        lm.cy_month AS comparison_month

    FROM "dibmw-dev-sellout"."sellout_agg_dealer" sd

    LEFT JOIN "dibmw-dev-sellout"."dealer_master" dm
        ON sd.dealer_code = dm.dealer_code

    CROSS JOIN last_month lm

    GROUP BY
        sd.dealer_code,
        lm.cy_month
),

country_ranked AS (

    SELECT
        ds.*,

        NTILE(3) OVER (
            PARTITION BY ds.country
            ORDER BY COALESCE(ds.sales_growth_decline_pct, -999999)
        ) AS country_growth_bucket

    FROM dealer_sales ds

)

SELECT
    dealer_code,
    dealer_name,
    country,
    region,
    dealer_city,
    current_year_sales_eur,
    last_year_sales_eur,
    sales_gap_eur,
    sales_growth_decline_pct,
    sales_performance_tag,
    comparison_month,

    CASE
        WHEN country_growth_bucket = 1 THEN 'Critical'
        WHEN country_growth_bucket = 2 THEN 'Needs Monitoring'
        ELSE 'Stable'
    END AS dealer_health_tag

FROM country_ranked
WHERE region IN ('UK IR')

ORDER BY
    country,
    sales_growth_decline_pct DESC;
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
