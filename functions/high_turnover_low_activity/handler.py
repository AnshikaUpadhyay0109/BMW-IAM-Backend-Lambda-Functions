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
WITH dataset_anchor AS (
    SELECT
        CAST(DATE_TRUNC('month', DATE_ADD('month', -1, MAX(invoice_date))) AS DATE) AS current_month,
        CAST(DATE_ADD('month', -5, DATE_TRUNC('month', DATE_ADD('month', -1, MAX(invoice_date)))) AS DATE) AS history_start_month
    FROM "dibmw-dev-sellout"."sellout_view"
),

dealer_attributes AS (
    SELECT
        country,
        dealer_code,
        MAX_BY(dealer_master_country, invoice_date) AS dealer_master_country,
        MAX_BY(region, invoice_date) AS region,
        MAX_BY(dealer_city, invoice_date) AS dealer_city,
        MAX_BY(district_area, invoice_date) AS district_area
    FROM "dibmw-dev-sellout"."sellout_view"
    GROUP BY country, dealer_code
),

monthly_dealer_metrics AS (
    SELECT
        s.country,
        s.dealer_code,
        CAST(DATE_TRUNC('month', s.invoice_date) AS DATE) AS reporting_month,
        SUM(COALESCE(s.dealer_landing_price, s.purchase_actual, 0)) AS monthly_purchase_value,
        SUM(COALESCE(s.net_amount, s.sales_actual, 0)) AS monthly_sales_revenue,
        COUNT(DISTINCT s.invoice_number) AS monthly_sales_invoice_count
    FROM "dibmw-dev-sellout"."sellout_view" s
    CROSS JOIN dataset_anchor a
    WHERE s.invoice_date >= a.history_start_month
      AND s.invoice_date < DATE_ADD('month', 1, a.current_month)
    GROUP BY
        s.country,
        s.dealer_code,
        CAST(DATE_TRUNC('month', s.invoice_date) AS DATE)
),

dealer_historical_metrics AS (
    SELECT
        m.country,
        m.dealer_code,

        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -5, a.current_month) THEN m.monthly_purchase_value END), 0) AS history_month_5_purchase_value,
        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -4, a.current_month) THEN m.monthly_purchase_value END), 0) AS history_month_4_purchase_value,
        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -3, a.current_month) THEN m.monthly_purchase_value END), 0) AS history_month_3_purchase_value,
        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -2, a.current_month) THEN m.monthly_purchase_value END), 0) AS history_month_2_purchase_value,
        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -1, a.current_month) THEN m.monthly_purchase_value END), 0) AS history_month_1_purchase_value,

        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -5, a.current_month) THEN m.monthly_sales_revenue END), 0) AS history_month_5_sales_revenue,
        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -4, a.current_month) THEN m.monthly_sales_revenue END), 0) AS history_month_4_sales_revenue,
        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -3, a.current_month) THEN m.monthly_sales_revenue END), 0) AS history_month_3_sales_revenue,
        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -2, a.current_month) THEN m.monthly_sales_revenue END), 0) AS history_month_2_sales_revenue,
        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -1, a.current_month) THEN m.monthly_sales_revenue END), 0) AS history_month_1_sales_revenue,

        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -5, a.current_month) THEN m.monthly_sales_invoice_count END), 0) AS history_month_5_invoice_count,
        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -4, a.current_month) THEN m.monthly_sales_invoice_count END), 0) AS history_month_4_invoice_count,
        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -3, a.current_month) THEN m.monthly_sales_invoice_count END), 0) AS history_month_3_invoice_count,
        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -2, a.current_month) THEN m.monthly_sales_invoice_count END), 0) AS history_month_2_invoice_count,
        COALESCE(MAX(CASE WHEN m.reporting_month = DATE_ADD('month', -1, a.current_month) THEN m.monthly_sales_invoice_count END), 0) AS history_month_1_invoice_count,

        AVG(m.monthly_purchase_value) AS dealer_avg_monthly_purchase_value,
        AVG(m.monthly_sales_revenue) AS dealer_avg_monthly_sales_revenue,
        AVG(CAST(m.monthly_sales_invoice_count AS DOUBLE)) AS dealer_avg_monthly_sales_invoice_count,
        COUNT(DISTINCT m.reporting_month) AS historical_months_available

    FROM monthly_dealer_metrics m
    CROSS JOIN dataset_anchor a
    WHERE m.reporting_month >= a.history_start_month
      AND m.reporting_month < a.current_month
    GROUP BY m.country, m.dealer_code
),

current_month_metrics AS (
    SELECT
        m.country,
        m.dealer_code,
        m.reporting_month AS current_reporting_month,
        m.monthly_purchase_value AS current_month_purchase_value,
        m.monthly_sales_revenue AS current_month_sales_revenue,
        m.monthly_sales_invoice_count AS current_month_sales_invoice_count
    FROM monthly_dealer_metrics m
    CROSS JOIN dataset_anchor a
    WHERE m.reporting_month = a.current_month
),

dealer_growth_metrics AS (
    SELECT
        c.country,
        c.dealer_code,

        d.dealer_master_country,
        d.region,
        d.dealer_city,
        d.district_area,

        c.current_reporting_month,
        c.current_month_purchase_value,
        c.current_month_sales_revenue,
        c.current_month_sales_invoice_count,

        h.history_month_5_purchase_value,
        h.history_month_4_purchase_value,
        h.history_month_3_purchase_value,
        h.history_month_2_purchase_value,
        h.history_month_1_purchase_value,

        h.history_month_5_sales_revenue,
        h.history_month_4_sales_revenue,
        h.history_month_3_sales_revenue,
        h.history_month_2_sales_revenue,
        h.history_month_1_sales_revenue,

        h.history_month_5_invoice_count,
        h.history_month_4_invoice_count,
        h.history_month_3_invoice_count,
        h.history_month_2_invoice_count,
        h.history_month_1_invoice_count,

        h.dealer_avg_monthly_purchase_value,
        h.dealer_avg_monthly_sales_revenue,
        h.dealer_avg_monthly_sales_invoice_count,
        COALESCE(h.historical_months_available, 0) AS historical_months_available,

        CASE
            WHEN h.dealer_avg_monthly_purchase_value > 0
            THEN ((c.current_month_purchase_value - h.dealer_avg_monthly_purchase_value) / h.dealer_avg_monthly_purchase_value) * 100.0
        END AS purchase_growth_pct,

        CASE
            WHEN h.dealer_avg_monthly_sales_revenue > 0
            THEN ((c.current_month_sales_revenue - h.dealer_avg_monthly_sales_revenue) / h.dealer_avg_monthly_sales_revenue) * 100.0
        END AS sales_growth_pct,

        CASE
            WHEN h.dealer_avg_monthly_sales_invoice_count > 0
            THEN ((CAST(c.current_month_sales_invoice_count AS DOUBLE) - h.dealer_avg_monthly_sales_invoice_count) / h.dealer_avg_monthly_sales_invoice_count) * 100.0
        END AS invoice_growth_pct

    FROM current_month_metrics c

    LEFT JOIN dealer_attributes d
        ON c.country = d.country
       AND c.dealer_code = d.dealer_code

    LEFT JOIN dealer_historical_metrics h
        ON c.country = h.country
       AND c.dealer_code = h.dealer_code
),

dealer_scored_metrics AS (
    SELECT
        g.*,

        CASE
            WHEN g.sales_growth_pct IS NOT NULL
             AND g.invoice_growth_pct IS NOT NULL
            THEN (0.80 * g.sales_growth_pct) + (0.20 * g.invoice_growth_pct)
        END AS turnover_score

    FROM dealer_growth_metrics g
)

SELECT
    g.country,
    g.dealer_code,
    g.dealer_master_country,
    g.region,
    g.dealer_city,
    g.district_area,
    g.current_reporting_month,

    DATE_FORMAT(a.current_month, '%b-%Y') AS current_month_name,

    ARRAY_JOIN(
        ARRAY[
            DATE_FORMAT(DATE_ADD('month', -5, a.current_month), '%b-%Y'),
            DATE_FORMAT(DATE_ADD('month', -4, a.current_month), '%b-%Y'),
            DATE_FORMAT(DATE_ADD('month', -3, a.current_month), '%b-%Y'),
            DATE_FORMAT(DATE_ADD('month', -2, a.current_month), '%b-%Y'),
            DATE_FORMAT(DATE_ADD('month', -1, a.current_month), '%b-%Y')
        ],
        ', '
    ) AS historical_month_sequence,

    ARRAY_JOIN(
        ARRAY[
            CAST(ROUND(CAST(COALESCE(g.history_month_5_purchase_value, 0) AS DOUBLE), 1) AS VARCHAR),
            CAST(ROUND(CAST(COALESCE(g.history_month_4_purchase_value, 0) AS DOUBLE), 1) AS VARCHAR),
            CAST(ROUND(CAST(COALESCE(g.history_month_3_purchase_value, 0) AS DOUBLE), 1) AS VARCHAR),
            CAST(ROUND(CAST(COALESCE(g.history_month_2_purchase_value, 0) AS DOUBLE), 1) AS VARCHAR),
            CAST(ROUND(CAST(COALESCE(g.history_month_1_purchase_value, 0) AS DOUBLE), 1) AS VARCHAR)
        ],
        ', '
    ) AS historical_purchase_values,

    ROUND(CAST(g.dealer_avg_monthly_purchase_value AS DOUBLE), 1) AS dealer_avg_monthly_purchase_value,
    ROUND(CAST(g.current_month_purchase_value AS DOUBLE), 1) AS current_month_purchase_value,
    ROUND(CAST(g.purchase_growth_pct AS DOUBLE), 1) AS purchase_growth_pct,

    ARRAY_JOIN(
        ARRAY[
            CAST(ROUND(CAST(COALESCE(g.history_month_5_sales_revenue, 0) AS DOUBLE), 1) AS VARCHAR),
            CAST(ROUND(CAST(COALESCE(g.history_month_4_sales_revenue, 0) AS DOUBLE), 1) AS VARCHAR),
            CAST(ROUND(CAST(COALESCE(g.history_month_3_sales_revenue, 0) AS DOUBLE), 1) AS VARCHAR),
            CAST(ROUND(CAST(COALESCE(g.history_month_2_sales_revenue, 0) AS DOUBLE), 1) AS VARCHAR),
            CAST(ROUND(CAST(COALESCE(g.history_month_1_sales_revenue, 0) AS DOUBLE), 1) AS VARCHAR)
        ],
        ', '
    ) AS historical_sales_values,

    ROUND(CAST(g.dealer_avg_monthly_sales_revenue AS DOUBLE), 1) AS dealer_avg_monthly_sales_revenue,
    ROUND(CAST(g.current_month_sales_revenue AS DOUBLE), 1) AS current_month_sales_revenue,
    ROUND(CAST(g.sales_growth_pct AS DOUBLE), 1) AS sales_growth_pct,

    ARRAY_JOIN(
        ARRAY[
            CAST(ROUND(CAST(COALESCE(g.history_month_5_invoice_count, 0) AS DOUBLE), 1) AS VARCHAR),
            CAST(ROUND(CAST(COALESCE(g.history_month_4_invoice_count, 0) AS DOUBLE), 1) AS VARCHAR),
            CAST(ROUND(CAST(COALESCE(g.history_month_3_invoice_count, 0) AS DOUBLE), 1) AS VARCHAR),
            CAST(ROUND(CAST(COALESCE(g.history_month_2_invoice_count, 0) AS DOUBLE), 1) AS VARCHAR),
            CAST(ROUND(CAST(COALESCE(g.history_month_1_invoice_count, 0) AS DOUBLE), 1) AS VARCHAR)
        ],
        ', '
    ) AS historical_invoice_counts,

    ROUND(CAST(g.dealer_avg_monthly_sales_invoice_count AS DOUBLE), 1) AS dealer_avg_monthly_sales_invoice_count,
    ROUND(CAST(g.current_month_sales_invoice_count AS DOUBLE), 1) AS current_month_sales_invoice_count,
    ROUND(CAST(g.invoice_growth_pct AS DOUBLE), 1) AS invoice_growth_pct,

    ROUND(CAST(g.turnover_score AS DOUBLE), 1) AS turnover_score,

    COALESCE(g.historical_months_available, 0) AS historical_months_available,

    CASE
        WHEN COALESCE(g.historical_months_available, 0) <= 3
            THEN 'Data <= 3 Months Available'
        WHEN g.turnover_score IS NULL
            THEN 'Cannot Comment'
        WHEN g.turnover_score > 5
            THEN 'High Turnover'
        ELSE 'Low Turnover'
    END AS turnover_tag,

    CASE
        WHEN COALESCE(g.historical_months_available, 0) <= 3
            THEN 'Data <= 3 Months Available'
        WHEN g.purchase_growth_pct IS NULL
            THEN 'Cannot Comment'
        WHEN g.purchase_growth_pct < -15
            THEN 'Low Activity'
        WHEN g.purchase_growth_pct > 15
            THEN 'High Activity'
        ELSE 'Stable Activity'
    END AS activity_tag,

    CASE
        WHEN COALESCE(g.historical_months_available, 0) <= 3
            THEN 'Data <= 3 Months Available'
        WHEN g.invoice_growth_pct IS NULL
            THEN 'Cannot Comment'
        WHEN g.invoice_growth_pct > 10
            THEN 'High Invoice Momentum'
        WHEN g.invoice_growth_pct < -10
            THEN 'Low Invoice Momentum'
        ELSE 'Stable Invoice Momentum'
    END AS invoice_momentum_tag,

    CASE
        WHEN COALESCE(g.historical_months_available, 0) <= 3
            THEN 'Data <= 3 Months Available'

        WHEN g.purchase_growth_pct IS NULL
          OR g.turnover_score IS NULL
            THEN 'Cannot Comment - Historical Average Is Zero'

        WHEN g.turnover_score > 5
         AND g.purchase_growth_pct < -15
            THEN 'High Turnover, Low Activity'

        WHEN g.turnover_score > 5
         AND g.purchase_growth_pct >= -15
            THEN 'High Turnover, High Activity'

        WHEN g.turnover_score <= 5
         AND g.purchase_growth_pct < -15
            THEN 'Low Turnover, Low Activity'

        ELSE 'Low Turnover, High Activity'
    END AS dealer_segment,

    CASE
        WHEN COALESCE(g.historical_months_available, 0) <= 3
            THEN 'Data <= 3 Months Available'

        WHEN g.purchase_growth_pct IS NULL
          OR g.turnover_score IS NULL
            THEN 'Cannot Comment'

        WHEN g.turnover_score > 5
         AND g.purchase_growth_pct < -15
            THEN 'Replenishment Opportunity'

        WHEN g.turnover_score > 5
         AND g.purchase_growth_pct >= -15
            THEN 'Growth Opportunity'

        WHEN g.turnover_score <= 5
         AND g.purchase_growth_pct < -15
            THEN 'Dealer At Risk'

        ELSE 'Sell-Out Improvement Opportunity'
    END AS ai_opportunity_tag,

    CASE
        WHEN COALESCE(g.historical_months_available, 0) <= 3
            THEN 'Cannot Comment'

        WHEN g.purchase_growth_pct IS NULL
            THEN 'Cannot Comment'

        WHEN g.purchase_growth_pct <= -40
            THEN 'Critical'

        WHEN g.purchase_growth_pct <= -25
            THEN 'High'

        WHEN g.purchase_growth_pct < -15
            THEN 'Medium'

        ELSE 'Normal'
    END AS purchase_risk_level

FROM dealer_scored_metrics g
CROSS JOIN dataset_anchor a

WHERE region IN ('UK IR')

ORDER BY
    CASE
        WHEN COALESCE(g.historical_months_available, 0) > 3
         AND g.turnover_score > 5
         AND g.purchase_growth_pct < -15
            THEN 1

        WHEN COALESCE(g.historical_months_available, 0) > 3
         AND g.turnover_score > 5
            THEN 2

        WHEN COALESCE(g.historical_months_available, 0) > 3
         AND g.purchase_growth_pct < -15
            THEN 3

        WHEN COALESCE(g.historical_months_available, 0) > 3
            THEN 4

        ELSE 5
    END,
    g.turnover_score DESC,
    g.sales_growth_pct DESC,
    g.country,
    g.dealer_code;
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
