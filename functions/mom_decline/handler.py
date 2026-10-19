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
WITH months AS (
    SELECT
          month_offset
        , CAST(
              date_trunc(
                  'month',
                  date_add('month', -month_offset, current_date)
              ) AS date
          ) AS month_start
        , CAST(
              date_add(
                  'month',
                  1,
                  date_trunc(
                      'month',
                      date_add('month', -month_offset, current_date)
                  )
              ) AS date
          ) AS month_end
    FROM UNNEST(SEQUENCE(1, 4)) AS t(month_offset)
),

category_list AS (
    SELECT *
    FROM (
        VALUES
              (1, 'Comfort and Safety (Accessories)')
            , (2, 'Transport Solutions (Accessories)')
            , (3, 'Special Cases Parts')
            , (4, 'Maintenance')
            , (5, 'Motorsport')
            , (6, 'Accident')
            , (7, 'BMW Group Classic - Motorcycle')
            , (8, 'BMW Group Classic - Automobile')
            , (9, 'Repair')
            , (10, 'Wheels and Tires')
            , (11, 'Exteriour Design (Accessories)')
            , (12, 'Motorcycle Equipment')
            , (13, 'Electronics')
            , (14, 'Wear')
            , (15, 'Small Parts')
            , (16, 'Chemical Products (Accessories)')
            , (17, 'BMW Group Classic - Motorsport')
            , (18, 'Communication and Information (Accessories)')
            , (19, 'Headline')
            , (20, 'Interior Design (Accessories)')
            , (21, 'Invest')
            , (22, 'Lifestyle')
            , (23, 'Local Parts')
            , (24, 'Rider Equipment')
            , (25, 'Technical Documentation (Accessories)')
            , (26, 'Technical Tuning (Accessories)')
            , (27, 'Unknown')
    ) AS t(
        category_order,
        aftersales_category
    )
),

dealer_list AS (
    SELECT DISTINCT
          s.dealer_code
        , s.country
        , s.dealer_master_country
        , s.region
        , s.dealer_city
        , s.district_area
    FROM "dibmw-dev-sellout"."sellout_view" s
    INNER JOIN months m
        ON s.invoice_date >= m.month_start
       AND s.invoice_date < m.month_end
       AND m.month_offset IN (1, 2, 3)
),

sales_agg AS (
    SELECT
          s.dealer_code
        , m.month_offset
        , m.month_start AS sales_month
        , CASE
              WHEN s.aftersales_category IS NULL
                OR TRIM(s.aftersales_category) = ''
              THEN 'Unknown'
              ELSE s.aftersales_category
          END AS aftersales_category
        , SUM(COALESCE(s.net_amount, s.sales_actual, 0)) AS sales_amount
    FROM "dibmw-dev-sellout"."sellout_view" s
    INNER JOIN months m
        ON s.invoice_date >= m.month_start
       AND s.invoice_date < m.month_end
    GROUP BY
          s.dealer_code
        , m.month_offset
        , m.month_start
        , CASE
              WHEN s.aftersales_category IS NULL
                OR TRIM(s.aftersales_category) = ''
              THEN 'Unknown'
              ELSE s.aftersales_category
          END
),

complete_sales AS (
    SELECT
          d.dealer_code
        , m.month_offset
        , m.month_start AS sales_month
        , c.category_order
        , c.aftersales_category
        , COALESCE(a.sales_amount, 0) AS sales_amount
    FROM dealer_list d
    CROSS JOIN months m
    CROSS JOIN category_list c
    LEFT JOIN sales_agg a
        ON d.dealer_code = a.dealer_code
       AND m.month_offset = a.month_offset
       AND c.aftersales_category = a.aftersales_category
),

category_month_summary AS (
    SELECT
          dealer_code
        , category_order
        , aftersales_category
        , SUM(
              CASE
                  WHEN month_offset = 3 THEN sales_amount
                  ELSE 0
              END
          ) AS current_month_minus_3_sales
        , SUM(
              CASE
                  WHEN month_offset = 2 THEN sales_amount
                  ELSE 0
              END
          ) AS current_month_minus_2_sales
        , SUM(
              CASE
                  WHEN month_offset = 1 THEN sales_amount
                  ELSE 0
              END
          ) AS current_month_minus_1_sales
        , ARRAY_JOIN(
              ARRAY[
                    CAST(ROUND(SUM(CASE WHEN month_offset = 3 THEN sales_amount ELSE 0 END), 2) AS varchar)
                  , CAST(ROUND(SUM(CASE WHEN month_offset = 2 THEN sales_amount ELSE 0 END), 2) AS varchar)
                  , CAST(ROUND(SUM(CASE WHEN month_offset = 1 THEN sales_amount ELSE 0 END), 2) AS varchar)
              ],
              ', '
          ) AS sales_current_month_minus_3_to_minus_1
    FROM complete_sales
    WHERE month_offset IN (1, 2, 3)
    GROUP BY
          dealer_code
        , category_order
        , aftersales_category
),

pivoted_dealer AS (
    SELECT
          dealer_code
        , MAX(
              CASE
                  WHEN aftersales_category = 'Comfort and Safety (Accessories)'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS ComfortSafetyAccessories
        , MAX(
              CASE
                  WHEN aftersales_category = 'Transport Solutions (Accessories)'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS TransportSolutionsAccessories
        , MAX(
              CASE
                  WHEN aftersales_category = 'Special Cases Parts'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS SpecialCasesParts
        , MAX(
              CASE
                  WHEN aftersales_category = 'Maintenance'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS Maintenance
        , MAX(
              CASE
                  WHEN aftersales_category = 'Motorsport'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS Motorsport
        , MAX(
              CASE
                  WHEN aftersales_category = 'Accident'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS Accident
        , MAX(
              CASE
                  WHEN aftersales_category = 'BMW Group Classic - Motorcycle'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS BMWGroupClassicMotorcycle
        , MAX(
              CASE
                  WHEN aftersales_category = 'BMW Group Classic - Automobile'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS BMWGroupClassicAutomobile
        , MAX(
              CASE
                  WHEN aftersales_category = 'Repair'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS Repair
        , MAX(
              CASE
                  WHEN aftersales_category = 'Wheels and Tires'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS WheelsAndTires
        , MAX(
              CASE
                  WHEN aftersales_category = 'Exteriour Design (Accessories)'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS ExteriorDesignAccessories
        , MAX(
              CASE
                  WHEN aftersales_category = 'Motorcycle Equipment'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS MotorcycleEquipment
        , MAX(
              CASE
                  WHEN aftersales_category = 'Electronics'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS Electronics
        , MAX(
              CASE
                  WHEN aftersales_category = 'Wear'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS Wear
        , MAX(
              CASE
                  WHEN aftersales_category = 'Small Parts'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS SmallParts
        , MAX(
              CASE
                  WHEN aftersales_category = 'Chemical Products (Accessories)'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS ChemicalProductsAccessories
        , MAX(
              CASE
                  WHEN aftersales_category = 'BMW Group Classic - Motorsport'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS BMWGroupClassicMotorsport
        , MAX(
              CASE
                  WHEN aftersales_category = 'Communication and Information (Accessories)'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS CommunicationInformationAccessories
        , MAX(
              CASE
                  WHEN aftersales_category = 'Headline'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS Headline
        , MAX(
              CASE
                  WHEN aftersales_category = 'Interior Design (Accessories)'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS InteriorDesignAccessories
        , MAX(
              CASE
                  WHEN aftersales_category = 'Invest'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS Invest
        , MAX(
              CASE
                  WHEN aftersales_category = 'Lifestyle'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS Lifestyle
        , MAX(
              CASE
                  WHEN aftersales_category = 'Local Parts'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS LocalParts
        , MAX(
              CASE
                  WHEN aftersales_category = 'Rider Equipment'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS RiderEquipment
        , MAX(
              CASE
                  WHEN aftersales_category = 'Technical Documentation (Accessories)'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS TechnicalDocumentationAccessories
        , MAX(
              CASE
                  WHEN aftersales_category = 'Technical Tuning (Accessories)'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS TechnicalTuningAccessories
        , MAX(
              CASE
                  WHEN aftersales_category = 'Unknown'
                  THEN sales_current_month_minus_3_to_minus_1
              END
          ) AS Unknown
    FROM category_month_summary
    GROUP BY dealer_code
),

sales_with_previous_month AS (
    SELECT
          dealer_code
        , month_offset
        , sales_month
        , category_order
        , aftersales_category
        , sales_amount AS current_month_sales
        , LAG(sales_amount) OVER (
              PARTITION BY
                    dealer_code
                  , aftersales_category
              ORDER BY month_offset DESC
          ) AS previous_month_sales
    FROM complete_sales
),

decline_calc AS (
    SELECT
          dealer_code
        , category_order
        , aftersales_category
        , SUM(
              CASE
                  WHEN previous_month_sales IS NOT NULL
                   AND previous_month_sales > 0
                   AND current_month_sales < previous_month_sales
                  THEN 1
                  ELSE 0
              END
          ) AS decline_month_count
        , AVG(
              CASE
                  WHEN previous_month_sales IS NOT NULL
                   AND previous_month_sales > 0
                   AND current_month_sales < previous_month_sales
                  THEN
                      100.0
                      * (previous_month_sales - current_month_sales)
                      / previous_month_sales
              END
          ) AS avg_decline_pct
    FROM sales_with_previous_month
    WHERE month_offset IN (1, 2, 3)
    GROUP BY
          dealer_code
        , category_order
        , aftersales_category
),

decline_flags AS (
    SELECT
          dealer_code
        , ARRAY_JOIN(
              ARRAY_AGG(
                  aftersales_category
                  || ': '
                  || CAST(ROUND(avg_decline_pct, 2) AS varchar)
                  || '%'
                  ORDER BY category_order
              ),
              ', '
          ) AS Categories_With_Decline_Pct
    FROM decline_calc
    WHERE decline_month_count >= 2
    GROUP BY dealer_code
)

SELECT
      p.dealer_code
    , s.country
    , s.dealer_master_country
    , s.region
    , s.dealer_city
    , s.district_area
    , p.ComfortSafetyAccessories
    , p.TransportSolutionsAccessories
    , p.SpecialCasesParts
    , p.Maintenance
    , p.Motorsport
    , p.Accident
    , p.BMWGroupClassicMotorcycle
    , p.BMWGroupClassicAutomobile
    , p.Repair
    , p.WheelsAndTires
    , p.ExteriorDesignAccessories
    , p.MotorcycleEquipment
    , p.Electronics
    , p.Wear
    , p.SmallParts
    , p.ChemicalProductsAccessories
    , p.BMWGroupClassicMotorsport
    , p.CommunicationInformationAccessories
    , p.Headline
    , p.InteriorDesignAccessories
    , p.Invest
    , p.Lifestyle
    , p.LocalParts
    , p.RiderEquipment
    , p.TechnicalDocumentationAccessories
    , p.TechnicalTuningAccessories
    , p.Unknown
    , COALESCE(
          d.Categories_With_Decline_Pct,
          'None'
      ) AS Categories_With_Decline_Pct
FROM pivoted_dealer p
LEFT JOIN decline_flags d
    ON p.dealer_code = d.dealer_code
LEFT JOIN dealer_list s
    ON s.dealer_code = p.dealer_code
WHERE s.region IN ('UK IR')
ORDER BY p.dealer_code;
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
    print(f"[mom_decline] Query submitted → {execution_id}")

    for attempt in range(MAX_POLL_ATTEMPTS):
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        print(f"[mom_decline] Attempt {attempt + 1}: state={state}, waiting…")
        time.sleep(POLL_INTERVAL_SEC)
    else:
        raise TimeoutError(f"Query {execution_id} did not complete within the poll limit.")

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena {state}: {reason}")

    return execution_id


def lambda_handler(event, context):
    log_event("mom_decline", event)
    try:
        execution_id   = _run_athena_query(SQL_QUERY)
        output_s3_path = f"{S3_OUTPUT_LOCATION}{execution_id}.csv"
        print(f"[mom_decline] SUCCESS → {output_s3_path}")
        return success_response({
            "query_name":         "mom_decline",
            "query_execution_id": execution_id,
            "output_s3_path":     output_s3_path,
        })
    except Exception as exc:
        return error_response(f"mom_decline failed: {exc}")
