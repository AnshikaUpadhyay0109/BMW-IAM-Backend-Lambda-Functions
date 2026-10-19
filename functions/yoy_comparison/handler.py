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
WITH params AS (
    SELECT
          CAST(date_trunc('month', date_add('month', -1, current_date)) AS date) AS curr_month_start
        , CAST(date_add('month', 1, date_trunc('month', date_add('month', -1, current_date))) AS date) AS curr_month_end
        , CAST(date_add('year', -1, date_trunc('month', date_add('month', -1, current_date))) AS date) AS ly_month_start
        , CAST(date_add('year', -1, date_add('month', 1, date_trunc('month', date_add('month', -1, current_date)))) AS date) AS ly_month_end
),

base AS (
    SELECT
        dealer_code
        , country
        , dealer_master_country
        , region
        , dealer_city
        , district_area

        -- Current Period: Previous Month

        , SUM(CASE WHEN aftersales_category = 'Comfort and Safety (Accessories)'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS ComfortSafetyAccessories

        , SUM(CASE WHEN aftersales_category = 'Transport Solutions (Accessories)'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS TransportSolutionsAccessories

        , SUM(CASE WHEN aftersales_category = 'Special Cases Parts'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS SpecialCasesParts

        , SUM(CASE WHEN aftersales_category = 'Maintenance'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Maintenance

        , SUM(CASE WHEN aftersales_category = 'Motorsport'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Motorsport

        , SUM(CASE WHEN aftersales_category = 'Accident'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Accident

        , SUM(CASE WHEN aftersales_category = 'BMW Group Classic - Motorcycle'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS BMWGroupClassicMotorcycle

        , SUM(CASE WHEN aftersales_category = 'BMW Group Classic - Automobile'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS BMWGroupClassicAutomobile

        , SUM(CASE WHEN aftersales_category = 'Repair'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Repair

        , SUM(CASE WHEN aftersales_category = 'Wheels and Tires'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS WheelsAndTires

        , SUM(CASE WHEN aftersales_category = 'Exterior Design (Accessories)'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS ExteriorDesignAccessories

        , SUM(CASE WHEN aftersales_category = 'Motorcycle Equipment'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS MotorcycleEquipment

        , SUM(CASE WHEN aftersales_category = 'Electronics'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Electronics

        , SUM(CASE WHEN aftersales_category = 'Wear'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Wear

        , SUM(CASE WHEN aftersales_category = 'Small Parts'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS SmallParts

        , SUM(CASE WHEN aftersales_category = 'Chemical Products (Accessories)'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS ChemicalProductsAccessories

        , SUM(CASE WHEN aftersales_category = 'BMW Group Classic - Motorsport'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS BMWGroupClassicMotorsport

        , SUM(CASE WHEN aftersales_category = 'Communication and Information (Accessories)'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS CommunicationInformationAccessories

        , SUM(CASE WHEN aftersales_category = 'Headline'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS Headline

        , SUM(CASE WHEN aftersales_category = 'Interior Design (Accessories)'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS InteriorDesignAccessories

        , SUM(CASE WHEN aftersales_category = 'Invest'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS Invest

        , SUM(CASE WHEN aftersales_category = 'Lifestyle'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS Lifestyle

        , SUM(CASE WHEN aftersales_category = 'Local Parts'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS LocalParts

        , SUM(CASE WHEN aftersales_category = 'Rider Equipment'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS RiderEquipment

        , SUM(CASE WHEN aftersales_category = 'Technical Documentation (Accessories)'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS TechnicalDocumentationAccessories

        , SUM(CASE WHEN aftersales_category = 'Technical Tuning (Accessories)'
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS TechnicalTuningAccessories

        , SUM(CASE WHEN (aftersales_category IS NULL OR TRIM(aftersales_category) = '')
                    AND invoice_date >= curr_month_start
                    AND invoice_date < curr_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Unknown


        -- Same Month Last Year

        , SUM(CASE WHEN aftersales_category = 'Comfort and Safety (Accessories)'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS ComfortSafetyAccessories_LY

        , SUM(CASE WHEN aftersales_category = 'Transport Solutions (Accessories)'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS TransportSolutionsAccessories_LY

        , SUM(CASE WHEN aftersales_category = 'Special Cases Parts'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS SpecialCasesParts_LY

        , SUM(CASE WHEN aftersales_category = 'Maintenance'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Maintenance_LY

        , SUM(CASE WHEN aftersales_category = 'Motorsport'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Motorsport_LY

        , SUM(CASE WHEN aftersales_category = 'Accident'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Accident_LY

        , SUM(CASE WHEN aftersales_category = 'BMW Group Classic - Motorcycle'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS BMWGroupClassicMotorcycle_LY

        , SUM(CASE WHEN aftersales_category = 'BMW Group Classic - Automobile'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS BMWGroupClassicAutomobile_LY

        , SUM(CASE WHEN aftersales_category = 'Repair'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Repair_LY

        , SUM(CASE WHEN aftersales_category = 'Wheels and Tires'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS WheelsAndTires_LY

        , SUM(CASE WHEN aftersales_category = 'Exterior Design (Accessories)'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS ExteriorDesignAccessories_LY

        , SUM(CASE WHEN aftersales_category = 'Motorcycle Equipment'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS MotorcycleEquipment_LY

        , SUM(CASE WHEN aftersales_category = 'Electronics'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Electronics_LY

        , SUM(CASE WHEN aftersales_category = 'Wear'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Wear_LY

        , SUM(CASE WHEN aftersales_category = 'Small Parts'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS SmallParts_LY

        , SUM(CASE WHEN aftersales_category = 'Chemical Products (Accessories)'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS ChemicalProductsAccessories_LY

        , SUM(CASE WHEN aftersales_category = 'BMW Group Classic - Motorsport'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS BMWGroupClassicMotorsport_LY

        , SUM(CASE WHEN aftersales_category = 'Communication and Information (Accessories)'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS CommunicationInformationAccessories_LY

        , SUM(CASE WHEN aftersales_category = 'Headline'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS Headline_LY

        , SUM(CASE WHEN aftersales_category = 'Interior Design (Accessories)'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS InteriorDesignAccessories_LY

        , SUM(CASE WHEN aftersales_category = 'Invest'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS Invest_LY

        , SUM(CASE WHEN aftersales_category = 'Lifestyle'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS Lifestyle_LY

        , SUM(CASE WHEN aftersales_category = 'Local Parts'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS LocalParts_LY

        , SUM(CASE WHEN aftersales_category = 'Rider Equipment'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS RiderEquipment_LY

        , SUM(CASE WHEN aftersales_category = 'Technical Documentation (Accessories)'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS TechnicalDocumentationAccessories_LY

        , SUM(CASE WHEN aftersales_category = 'Technical Tuning (Accessories)'
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                  THEN COALESCE(net_amount, sales_actual, 0) ELSE 0 END) AS TechnicalTuningAccessories_LY

        , SUM(CASE WHEN (aftersales_category IS NULL OR TRIM(aftersales_category) = '')
                    AND invoice_date >= ly_month_start
                    AND invoice_date < ly_month_end
                   THEN coalesce(net_amount, sales_actual) ELSE 0 END) AS Unknown_LY

    FROM "dibmw-dev-sellout"."sellout_view"
    CROSS JOIN params

    GROUP BY dealer_code
        , country
        , dealer_master_country
        , region
        , dealer_city
        , district_area
)

SELECT
    *

    -- YoY % Difference

    , ROUND(
        100.0 * (ComfortSafetyAccessories - ComfortSafetyAccessories_LY)
        / NULLIF(ComfortSafetyAccessories_LY, 0)
      , 2) AS ComfortSafetyAccessories_YOY_PCT

    , ROUND(
        100.0 * (TransportSolutionsAccessories - TransportSolutionsAccessories_LY)
        / NULLIF(TransportSolutionsAccessories_LY, 0)
      , 2) AS TransportSolutionsAccessories_YOY_PCT

    , ROUND(
        100.0 * (SpecialCasesParts - SpecialCasesParts_LY)
        / NULLIF(SpecialCasesParts_LY, 0)
      , 2) AS SpecialCasesParts_YOY_PCT

    , ROUND(
        100.0 * (Maintenance - Maintenance_LY)
        / NULLIF(Maintenance_LY, 0)
      , 2) AS Maintenance_YOY_PCT

    , ROUND(
        100.0 * (Motorsport - Motorsport_LY)
        / NULLIF(Motorsport_LY, 0)
      , 2) AS Motorsport_YOY_PCT

    , ROUND(
        100.0 * (Accident - Accident_LY)
        / NULLIF(Accident_LY, 0)
      , 2) AS Accident_YOY_PCT

    , ROUND(
        100.0 * (BMWGroupClassicMotorcycle - BMWGroupClassicMotorcycle_LY)
        / NULLIF(BMWGroupClassicMotorcycle_LY, 0)
      , 2) AS BMWGroupClassicMotorcycle_YOY_PCT

    , ROUND(
        100.0 * (BMWGroupClassicAutomobile - BMWGroupClassicAutomobile_LY)
        / NULLIF(BMWGroupClassicAutomobile_LY, 0)
      , 2) AS BMWGroupClassicAutomobile_YOY_PCT

    , ROUND(
        100.0 * (Repair - Repair_LY)
        / NULLIF(Repair_LY, 0)
      , 2) AS Repair_YOY_PCT

    , ROUND(
        100.0 * (WheelsAndTires - WheelsAndTires_LY)
        / NULLIF(WheelsAndTires_LY, 0)
      , 2) AS WheelsAndTires_YOY_PCT

    , ROUND(
        100.0 * (ExteriorDesignAccessories - ExteriorDesignAccessories_LY)
        / NULLIF(ExteriorDesignAccessories_LY, 0)
      , 2) AS ExteriorDesignAccessories_YOY_PCT

    , ROUND(
        100.0 * (MotorcycleEquipment - MotorcycleEquipment_LY)
        / NULLIF(MotorcycleEquipment_LY, 0)
      , 2) AS MotorcycleEquipment_YOY_PCT

    , ROUND(
        100.0 * (Electronics - Electronics_LY)
        / NULLIF(Electronics_LY, 0)
      , 2) AS Electronics_YOY_PCT

    , ROUND(
        100.0 * (Wear - Wear_LY)
        / NULLIF(Wear_LY, 0)
      , 2) AS Wear_YOY_PCT

    , ROUND(
        100.0 * (SmallParts - SmallParts_LY)
        / NULLIF(SmallParts_LY, 0)
      , 2) AS SmallParts_YOY_PCT

    , ROUND(
        100.0 * (ChemicalProductsAccessories - ChemicalProductsAccessories_LY)
        / NULLIF(ChemicalProductsAccessories_LY, 0)
      , 2) AS ChemicalProductsAccessories_YOY_PCT

    , ROUND(
        100.0 * (BMWGroupClassicMotorsport - BMWGroupClassicMotorsport_LY)
        / NULLIF(BMWGroupClassicMotorsport_LY, 0)
      , 2) AS BMWGroupClassicMotorsport_YOY_PCT

    , ROUND(
        100.0 * (CommunicationInformationAccessories - CommunicationInformationAccessories_LY)
        / NULLIF(CommunicationInformationAccessories_LY, 0)
      , 2) AS CommunicationInformationAccessories_YOY_PCT

    , ROUND(
        100.0 * (Headline - Headline_LY)
        / NULLIF(Headline_LY, 0)
      , 2) AS Headline_YOY_PCT

    , ROUND(
        100.0 * (InteriorDesignAccessories - InteriorDesignAccessories_LY)
        / NULLIF(InteriorDesignAccessories_LY, 0)
      , 2) AS InteriorDesignAccessories_YOY_PCT

    , ROUND(
        100.0 * (Invest - Invest_LY)
        / NULLIF(Invest_LY, 0)
      , 2) AS Invest_YOY_PCT

    , ROUND(
        100.0 * (Lifestyle - Lifestyle_LY)
        / NULLIF(Lifestyle_LY, 0)
      , 2) AS Lifestyle_YOY_PCT

    , ROUND(
        100.0 * (LocalParts - LocalParts_LY)
        / NULLIF(LocalParts_LY, 0)
      , 2) AS LocalParts_YOY_PCT

    , ROUND(
        100.0 * (RiderEquipment - RiderEquipment_LY)
        / NULLIF(RiderEquipment_LY, 0)
      , 2) AS RiderEquipment_YOY_PCT

    , ROUND(
        100.0 * (TechnicalDocumentationAccessories - TechnicalDocumentationAccessories_LY)
        / NULLIF(TechnicalDocumentationAccessories_LY, 0)
      , 2) AS TechnicalDocumentationAccessories_YOY_PCT

    , ROUND(
        100.0 * (TechnicalTuningAccessories - TechnicalTuningAccessories_LY)
        / NULLIF(TechnicalTuningAccessories_LY, 0)
      , 2) AS TechnicalTuningAccessories_YOY_PCT

    , ROUND(
        100.0 * (Unknown - Unknown_LY)
        / NULLIF(Unknown_LY, 0)
      , 2) AS Unknown_YOY_PCT

FROM base

WHERE region IN ('UK IR');
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
    print(f"[yoy_comparison] Query submitted → {execution_id}")

    for attempt in range(MAX_POLL_ATTEMPTS):
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        print(f"[yoy_comparison] Attempt {attempt + 1}: state={state}, waiting…")
        time.sleep(POLL_INTERVAL_SEC)
    else:
        raise TimeoutError(f"Query {execution_id} did not complete within the poll limit.")

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena {state}: {reason}")

    return execution_id


def lambda_handler(event, context):
    log_event("yoy_comparison", event)
    try:
        execution_id   = _run_athena_query(SQL_QUERY)
        output_s3_path = f"{S3_OUTPUT_LOCATION}{execution_id}.csv"
        print(f"[yoy_comparison] SUCCESS → {output_s3_path}")
        return success_response({
            "query_name":         "yoy_comparison",
            "query_execution_id": execution_id,
            "output_s3_path":     output_s3_path,
        })
    except Exception as exc:
        return error_response(f"yoy_comparison failed: {exc}")
