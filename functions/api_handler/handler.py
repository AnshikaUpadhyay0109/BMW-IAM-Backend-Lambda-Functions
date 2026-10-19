import boto3
import json
import os
import sys
from decimal import Decimal
from typing import Any

from boto3.dynamodb.conditions import Attr
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))
from shared.config import RESULTS_BUCKET, DEALER_TABLE_NAME

# ── AWS clients (module-level — reused across warm Lambda invocations) ─────────
s3             = boto3.client("s3")
sfn            = boto3.client("stepfunctions", region_name="eu-central-1")
dynamo         = boto3.resource("dynamodb", region_name="eu-central-1")
lambda_client  = boto3.client("lambda", region_name="eu-central-1")

LATEST_KEY             = "processed/latest_combined.json"
PIPELINE_ARN           = os.environ.get("PIPELINE_ARN", "")
API_ROOT_PATH          = os.environ.get("API_ROOT_PATH", "/Prod")
INSIGHTS_GENERATOR_ARN = os.environ.get("INSIGHTS_GENERATOR_ARN", "")

# ── FastAPI app ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="DIBMW Dealer Analytics API",
    description="Per-dealer KPI data from Athena pipelines — ABC segmentation, MoM decline, YoY comparison.",
    version="1.0.0",
    root_path=API_ROOT_PATH,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _decimal_to_float(obj: Any) -> Any:
    """Recursively convert Decimal (returned by DynamoDB) back to float for JSON."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decimal_to_float(i) for i in obj]
    return obj


def _read_latest_s3() -> dict:
    try:
        obj = s3.get_object(Bucket=RESULTS_BUCKET, Key=LATEST_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        raise HTTPException(
            status_code=404,
            detail="No results available yet — trigger the pipeline first via POST /trigger",
        )


# Only the small, flat KPI sub-maps the overview list needs — deliberately excludes the
# heavy per-dealer blobs (market_basket_uk_ir, parts_category_mix_benchmark) so the list
# response stays well under Lambda's 6 MB payload cap regardless of dealer count.
_LIST_PROJECTION = (
    "dealer_code, sk, run_date, dealer_name, "
    "kpis.abc_segmentation, kpis.revenue_yoy, kpis.high_turnover_low_activity, "
    "kpis.comp_customer_count, kpis.sale_revenue_vs_target, kpis.customer_trend"
)


def _dealer_summary(item: dict) -> dict:
    """Shape one DynamoDB dealer record into a lightweight overview row.
    Full detail (including market basket + category mix) is served by
    GET /dealers/{dealer_code}."""
    kpis = item.get("kpis", {})
    abc  = kpis.get("abc_segmentation", {})
    rev  = kpis.get("revenue_yoy", {})
    htla = kpis.get("high_turnover_low_activity", {})
    comp = kpis.get("comp_customer_count", {})
    svt  = kpis.get("sale_revenue_vs_target", {})
    cust = kpis.get("customer_trend", {})
    return {
        "dealer_code":                item.get("dealer_code"),
        "dealer_name":                item.get("dealer_name"),
        "run_date":                   item.get("run_date"),
        "country":                    abc.get("country"),
        "segment":                    abc.get("segment"),
        "ytd_sales_eur":              abc.get("ytd_sales_eur"),
        "dealer_health_tag":          rev.get("dealer_health_tag"),
        "sales_performance_tag":      rev.get("sales_performance_tag"),
        "sales_growth_decline_pct":   rev.get("sales_growth_decline_pct"),
        "dealer_segment":             htla.get("dealer_segment"),
        "purchase_risk_level":        htla.get("purchase_risk_level"),
        "customer_growth_health_tag": comp.get("customer_growth_health_tag"),
        "m1_achv_pct":                svt.get("M1_AchvPct"),
        "customer_count_trend":       cust.get("customer_count_trend"),
    }


# ── DynamoDB routes ───────────────────────────────────────────────────────────

@app.get("/dealers", summary="List all dealers with lightweight overview KPIs")
def get_all_dealers():
    """
    Returns a lightweight summary row per dealer (SK=LATEST) for the priority list /
    overview screen. Heavy per-dealer detail (market basket, category mix, full KPI maps)
    is intentionally excluded to keep the response under Lambda's 6 MB payload cap —
    fetch it per dealer via GET /dealers/{dealer_code}.
    """
    from botocore.exceptions import ClientError
    try:
        table    = dynamo.Table(DEALER_TABLE_NAME)
        response = table.scan(
            FilterExpression=Attr("sk").eq("LATEST"),
            ProjectionExpression=_LIST_PROJECTION,
        )
        items    = response.get("Items", [])

        while "LastEvaluatedKey" in response:
            response = table.scan(
                FilterExpression=Attr("sk").eq("LATEST"),
                ProjectionExpression=_LIST_PROJECTION,
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            items.extend(response.get("Items", []))
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ResourceNotFoundException":
            raise HTTPException(
                status_code=404,
                detail="Dealer table not found — run the pipeline first via POST /trigger",
            )
        raise HTTPException(status_code=500, detail=f"DynamoDB error: {code}")

    dealers = [_dealer_summary(_decimal_to_float(item)) for item in items]
    return {"total": len(dealers), "dealers": dealers}


@app.get("/dealers/{dealer_code}", summary="Get a single dealer's full KPI profile")
def get_dealer(dealer_code: str):
    """
    Fetch one dealer by dealer_code.
    Use this for the dealer deep-dive screen on the frontend.
    Returns 404 if the dealer_code doesn't exist in the current run's data.
    """
    table  = dynamo.Table(DEALER_TABLE_NAME)
    result = table.get_item(Key={"dealer_code": dealer_code, "sk": "LATEST"})
    item   = result.get("Item")

    if not item:
        raise HTTPException(status_code=404, detail=f"Dealer '{dealer_code}' not found")

    return _decimal_to_float(item)


# ── Aggregate S3 routes (all-dealers summaries) ───────────────────────────────

@app.get("/results", summary="All 3 KPI summaries combined (S3)")
def get_all_results():
    """Returns the full combined JSON written by result_aggregator (all dealers, all KPIs)."""
    return _read_latest_s3()

@app.get("/results/abc-segmentation", summary="QTD dealer achievement summary")
def get_abc():
    data = _read_latest_s3()
    return {"processed_at": data.get("processed_at"), "abc_segmentation": data.get("abc_segmentation")}


@app.get("/results/mom-decline", summary="MoM parts category sales decline summary")
def get_mom():
    data = _read_latest_s3()
    return {"processed_at": data.get("processed_at"), "mom_decline": data.get("mom_decline")}


@app.get("/results/yoy-comparison", summary="YoY parts category sales comparison summary")
def get_yoy():
    data = _read_latest_s3()
    return {"processed_at": data.get("processed_at"), "yoy_comparison": data.get("yoy_comparison")}


@app.get("/results/revenue-yoy", summary="CY vs LY MTD dealer revenue comparison")
def get_revenue_yoy():
    data = _read_latest_s3()
    return {"processed_at": data.get("processed_at"), "revenue_yoy": data.get("revenue_yoy")}


@app.get("/results/customer-trend", summary="Monthly customer count trend (Apr–Jun) per dealer")
def get_customer_trend():
    data = _read_latest_s3()
    return {"processed_at": data.get("processed_at"), "customer_trend": data.get("customer_trend")}


@app.get("/results/high-turnover-low-activity", summary="Dealers with high turnover but low invoice frequency or poor recency")
def get_high_turnover_low_activity():
    data = _read_latest_s3()
    return {"processed_at": data.get("processed_at"), "high_turnover_low_activity": data.get("high_turnover_low_activity")}


@app.get("/results/sale-revenue-vs-target", summary="Sale revenue actual vs target per dealer (3-month rolling)")
def get_sale_revenue_vs_target():
    data = _read_latest_s3()
    return {"processed_at": data.get("processed_at"), "sale_revenue_vs_target": data.get("sale_revenue_vs_target")}


@app.get("/results/purchase-revenue-vs-target", summary="Purchase revenue actual vs target per dealer (3-month rolling)")
def get_purchase_revenue_vs_target():
    data = _read_latest_s3()
    return {"processed_at": data.get("processed_at"), "purchase_revenue_vs_target": data.get("purchase_revenue_vs_target")}


# ── AI Insights ──────────────────────────────────────────────────────────────

@app.get("/dealers/{dealer_code}/insights", summary="Get AI-generated insights for a dealer")
def get_dealer_insights(dealer_code: str):
    """
    Returns top_issues, summary, and pitch for the dealer.
    Results are cached in DynamoDB (sk=INSIGHTS_LATEST) keyed by run_date.
    Cache hit: ~200 ms (DynamoDB read). Cache miss: triggers Bedrock generation (~10-20 s).
    """
    table = dynamo.Table(DEALER_TABLE_NAME)

    # Fetch KPI record to know the current run_date
    latest = table.get_item(Key={"dealer_code": dealer_code, "sk": "LATEST"}).get("Item")
    if not latest:
        raise HTTPException(status_code=404, detail=f"Dealer '{dealer_code}' not found")

    run_date = latest.get("run_date", "")

    # Return cached insights when run_date still matches
    cached = table.get_item(Key={"dealer_code": dealer_code, "sk": "INSIGHTS_LATEST"}).get("Item")
    if cached and cached.get("run_date") == run_date:
        return _decimal_to_float(cached)

    # Cache miss — invoke the insights generator Lambda synchronously
    if not INSIGHTS_GENERATOR_ARN:
        raise HTTPException(status_code=503, detail="INSIGHTS_GENERATOR_ARN not configured")

    resp = lambda_client.invoke(
        FunctionName=INSIGHTS_GENERATOR_ARN,
        InvocationType="RequestResponse",
        Payload=json.dumps({"dealer_code": dealer_code}),
    )

    if resp.get("FunctionError"):
        raise HTTPException(status_code=502, detail="Insights generation failed — check InsightsGenerator logs")

    payload = json.loads(resp["Payload"].read())
    if payload.get("statusCode") != 200:
        body = json.loads(payload.get("body", "{}"))
        raise HTTPException(status_code=502, detail=body.get("error", "Insights generation failed"))

    return json.loads(payload["body"])


# ── Pipeline trigger ──────────────────────────────────────────────────────────

@app.post("/trigger", status_code=202, summary="Trigger a fresh pipeline run")
def trigger_pipeline():
    """
    Starts a new Step Functions execution — all 3 Athena queries run in parallel.
    Results will be available in DynamoDB and S3 after ~2-3 minutes.
    Returns the execution ARN so the frontend can poll for completion if needed.
    """
    if not PIPELINE_ARN:
        raise HTTPException(status_code=500, detail="PIPELINE_ARN environment variable not configured")

    resp = sfn.start_execution(stateMachineArn=PIPELINE_ARN, input="{}")
    return {
        "message":       "Pipeline triggered — results will be ready in a few minutes",
        "execution_arn": resp["executionArn"],
    }


# ── Mangum adapter — translates API Gateway events ↔ FastAPI/ASGI ─────────────
# api_gateway_base_path sets ASGI root_path so Swagger UI fetches /Prod/openapi.json
# instead of /openapi.json, which 403s at API Gateway without the stage prefix.
# lifespan="off" because Lambda is stateless; startup/shutdown hooks don't apply.
lambda_handler = Mangum(app, lifespan="off", api_gateway_base_path=os.environ.get("API_ROOT_PATH", "/Prod"))
