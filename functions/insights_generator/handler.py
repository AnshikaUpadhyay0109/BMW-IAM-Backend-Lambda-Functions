import boto3
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))
from shared.config import DEALER_TABLE_NAME

dynamo  = boto3.resource("dynamodb", region_name="eu-central-1")
bedrock = boto3.client("bedrock-runtime", region_name="eu-central-1")

BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "eu.anthropic.claude-haiku-4-5-20251001-v1:0")

_SYSTEM = (
    "You are a BMW IAM (Independent Aftermarket) sales coach helping field reps prepare "
    "for dealer visits. Analyse KPI data and respond with valid JSON only — "
    "no markdown fences, no text outside the JSON object."
)


def _decimal_to_python(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _decimal_to_python(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decimal_to_python(i) for i in obj]
    return obj


def _float_to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _float_to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_float_to_decimal(i) for i in obj]
    return obj


def _build_prompt(dealer_code: str, dealer_name: str, kpis: dict) -> str:
    mom      = kpis.get("mom_decline", {})
    ht       = kpis.get("high_turnover_low_activity", {})
    sale_rvt = kpis.get("sale_revenue_vs_target", {})
    abc      = kpis.get("abc_segmentation", {})
    cust     = kpis.get("customer_trend", {})
    rev_yoy  = kpis.get("revenue_yoy", {})
    yoy      = kpis.get("yoy_comparison", {})

    return f"""Dealer: {dealer_name} (code: {dealer_code})

You are preparing a pre-visit intelligence brief for a BMW IAM field representative.
Return a JSON object with exactly these three fields:

{{
  "top_issues": [
    {{
      "num": 1,
      "title": "<exact category name from categories_with_decline_pct>",
      "root_cause": "<2-3 sentences: explain WHY this decline likely happened — consider seasonality, competitor activity, stock gaps, lost accounts, or basket issues. Be specific to the numbers.>",
      "impact": "<2-3 sentences: explain the business consequence if unaddressed — revenue risk, customer churn, quarter-end exposure. Quantify where possible.>"
    }}
  ],
  "summary": "<Write 4-5 sentences covering the dealer's full performance picture. Include: (1) revenue achievement vs target with M2_AchvPct percentage and whether the dealer is on track, (2) ABC segment and what it means for their priority tier, (3) customer count trend over Apr-Jun and whether the base is growing or shrinking, (4) YoY revenue direction with cy vs ly figures, (5) overall risk level and urgency for this visit.>",
  "pitch": "<Write 4-5 structured talking points for the field rep to use during the visit. Each point should: name the specific issue, give the supporting data number, and suggest a concrete action or question to raise with the dealer. Cover: top declining category, high-turnover/low-activity flag if present, customer trend, revenue gap vs target, and one positive to open or close on.>"
}}

MoM Decline (categories_with_decline_pct lists parts categories with consecutive monthly decline):
{json.dumps(mom, indent=2)}

High Turnover / Low Activity:
{json.dumps(ht, indent=2)}

Sale Revenue vs Target (M1=oldest, M2=current month, M3=next):
{json.dumps(sale_rvt, indent=2)}

ABC Segmentation (YTD):
{json.dumps(abc, indent=2)}

Customer Trend (Apr–Jun 2026):
{json.dumps(cust, indent=2)}

Revenue YoY (CY vs LY MTD):
{json.dumps(rev_yoy, indent=2)}

YoY by Parts Category:
{json.dumps(yoy, indent=2)}

Rules:
- top_issues: 2–5 items ordered most critical first; titles must be exact category names from categories_with_decline_pct
- Every sentence must reference actual numbers from the data — no generic statements
- summary and pitch must feel like they were written by an experienced sales coach, not a data report
- Return JSON only — no markdown fences, no text outside the JSON"""


def _invoke_bedrock(prompt: str) -> dict:
    response = bedrock.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 2048,
            "system": _SYSTEM,
            "messages": [{"role": "user", "content": prompt}],
        }),
    )
    text = json.loads(response["body"].read())["content"][0]["text"].strip()
    # strip accidental markdown fences
    if text.startswith("```"):
        text = text.split("```", 2)[1]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())


def lambda_handler(event, context):
    dealer_code = event.get("dealer_code")
    if not dealer_code:
        return {"statusCode": 400, "body": json.dumps({"error": "dealer_code required"})}

    table = dynamo.Table(DEALER_TABLE_NAME)

    # 1. Fetch KPI record — determines current run_date
    latest = table.get_item(Key={"dealer_code": dealer_code, "sk": "LATEST"}).get("Item")
    if not latest:
        return {"statusCode": 404, "body": json.dumps({"error": f"Dealer '{dealer_code}' not found"})}

    run_date    = latest.get("run_date", "")
    dealer_name = latest.get("dealer_name", dealer_code)
    kpis        = _decimal_to_python(latest.get("kpis", {}))

    # 2. Return cached insights if run_date still matches
    cached = table.get_item(Key={"dealer_code": dealer_code, "sk": "INSIGHTS_LATEST"}).get("Item")
    if cached and cached.get("run_date") == run_date:
        return {"statusCode": 200, "body": json.dumps(_decimal_to_python(cached))}

    # 3. Generate fresh insights via Bedrock
    prompt   = _build_prompt(dealer_code, dealer_name, kpis)
    insights = _invoke_bedrock(prompt)

    item = {
        "dealer_code":  dealer_code,
        "sk":           "INSIGHTS_LATEST",
        "run_date":     run_date,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "top_issues":   insights.get("top_issues", []),
        "summary":      insights.get("summary", ""),
        "pitch":        insights.get("pitch", ""),
    }

    # 4. Cache in DynamoDB
    table.put_item(Item=_float_to_decimal(item))

    return {"statusCode": 200, "body": json.dumps(item)}
