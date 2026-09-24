import boto3
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))
from shared.config import DEALER_TABLE_NAME

dynamo  = boto3.resource("dynamodb", region_name="eu-central-1")
bedrock = boto3.client("bedrock-runtime", region_name="eu-central-1")
s3      = boto3.client("s3")

BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "eu.anthropic.claude-haiku-4-5-20251001-v1:0")
RESULTS_BUCKET   = os.environ.get("RESULTS_BUCKET", "dibmw-dev-athena-results-bucket")

_BENCH_MR_PREFIX  = "results/benchmark/benchmark_market_region/"
_BENCH_MRS_PREFIX = "results/benchmark/benchmark_market_region_segment/"

_SYSTEM = (
    "You are a BMW IAM (Independent Aftermarket) sales coach helping field reps prepare "
    "for dealer visits. Analyse KPI data and respond with valid JSON only — "
    "no markdown fences, no text outside the JSON object."
)

# Module-level caches — populated once per container lifecycle
_metadata: dict = {}
_bench_mr:  list = []
_bench_mrs: list = []
_benchmarks_loaded = False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_float(v):
    try:
        return float(v) if v not in (None, "", "null") else None
    except (ValueError, TypeError):
        return None


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


# ── Metadata loading ──────────────────────────────────────────────────────────

def _load_metadata() -> dict:
    global _metadata
    if _metadata:
        return _metadata
    candidates = [
        "/opt/python/shared/metadata",
        os.path.join(os.path.dirname(__file__), "../../shared/python/shared/metadata"),
    ]
    metadata_dir = next((p for p in candidates if os.path.isdir(p)), None)
    if not metadata_dir:
        print("[insights_generator] metadata directory not found — proceeding without")
        return {}
    for fname in os.listdir(metadata_dir):
        if fname.endswith(".json"):
            key = fname[:-5]
            with open(os.path.join(metadata_dir, fname)) as f:
                _metadata[key] = json.load(f)
    print(f"[insights_generator] Loaded metadata for: {sorted(_metadata.keys())}")
    return _metadata


# ── Benchmark loading via S3 Select ──────────────────────────────────────────

def _s3_select_parquet(prefix: str) -> list[dict]:
    """Read all Parquet part files under an S3 prefix using S3 Select."""
    records = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=RESULTS_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            try:
                resp = s3.select_object_content(
                    Bucket=RESULTS_BUCKET,
                    Key=key,
                    ExpressionType="SQL",
                    Expression="SELECT * FROM S3Object",
                    InputSerialization={"Parquet": {}},
                    OutputSerialization={"JSON": {"RecordDelimiter": "\n"}},
                )
                for event in resp["Payload"]:
                    if "Records" in event:
                        chunk = event["Records"]["Payload"].decode("utf-8")
                        for line in chunk.strip().splitlines():
                            if line.strip():
                                records.append(json.loads(line))
            except Exception as exc:
                print(f"[insights_generator] S3 Select failed for {key}: {exc}")
    return records


def _load_benchmarks():
    global _bench_mr, _bench_mrs, _benchmarks_loaded
    if _benchmarks_loaded:
        return
    _benchmarks_loaded = True
    try:
        _bench_mr  = _s3_select_parquet(_BENCH_MR_PREFIX)
        _bench_mrs = _s3_select_parquet(_BENCH_MRS_PREFIX)
        print(f"[insights_generator] Benchmarks loaded — MR: {len(_bench_mr)} rows, MRS: {len(_bench_mrs)} rows")
    except Exception as exc:
        print(f"[insights_generator] Benchmark load failed: {exc}")


def _aggregate_by_category(rows: list[dict]) -> list[dict]:
    """Average numeric benchmark metrics across rows sharing the same aftersales_category."""
    buckets: dict = defaultdict(lambda: defaultdict(list))
    for r in rows:
        cat = r.get("aftersales_category", "")
        if not cat:
            continue
        for col in ("avg_sales_per_dealer", "median_sales_per_dealer", "avg_customers_per_dealer"):
            v = _safe_float(r.get(col))
            if v is not None:
                buckets[cat][col].append(v)
    result = []
    for cat in sorted(buckets):
        entry = {"aftersales_category": cat}
        for col, vals in buckets[cat].items():
            entry[col] = round(sum(vals) / len(vals), 2)
        result.append(entry)
    return result


def _get_benchmark(country: str, segment: str) -> dict:
    if not country:
        return {}

    # Preferred: match on country + segment (benchmark_market_region_segment)
    matched_mrs = [
        r for r in _bench_mrs
        if r.get("country") == country and r.get("segment") == segment
    ]
    if matched_mrs:
        return {
            "benchmark_type": "market+segment",
            "country": country,
            "segment": segment,
            "note": "Averages aggregated across regions — same country and segment tier",
            "by_category": _aggregate_by_category(matched_mrs),
        }

    # Fallback: country only (benchmark_market_region)
    matched_mr = [r for r in _bench_mr if r.get("country") == country]
    if matched_mr:
        return {
            "benchmark_type": "market_only",
            "country": country,
            "note": "Averages aggregated across all regions in same country (no segment filter)",
            "by_category": _aggregate_by_category(matched_mr),
        }

    return {}


# ── Prompt construction ───────────────────────────────────────────────────────

def _format_metadata_rules(metadata: dict) -> str:
    dataset_map = [
        ("mom_decline",                "MoM Category Decline"),
        ("high_turnover_low_activity", "High Turnover / Low Activity"),
        ("sale_revenue_vs_target",     "Sale Revenue vs Target"),
        ("purchase_revenue_vs_target", "Purchase Revenue vs Target"),
        ("abc_segmentation",           "ABC Segmentation"),
        ("customer_trend",             "Customer Count Trend"),
        ("revenue_yoy",                "Revenue Year-over-Year"),
    ]
    sections = []
    for key, label in dataset_map:
        instructions = metadata.get(key, {}).get("llm_instructions", [])
        if instructions:
            rules = "\n".join(f"  - {r}" for r in instructions)
            sections.append(f"[{label}]\n{rules}")
    return "\n\n".join(sections)


def _build_prompt(dealer_code: str, dealer_name: str, kpis: dict, metadata: dict, benchmark: dict) -> str:
    mom      = kpis.get("mom_decline", {})
    ht       = kpis.get("high_turnover_low_activity", {})
    sale_rvt = kpis.get("sale_revenue_vs_target", {})
    pur_rvt  = kpis.get("purchase_revenue_vs_target", {})
    abc      = kpis.get("abc_segmentation", {})
    cust     = kpis.get("customer_trend", {})
    rev_yoy  = kpis.get("revenue_yoy", {})
    yoy      = kpis.get("yoy_comparison", {})

    metadata_rules  = _format_metadata_rules(metadata)
    benchmark_block = json.dumps(benchmark, indent=2) if benchmark else "Not available"
    bench_context   = (
        f"same country and segment tier ({benchmark.get('country')}, Segment {benchmark.get('segment')})"
        if benchmark.get("benchmark_type") == "market+segment"
        else f"same country ({benchmark.get('country')}, all segments)"
        if benchmark.get("benchmark_type") == "market_only"
        else "not available"
    )

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
  "summary": "<Write 4-5 sentences covering the dealer's full performance picture. Include: (1) revenue achievement vs target with M1_AchvPct percentage and whether the dealer is on track, (2) ABC segment and what it means for their priority tier, (3) customer count trend over the last 3 months and whether the base is growing or shrinking, (4) YoY revenue direction with cy vs ly figures, (5) overall risk level and urgency for this visit.>",
  "pitch": "<Write 4-5 structured talking points for the field rep to use during the visit. Each point should: name the specific issue, give the supporting data number, and suggest a concrete action or question to raise with the dealer. Cover: top declining category, high-turnover/low-activity flag if present, customer trend, revenue gap vs target, and one positive to open or close on.>"
}}

=== DATA INTERPRETATION RULES ===
{metadata_rules}

=== MARKET BENCHMARKS ({bench_context}) ===
{benchmark_block}
Where benchmark data is available, compare the dealer's category-level sales against the avg_sales_per_dealer figures above. Call out categories where the dealer is significantly below the market average.

=== KPI DATA ===

MoM Decline (categories_with_decline_pct lists parts categories with consecutive monthly decline):
{json.dumps(mom, indent=2)}

High Turnover / Low Activity:
{json.dumps(ht, indent=2)}

Sale Revenue vs Target (M1=latest month, M2=middle month, M3=oldest month):
{json.dumps(sale_rvt, indent=2)}

Purchase Revenue vs Target (M1=latest month, M2=middle month, M3=oldest month):
{json.dumps(pur_rvt, indent=2)}

ABC Segmentation (YTD):
{json.dumps(abc, indent=2)}

Customer Trend (customer_count_last_3_months, customer_count_trend: Stable or Declining with avg decline %):
{json.dumps(cust, indent=2)}

Revenue YoY (CY vs LY MTD):
{json.dumps(rev_yoy, indent=2)}

YoY by Parts Category:
{json.dumps(yoy, indent=2)}

Rules:
- top_issues: 2-5 items ordered most critical first; titles must be exact category names from categories_with_decline_pct
- Every sentence must reference actual numbers from the data — no generic statements
- Where benchmark data is available, compare dealer numbers against market averages
- summary and pitch must feel like they were written by an experienced sales coach, not a data report
- Return JSON only — no markdown fences, no text outside the JSON"""


# ── Bedrock ───────────────────────────────────────────────────────────────────

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
    if text.startswith("```"):
        text = text.split("```", 2)[1]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())


# ── Handler ───────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    dealer_code = event.get("dealer_code")
    if not dealer_code:
        return {"statusCode": 400, "body": json.dumps({"error": "dealer_code required"})}

    table = dynamo.Table(DEALER_TABLE_NAME)

    # 1. Fetch KPI record
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

    # 3. Load context (module-level caches — no-op after first invocation per container)
    metadata = _load_metadata()
    _load_benchmarks()

    country   = kpis.get("abc_segmentation", {}).get("country", "")
    segment   = kpis.get("abc_segmentation", {}).get("segment", "")
    benchmark = _get_benchmark(country, segment)

    if benchmark:
        print(f"[insights_generator] Benchmark matched: {benchmark['benchmark_type']} for {country}/{segment}")
    else:
        print(f"[insights_generator] No benchmark match for country={country!r} segment={segment!r}")

    # 4. Generate fresh insights via Bedrock
    prompt   = _build_prompt(dealer_code, dealer_name, kpis, metadata, benchmark)
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

    # 5. Cache in DynamoDB
    table.put_item(Item=_float_to_decimal(item))

    return {"statusCode": 200, "body": json.dumps(item)}
