import boto3
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))
from shared.config import DEALER_TABLE_NAME
from boto3.dynamodb.conditions import Attr, Key

dynamo  = boto3.resource("dynamodb", region_name="eu-central-1")
bedrock = boto3.client("bedrock-runtime", region_name="eu-central-1")
s3      = boto3.client("s3")

VISIT_CAPTURE_TABLE_NAME = os.environ.get("VISIT_CAPTURE_TABLE_NAME", "bmw_visit_capture-dev")

BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "eu.anthropic.claude-haiku-4-5-20251001-v1:0")
RESULTS_BUCKET   = os.environ.get("RESULTS_BUCKET", "dibmw-dev-athena-results-bucket")

# Toggle: feed market-basket (co-purchase) gaps into the pitch prompt. The data is still
# computed by the aggregator and written to DynamoDB regardless — this only controls
# whether it contributes to the generated pitch. Set env INCLUDE_MARKET_BASKET_IN_PITCH=true to re-enable.
INCLUDE_MARKET_BASKET_IN_PITCH = os.environ.get("INCLUDE_MARKET_BASKET_IN_PITCH", "false").lower() == "true"

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


# ── Last visit fetch ─────────────────────────────────────────────────────────

def _get_last_submitted_visit(dealer_code: str) -> dict | None:
    try:
        table = dynamo.Table(VISIT_CAPTURE_TABLE_NAME)
        response = table.query(
            KeyConditionExpression=Key("dealer_code").eq(dealer_code),
            FilterExpression=Attr("status").eq("submitted"),
        )
        items = sorted(response.get("Items", []), key=lambda x: x.get("visit_date", ""), reverse=True)
        return _decimal_to_python(items[0]) if items else None
    except Exception as exc:
        print(f"[insights_generator] Failed to fetch last visit for {dealer_code}: {exc}")
        return None


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
        meta = metadata.get(key, {})
        instructions = meta.get("llm_instructions", [])
        if not instructions:
            continue

        lines = [f"[{label}]"]

        # Reporting-period mapping (e.g. M1 = Current Date - 2 Months) so the model
        # places each month correctly instead of guessing the spacing.
        period = meta.get("reporting_period_logic", {})
        if period:
            period_lines = "\n".join(f"    {k}: {v}" for k, v in period.items())
            lines.append(f"  Reporting period mapping:\n{period_lines}")

        # Monetary unit(s) declared on the metrics, so revenue is rendered in the
        # right currency rather than being guessed from region.
        units = {
            m["unit"] for m in meta.get("metrics", {}).values()
            if isinstance(m, dict) and m.get("unit") in ("EUR", "GBP", "USD")
        }
        if units:
            lines.append(f"  Monetary unit: all revenue/target/gap figures are in {', '.join(sorted(units))}.")

        rules = "\n".join(f"  - {r}" for r in instructions)
        lines.append(rules)
        sections.append("\n".join(lines))
    return "\n\n".join(sections)


def _top_mix_gaps(mix: dict, top_n: int = 5, min_benchmark: float = 1.0) -> dict:
    """From parts_category_mix_benchmark, surface material under-indexed (opportunity)
    and over-concentrated (risk) categories. gap_pct = country_benchmark - dealer_mix,
    so positive = dealer sells less than peers. Ignore categories the country barely
    sells (min_benchmark) so we don't pitch noise."""
    rows = []
    for cat, m in (mix or {}).items():
        gap   = _safe_float(m.get("gap_pct"))
        bench = _safe_float(m.get("country_benchmark_mix_pct"))
        deal  = _safe_float(m.get("dealer_mix_pct"))
        if gap is None or bench is None or bench < min_benchmark:
            continue
        rows.append({"category": cat, "dealer_mix_pct": deal,
                     "country_benchmark_mix_pct": bench, "gap_pct": gap})
    opportunities = sorted([r for r in rows if r["gap_pct"] > 0],
                           key=lambda r: r["gap_pct"], reverse=True)[:top_n]
    over_concentrated = sorted([r for r in rows if r["gap_pct"] < 0],
                               key=lambda r: r["gap_pct"])[:top_n]
    return {"under_indexed_opportunities": opportunities,
            "over_concentrated_risks": over_concentrated}


def _top_cross_sell_gaps(baskets: list, top_n: int = 6,
                         min_antecedent_baskets: float = 10, min_gap: float = 2.0) -> list:
    """From market_basket_uk_ir, surface the strongest co-purchase gaps on the dealer's
    ORDERS: category pairs where peers who order the first category also order the second
    far more often than this dealer does. (These are dealer purchase/order invoices, not
    end-customer sales.) Filter out low-sample rows (dealer_antecedent_baskets below
    threshold) so we only pitch statistically real gaps."""
    rows = []
    for b in (baskets or []):
        ant_baskets = _safe_float(b.get("dealer_antecedent_baskets")) or 0
        gap         = _safe_float(b.get("confidence_gap_pct"))
        if gap is None or gap < min_gap or ant_baskets < min_antecedent_baskets:
            continue
        rows.append({
            "when_dealer_orders":        b.get("antecedent_category", ""),
            "should_also_order":         b.get("below_benchmark_category", ""),
            "country_co_order_pct":      _safe_float(b.get("country_confidence_pct")),
            "dealer_co_order_pct":       _safe_float(b.get("dealer_confidence_pct")),
            "co_order_gap_pct":          gap,
            "dealer_antecedent_orders":  ant_baskets,
        })
    return sorted(rows, key=lambda r: r["co_order_gap_pct"], reverse=True)[:top_n]


def _format_last_visit_block(visit: dict | None) -> str:
    if not visit:
        return "No previous submitted visit on record."
    fd = visit.get("form_data", {})
    competitor = fd.get("competitor_activity", {})
    competitor_note = (
        competitor.get("comment", "")
        if competitor.get("observed") else "None observed"
    )
    actions = fd.get("actions_agreed", [])
    actions_bullets = "\n".join(f"  - {a}" for a in actions) if actions else "  - None recorded"
    return (
        f"Date: {visit.get('visit_date', 'unknown')}, by: {visit.get('submitted_by', 'unknown')}\n"
        f"- Topics discussed: {fd.get('topics_discussed', '')}\n"
        f"- Biggest challenge: {fd.get('biggest_challenge', '')}\n"
        f"- Biggest opportunity: {fd.get('biggest_opportunity', '')}\n"
        f"- Visit health: {fd.get('overall_status', '')}\n"
        f"- Contact met: {fd.get('contact_met', '')}\n"
        f"- Notes: {fd.get('visit_notes', '')}\n"
        f"- Competitor activity: {competitor_note}\n"
        f"- Actions agreed:\n{actions_bullets}"
    )


def _build_prompt(dealer_code: str, dealer_name: str, kpis: dict, metadata: dict, benchmark: dict, last_visit: dict | None = None, run_date: str = "") -> str:
    mom      = kpis.get("mom_decline", {})
    ht       = kpis.get("high_turnover_low_activity", {})
    sale_rvt = kpis.get("sale_revenue_vs_target", {})
    pur_rvt  = kpis.get("purchase_revenue_vs_target", {})
    abc      = kpis.get("abc_segmentation", {})
    cust     = kpis.get("customer_trend", {})
    rev_yoy  = kpis.get("revenue_yoy", {})
    yoy      = kpis.get("yoy_comparison", {})

    mix_gaps         = _top_mix_gaps(kpis.get("parts_category_mix_benchmark", {}))
    cross_sell_gaps  = (
        _top_cross_sell_gaps(kpis.get("market_basket_uk_ir", []))
        if INCLUDE_MARKET_BASKET_IN_PITCH else []
    )

    metadata_rules     = _format_metadata_rules(metadata)
    benchmark_block    = json.dumps(benchmark, indent=2) if benchmark else "Not available"
    last_visit_block   = _format_last_visit_block(last_visit)
    bench_context   = (
        f"same country and segment tier ({benchmark.get('country')}, Segment {benchmark.get('segment')})"
        if benchmark.get("benchmark_type") == "market+segment"
        else f"same country ({benchmark.get('country')}, all segments)"
        if benchmark.get("benchmark_type") == "market_only"
        else "not available"
    )

    reference_date_line = (
        f"Reference date (treat this as \"Current Date\" when resolving any \"Current Date - N Months\" "
        f"period mapping below): {run_date}\n\n"
        if run_date else ""
    )

    # Market-basket block + its pitch rule are only included when the toggle is on.
    if INCLUDE_MARKET_BASKET_IN_PITCH:
        cross_sell_section = (
            "Co-Purchase Gaps on Dealer Orders (these come from the dealer's own PURCHASE/ORDER invoices, "
            "not end-customer sales — they show what the dealer already orders vs what peers order alongside it; "
            "when_dealer_orders = a category this dealer already orders in real volume, should_also_order = a category "
            "that peers who order the first category also order far more often, country_co_order_pct vs dealer_co_order_pct "
            "= the share of orders containing the first category that also contain the second, for the country vs this dealer, "
            "co_order_gap_pct = country minus dealer (bigger = bigger range-extension opportunity), dealer_antecedent_orders "
            "= how many of the dealer's orders already contain the first category):\n"
            f"{json.dumps(cross_sell_gaps, indent=2)}\n"
        )
        cross_sell_rule = (
            "\n- IF the Co-Purchase Gaps section above is non-empty, include at least ONE pitch item that is a concrete "
            "range-extension play built from it: name the when_dealer_orders → should_also_order pair with the largest "
            "realistic gap, cite country_co_order_pct vs dealer_co_order_pct, and frame it as what the dealer should start "
            "ordering/stocking alongside a line they already buy (e.g. \"N of your orders already include X — peers who order "
            "X add Y to P% of those orders, you're at M%; start pulling Y onto those orders\"). Do NOT frame this as upselling "
            "end customers — it is about the dealer's own ordering/stocking behaviour."
            "\n- If a category shows up as both a mix opportunity AND a co-purchase target (e.g. the same should_also_order "
            "category), lead with it — the two signals reinforce each other."
        )
    else:
        cross_sell_section = ""
        cross_sell_rule = ""

    return f"""Dealer: {dealer_name} (code: {dealer_code})

{reference_date_line}You are preparing a pre-visit intelligence brief for a BMW IAM field representative.
Return a JSON object with exactly these three fields, generated in this order (pitch FIRST — it is the most important and must never be empty):

{{
  "pitch": [
    {{
      "issue": "<short title for this talking point>",
      "data": "<the specific number or metric that supports this point>",
      "action": "<the exact question or action the rep should raise with the dealer>"
    }}
  ],
  "summary": "<Write 4-5 sentences covering the dealer's full performance picture. Include: (1) revenue achievement vs target with M1_AchvPct percentage and whether the dealer is on track, (2) ABC segment and what it means for their priority tier, (3) customer count trend over the last 3 months and whether the base is growing or shrinking, (4) YoY revenue direction with cy vs ly figures, (5) overall risk level and urgency for this visit.>",
  "top_issues": [
    {{
      "num": 1,
      "title": "<exact category name from categories_with_decline_pct>",
      "root_cause": "<2-3 sentences: explain WHY this decline likely happened — consider seasonality, competitor activity, stock gaps, lost accounts, or basket issues. Be specific to the numbers.>",
      "impact": "<2-3 sentences: explain the business consequence if unaddressed — revenue risk, customer churn, quarter-end exposure. Quantify where possible.>"
    }}
  ]
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

Parts Category Mix vs Country (where this dealer's SALES MIX is under- or over-indexed vs the country benchmark; gap_pct = country_benchmark_mix_pct - dealer_mix_pct, so a positive gap means the dealer sells proportionally LESS of that category than peers — a growth opportunity; a negative gap means the dealer is over-concentrated in that category — a dependency risk):
{json.dumps(mix_gaps, indent=2)}

{cross_sell_section}
=== LAST VISIT ===
{last_visit_block}
Use the last visit to make pitch points forward-looking — what should the rep follow up on in the next visit. Reference open actions, unresolved challenges, and stated opportunities.

Rules:
- top_issues: 2-5 items ordered most critical first; titles must be exact category names from categories_with_decline_pct
- pitch: 5-6 items; each must have issue (short title), data (specific metric), and action (exact question or action for the rep); cover top declining category, high-turnover/low-activity flag if present, customer trend, revenue gap vs target, and one positive to open or close on
- pitch must never be empty; always return at least 4 items even if some data sections are unavailable.
- IF the Parts Category Mix section above is non-empty, include at least ONE pitch item that is a concrete upsell built from it: name a category where the dealer is under-indexed vs the country (positive gap_pct with a material benchmark share), cite dealer_mix_pct vs country_benchmark_mix_pct, and frame it as upside.{cross_sell_rule}
- Flag any over_concentrated_risks category (large negative gap_pct) in the summary or an issue as a dependency risk.
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
            "max_tokens": 6000,
            "system": _SYSTEM,
            "messages": [{"role": "user", "content": prompt}],
        }),
    )
    payload = json.loads(response["body"].read())
    stop_reason = payload.get("stop_reason")
    # Join every text block (some responses split across blocks; taking [0] alone
    # would silently drop the tail — including the pitch).
    text = "".join(
        block.get("text", "")
        for block in payload.get("content", [])
        if block.get("type") == "text"
    ).strip()
    print(f"[insights_generator] Bedrock stop_reason={stop_reason}, output_chars={len(text)}")

    # If the model hit the token ceiling the JSON is truncated — surface a clear
    # cause instead of a cryptic "Unterminated string" from json.loads.
    if stop_reason == "max_tokens":
        raise ValueError("Bedrock response truncated at max_tokens — increase max_tokens")
    if text.startswith("```"):
        text = text.split("```", 2)[1]
        if text.startswith("json"):
            text = text[4:]
    parsed = json.loads(text.strip())
    if not parsed.get("pitch"):
        print(f"[insights_generator] WARNING empty pitch — raw model output: {text[:4000]}")
    return parsed


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

    # 4. Fetch last submitted visit for this dealer
    last_visit = _get_last_submitted_visit(dealer_code)

    # 5. Generate fresh insights via Bedrock
    prompt = _build_prompt(dealer_code, dealer_name, kpis, metadata, benchmark, last_visit, run_date)
    try:
        insights = _invoke_bedrock(prompt)
    except Exception as exc:
        print(f"[insights_generator] Bedrock invocation failed: {exc}")
        return {"statusCode": 502, "body": json.dumps({"error": "Bedrock invocation failed", "detail": str(exc)})}

    last_visit_meta = {}
    if last_visit:
        fd = last_visit.get("form_data", {})
        last_visit_meta = {
            "visit_date":          last_visit.get("visit_date"),
            "overall_status":      fd.get("overall_status"),
            "biggest_challenge":   fd.get("biggest_challenge"),
            "biggest_opportunity": fd.get("biggest_opportunity"),
            "contact_met":         fd.get("contact_met"),
        }

    item = {
        "dealer_code":     dealer_code,
        "sk":              "INSIGHTS_LATEST",
        "run_date":        run_date,
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "top_issues":      insights.get("top_issues", []),
        "summary":         insights.get("summary", ""),
        "pitch":           insights.get("pitch", ""),
        "last_visit_meta": last_visit_meta,
    }

    # 5. Cache in DynamoDB
    table.put_item(Item=_float_to_decimal(item))

    return {"statusCode": 200, "body": json.dumps(item)}
