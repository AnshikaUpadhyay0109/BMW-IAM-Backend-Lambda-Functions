import boto3
import csv
import io
import json
import sys
import os
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))
from shared.config import RESULTS_BUCKET, PROCESSED_PREFIX, DEALER_TABLE_NAME
from shared.utils import success_response, error_response, log_event

s3     = boto3.client("s3")
dynamo = boto3.resource("dynamodb", region_name="eu-central-1")

CATEGORIES = [
    "ComfortSafetyAccessories", "TransportSolutionsAccessories", "SpecialCasesParts",
    "Maintenance", "Motorsport", "Accident", "BMWGroupClassicMotorcycle",
    "BMWGroupClassicAutomobile", "Repair", "WheelsAndTires", "ExteriorDesignAccessories",
    "MotorcycleEquipment", "Electronics", "Wear", "SmallParts",
    "ChemicalProductsAccessories", "Unknown",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_s3_path(s3_path: str) -> tuple[str, str]:
    path = s3_path.replace("s3://", "")
    bucket, _, key = path.partition("/")
    return bucket, key


def _read_csv(s3_path: str) -> list[dict]:
    bucket, key = _parse_s3_path(s3_path)
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")
    return list(csv.DictReader(io.StringIO(content)))


def _safe_float(v) -> float | None:
    try:
        return float(v) if v not in (None, "", "null") else None
    except (ValueError, TypeError):
        return None


def _to_dynamo(obj):
    """Recursively convert floats→Decimal and drop None values (DynamoDB requirements)."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _to_dynamo(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_to_dynamo(i) for i in obj if i is not None]
    return obj


# ── S3 summary processors ─────────────────────────────────────────────────────

def _process_abc(rows: list[dict]) -> dict:
    """YTD NTILE(3) segmentation — A/B/C dealer tiers."""
    dealers = [
        {
            "dealer_code":   row.get("dealer_code", ""),
            "dealer_name":   row.get("dealer_name", ""),
            "country":       row.get("country", ""),
            "ytd_sales_eur": _safe_float(row.get("ytd_sales_eur")),
            "quantile":      row.get("quantile"),
            "segment":       row.get("segment", ""),
        }
        for row in rows
    ]
    return {
        "total_dealers": len(dealers),
        "segment_A":     [d for d in dealers if d["segment"] == "A"],
        "segment_B":     [d for d in dealers if d["segment"] == "B"],
        "segment_C":     [d for d in dealers if d["segment"] == "C"],
        "all_dealers":   dealers,
    }


def _process_mom(rows: list[dict]) -> dict:
    with_declines, no_decline = [], 0
    for row in rows:
        decline_str = row.get("Categories_With_Decline_Pct", "None")
        if decline_str and decline_str != "None":
            with_declines.append({
                "dealer_code":                 row.get("dealer_code", ""),
                "decline_category_count":      decline_str.count(":"),
                "categories_with_decline_pct": decline_str,
            })
        else:
            no_decline += 1
    top_declining = sorted(
        with_declines, key=lambda d: d["decline_category_count"], reverse=True
    )[:5]
    return {
        "total_dealers":           len(rows),
        "dealers_with_declines":   len(with_declines),
        "dealers_no_decline":      no_decline,
        "top_5_declining_dealers": top_declining,
        "all_dealers": [
            {
                "dealer_code":                 r.get("dealer_code", ""),
                "categories_with_decline_pct": r.get("Categories_With_Decline_Pct", "None"),
            }
            for r in rows
        ],
    }


def _process_yoy(rows: list[dict]) -> dict:
    cat_yoy: dict[str, list[float]] = {cat: [] for cat in CATEGORIES}
    for row in rows:
        for cat in CATEGORIES:
            v = _safe_float(row.get(f"{cat}_YOY_PCT"))
            if v is not None:
                cat_yoy[cat].append(v)
    cat_avg = {
        cat: round(sum(vals) / len(vals), 2) if vals else None
        for cat, vals in cat_yoy.items()
    }
    sorted_cats = sorted(
        [(c, v) for c, v in cat_avg.items() if v is not None],
        key=lambda x: x[1], reverse=True,
    )
    return {
        "total_dealers":          len(rows),
        "best_5_categories_yoy":  [{"category": c, "avg_yoy_pct": v} for c, v in sorted_cats[:5]],
        "worst_5_categories_yoy": [{"category": c, "avg_yoy_pct": v} for c, v in sorted_cats[-5:]],
        "category_avg_yoy":       cat_avg,
        "all_dealers": [
            {
                "dealer_code": r.get("dealer_code", ""),
                **{f"{cat}_YOY_PCT": _safe_float(r.get(f"{cat}_YOY_PCT")) for cat in CATEGORIES},
            }
            for r in rows
        ],
    }


def _process_high_turnover_low_activity(rows: list[dict]) -> dict:
    dealers = [
        {
            "dealer_code":             row.get("dealer_code", ""),
            "country":                 row.get("country", ""),
            "turnover_score":          _safe_float(row.get("turnover_score")),
            "purchase_growth_pct":     _safe_float(row.get("purchase_growth_pct")),
            "sales_growth_pct":        _safe_float(row.get("sales_growth_pct")),
            "invoice_growth_pct":      _safe_float(row.get("invoice_growth_pct")),
            "turnover_tag":            row.get("turnover_tag", ""),
            "activity_tag":            row.get("activity_tag", ""),
            "invoice_momentum_tag":    row.get("invoice_momentum_tag", ""),
            "dealer_segment":          row.get("dealer_segment", ""),
            "ai_opportunity_tag":      row.get("ai_opportunity_tag", ""),
            "purchase_risk_level":     row.get("purchase_risk_level", ""),
        }
        for row in rows
    ]
    flagged = [d for d in dealers if d["dealer_segment"] == "High Turnover, Low Activity"]
    return {
        "total_dealers": len(dealers),
        "flagged_count": len(flagged),
        "flagged":       flagged,
        "all_dealers":   dealers,
    }


def _process_revenue_yoy(rows: list[dict]) -> dict:
    dealers = [
        {
            "dealer_code":              row.get("dealer_code", ""),
            "dealer_name":              row.get("dealer_name", ""),
            "country":                  row.get("country", ""),
            "cy_revenue_eur":           _safe_float(row.get("current_year_sales_eur")),
            "ly_revenue_eur":           _safe_float(row.get("last_year_sales_eur")),
            "sales_gap_eur":            _safe_float(row.get("sales_gap_eur")),
            "sales_growth_decline_pct": _safe_float(row.get("sales_growth_decline_pct")),
            "sales_performance_tag":    row.get("sales_performance_tag", ""),
            "dealer_health_tag":        row.get("dealer_health_tag", ""),
            "comparison_month":         row.get("comparison_month"),
        }
        for row in rows
    ]
    return {"total_dealers": len(dealers), "all_dealers": dealers}


def _process_comp_customer_count(rows: list[dict]) -> dict:
    dealers = [
        {
            "dealer_code":                         r.get("dealer_code", ""),
            "country":                             r.get("country", ""),
            "current_month_customer_count":        _safe_float(r.get("current_month_customer_count")),
            "last_year_same_month_customer_count": _safe_float(r.get("last_year_same_month_customer_count")),
            "growth_degrowth_pct":                 _safe_float(r.get("growth_degrowth_pct")),
            "customer_growth_health_tag":          r.get("customer_growth_health_tag", ""),
        }
        for r in rows
    ]
    critical   = [d for d in dealers if d["customer_growth_health_tag"] == "Critical"]
    monitoring = [d for d in dealers if d["customer_growth_health_tag"] == "Needs Monitoring"]
    stable     = [d for d in dealers if d["customer_growth_health_tag"] == "Stable"]
    return {
        "total_dealers":    len(dealers),
        "critical_count":   len(critical),
        "monitoring_count": len(monitoring),
        "stable_count":     len(stable),
        "all_dealers":      dealers,
    }


def _process_customer_trend(rows: list[dict]) -> dict:
    all_d = [
        {
            "dealer_code":                r.get("dealer_code", ""),
            "customer_count_last_3_months": r.get("customer_count_last_3_months", ""),
            "customer_count_trend":       r.get("customer_count_trend", ""),
        }
        for r in rows
    ]
    declining = [d for d in all_d if d["customer_count_trend"].startswith("Declining")]
    stable    = [d for d in all_d if d["customer_count_trend"] == "Stable"]
    return {
        "total_dealers":   len(all_d),
        "declining_count": len(declining),
        "stable_count":    len(stable),
        "all_dealers":     all_d,
    }


def _process_vs_target(rows: list[dict]) -> dict:
    dealers = [
        {
            "dealer_code": row.get("dealer_code", ""),
            "dealer_name": row.get("dealer_name", ""),
            "M3_Actual":   _safe_float(row.get("M3_Actual")),
            "M3_Target":   _safe_float(row.get("M3_Target")),
            "M3_AchvPct":  _safe_float(row.get("M3_AchvPct")),
            "M2_Actual":   _safe_float(row.get("M2_Actual")),
            "M2_Target":   _safe_float(row.get("M2_Target")),
            "M2_AchvPct":  _safe_float(row.get("M2_AchvPct")),
            "M1_Actual":   _safe_float(row.get("M1_Actual")),
            "M1_Target":   _safe_float(row.get("M1_Target")),
            "M1_AchvPct":  _safe_float(row.get("M1_AchvPct")),
        }
        for row in rows
    ]
    ranked = sorted(
        [d for d in dealers if d["M1_AchvPct"] is not None],
        key=lambda d: d["M1_AchvPct"],
    )
    return {
        "total_dealers": len(dealers),
        "worst_5_m1":    ranked[:5],
        "best_5_m1":     list(reversed(ranked[-5:])),
        "all_dealers":   dealers,
    }


# ── Per-dealer indexers (for DynamoDB) ────────────────────────────────────────

def _abc_by_dealer(rows: list[dict]) -> dict:
    return {
        row["dealer_code"]: {
            "dealer_name":   row.get("dealer_name", ""),
            "country":       row.get("country", ""),
            "ytd_sales_eur": _safe_float(row.get("ytd_sales_eur")),
            "quantile":      row.get("quantile"),
            "segment":       row.get("segment", ""),
        }
        for row in rows if row.get("dealer_code")
    }


def _mom_by_dealer(rows: list[dict]) -> dict:
    return {
        row["dealer_code"]: {
            "categories_with_decline_pct": row.get("Categories_With_Decline_Pct", "None"),
        }
        for row in rows if row.get("dealer_code")
    }


def _yoy_by_dealer(rows: list[dict]) -> dict:
    result = {}
    for row in rows:
        dc = row.get("dealer_code", "")
        if not dc:
            continue
        result[dc] = {f"{cat}_YOY_PCT": _safe_float(row.get(f"{cat}_YOY_PCT")) for cat in CATEGORIES}
    return result


def _revenue_yoy_by_dealer(rows: list[dict]) -> dict:
    return {
        row["dealer_code"]: {
            "cy_revenue_eur":           _safe_float(row.get("current_year_sales_eur")),
            "ly_revenue_eur":           _safe_float(row.get("last_year_sales_eur")),
            "sales_gap_eur":            _safe_float(row.get("sales_gap_eur")),
            "sales_growth_decline_pct": _safe_float(row.get("sales_growth_decline_pct")),
            "sales_performance_tag":    row.get("sales_performance_tag", ""),
            "dealer_health_tag":        row.get("dealer_health_tag", ""),
            "comparison_month":         row.get("comparison_month"),
        }
        for row in rows if row.get("dealer_code")
    }


def _comp_customer_count_by_dealer(rows: list[dict]) -> dict:
    return {
        row["dealer_code"]: {
            "country":                             row.get("country", ""),
            "current_month_customer_count":        _safe_float(row.get("current_month_customer_count")),
            "last_year_same_month_customer_count": _safe_float(row.get("last_year_same_month_customer_count")),
            "growth_degrowth_pct":                 _safe_float(row.get("growth_degrowth_pct")),
            "customer_growth_health_tag":          row.get("customer_growth_health_tag", ""),
        }
        for row in rows if row.get("dealer_code")
    }


def _customer_trend_by_dealer(rows: list[dict]) -> dict:
    return {
        row["dealer_code"]: {
            "customer_count_last_3_months": row.get("customer_count_last_3_months", ""),
            "customer_count_trend":         row.get("customer_count_trend", ""),
        }
        for row in rows if row.get("dealer_code")
    }


def _high_turnover_low_activity_by_dealer(rows: list[dict]) -> dict:
    return {
        row["dealer_code"]: {
            "country":              row.get("country", ""),
            "turnover_score":       _safe_float(row.get("turnover_score")),
            "purchase_growth_pct":  _safe_float(row.get("purchase_growth_pct")),
            "sales_growth_pct":     _safe_float(row.get("sales_growth_pct")),
            "invoice_growth_pct":   _safe_float(row.get("invoice_growth_pct")),
            "turnover_tag":         row.get("turnover_tag", ""),
            "activity_tag":         row.get("activity_tag", ""),
            "invoice_momentum_tag": row.get("invoice_momentum_tag", ""),
            "dealer_segment":       row.get("dealer_segment", ""),
            "ai_opportunity_tag":   row.get("ai_opportunity_tag", ""),
            "purchase_risk_level":  row.get("purchase_risk_level", ""),
        }
        for row in rows if row.get("dealer_code")
    }


def _vs_target_by_dealer(rows: list[dict]) -> dict:
    return {
        row["dealer_code"]: {
            "M3_Actual":   _safe_float(row.get("M3_Actual")),
            "M3_Target":   _safe_float(row.get("M3_Target")),
            "M3_AchvPct":  _safe_float(row.get("M3_AchvPct")),
            "M2_Actual":   _safe_float(row.get("M2_Actual")),
            "M2_Target":   _safe_float(row.get("M2_Target")),
            "M2_AchvPct":  _safe_float(row.get("M2_AchvPct")),
            "M1_Actual":   _safe_float(row.get("M1_Actual")),
            "M1_Target":   _safe_float(row.get("M1_Target")),
            "M1_AchvPct":  _safe_float(row.get("M1_AchvPct")),
        }
        for row in rows if row.get("dealer_code")
    }


def _write_to_dynamodb(
    abc_by_d: dict, mom_by_d: dict, yoy_by_d: dict,
    rev_yoy_by_d: dict, cust_by_d: dict, htla_by_d: dict,
    sales_rvt_by_d: dict, purchase_rvt_by_d: dict,
    comp_cust_by_d: dict,
    run_date: str,
):
    table       = dynamo.Table(DEALER_TABLE_NAME)
    all_dealers = (
        set(abc_by_d) | set(mom_by_d) | set(yoy_by_d)
        | set(rev_yoy_by_d) | set(cust_by_d) | set(htla_by_d)
        | set(sales_rvt_by_d) | set(purchase_rvt_by_d)
        | set(comp_cust_by_d)
    )
    print(f"[result_aggregator] Writing {len(all_dealers)} dealer items to DynamoDB ({DEALER_TABLE_NAME})")

    with table.batch_writer() as batch:
        for dc in all_dealers:
            abc_kpis    = abc_by_d.get(dc, {})
            dealer_name = abc_kpis.get("dealer_name") or rev_yoy_by_d.get(dc, {}).get("dealer_name", "")
            abc_clean   = {k: v for k, v in abc_kpis.items() if k != "dealer_name"}

            item = {
                "dealer_code": dc,
                "sk":          "LATEST",
                "run_date":    run_date,
                "dealer_name": dealer_name,
                "kpis": {
                    "abc_segmentation":           abc_clean,
                    "mom_decline":                mom_by_d.get(dc, {}),
                    "yoy_comparison":             yoy_by_d.get(dc, {}),
                    "revenue_yoy":                rev_yoy_by_d.get(dc, {}),
                    "customer_trend":             cust_by_d.get(dc, {}),
                    "high_turnover_low_activity": htla_by_d.get(dc, {}),
                    "sale_revenue_vs_target":     sales_rvt_by_d.get(dc, {}),
                    "purchase_revenue_vs_target": purchase_rvt_by_d.get(dc, {}),
                    "comp_customer_count":        comp_cust_by_d.get(dc, {}),
                },
            }
            batch.put_item(Item=_to_dynamo(item))

    print(f"[result_aggregator] DynamoDB write complete — {len(all_dealers)} dealers")


# ── Handler ───────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    print(f"[result_aggregator] Received {len(event)} branch results")

    try:
        results_by_name: dict = {}
        failed_queries: list  = []
        for item in event:
            body = item.get("body", item)
            if isinstance(body, str):
                body = json.loads(body)
            if "query_name" in body and item.get("statusCode", 200) == 200:
                results_by_name[body["query_name"]] = body
            else:
                failed_queries.append(body.get("error", str(body)))

        print(f"[result_aggregator] Succeeded: {list(results_by_name.keys())}")
        if failed_queries:
            print(f"[result_aggregator] Skipped (failed): {failed_queries}")

        if not results_by_name:
            return error_response("result_aggregator failed: all branches failed, nothing to aggregate")

        def _rows(name):
            return _read_csv(results_by_name[name]["output_s3_path"]) if name in results_by_name else []

        abc_rows   = _rows("abc_segmentation")
        mom_rows   = _rows("mom_decline")
        yoy_rows   = _rows("yoy_comparison")
        rev_rows   = _rows("revenue_yoy")
        cst_rows   = _rows("customer_trend")
        htla_rows  = _rows("high_turnover_low_activity")
        svt_rows   = _rows("sale_revenue_vs_target")
        pvt_rows   = _rows("purchase_revenue_vs_target")
        comp_rows  = _rows("comp_customer_count")

        combined = {
            "processed_at":               datetime.now(timezone.utc).isoformat(),
            "partial":                    bool(failed_queries),
            "failed_queries":             failed_queries,
            "abc_segmentation":           _process_abc(abc_rows)                          if abc_rows   else None,
            "mom_decline":                _process_mom(mom_rows)                          if mom_rows   else None,
            "yoy_comparison":             _process_yoy(yoy_rows)                          if yoy_rows   else None,
            "revenue_yoy":                _process_revenue_yoy(rev_rows)                  if rev_rows   else None,
            "customer_trend":             _process_customer_trend(cst_rows)               if cst_rows   else None,
            "high_turnover_low_activity": _process_high_turnover_low_activity(htla_rows)  if htla_rows  else None,
            "sale_revenue_vs_target":     _process_vs_target(svt_rows)                    if svt_rows   else None,
            "purchase_revenue_vs_target": _process_vs_target(pvt_rows)                    if pvt_rows   else None,
            "comp_customer_count":        _process_comp_customer_count(comp_rows)         if comp_rows  else None,
        }
        payload = json.dumps(combined, ensure_ascii=False)

        exec_id = next(iter(results_by_name.values()))["query_execution_id"]

        s3.put_object(
            Bucket=RESULTS_BUCKET,
            Key=f"{PROCESSED_PREFIX}{exec_id}_combined_processed.json",
            Body=payload, ContentType="application/json",
        )
        s3.put_object(
            Bucket=RESULTS_BUCKET,
            Key=f"{PROCESSED_PREFIX}latest_combined.json",
            Body=payload, ContentType="application/json",
        )
        print(f"[result_aggregator] S3 writes complete")

        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        _write_to_dynamodb(
            _abc_by_dealer(abc_rows)                           if abc_rows   else {},
            _mom_by_dealer(mom_rows)                           if mom_rows   else {},
            _yoy_by_dealer(yoy_rows)                           if yoy_rows   else {},
            _revenue_yoy_by_dealer(rev_rows)                   if rev_rows   else {},
            _customer_trend_by_dealer(cst_rows)                if cst_rows   else {},
            _high_turnover_low_activity_by_dealer(htla_rows)   if htla_rows  else {},
            _vs_target_by_dealer(svt_rows)                     if svt_rows   else {},
            _vs_target_by_dealer(pvt_rows)                     if pvt_rows   else {},
            _comp_customer_count_by_dealer(comp_rows)          if comp_rows  else {},
            run_date,
        )

        return success_response({
            "message":          "partial" if failed_queries else "All results aggregated successfully",
            "processed_s3_key": f"{PROCESSED_PREFIX}{exec_id}_combined_processed.json",
            "succeeded":        list(results_by_name.keys()),
            "failed":           failed_queries,
        })

    except Exception as exc:
        return error_response(f"result_aggregator failed: {exc}")
