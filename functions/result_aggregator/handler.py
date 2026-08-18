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

s3       = boto3.client("s3")
dynamo   = boto3.resource("dynamodb", region_name="eu-central-1")

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


# ── S3 summary processors (aggregate view — for report + S3 archive) ──────────

def _process_abc(rows: list[dict]) -> dict:
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
            "QTD_Actual":  _safe_float(row.get("QTD_Actual")),
            "QTD_Target":  _safe_float(row.get("QTD_Target")),
            "QTD_AchvPct": _safe_float(row.get("QTD_AchvPct")),
        }
        for row in rows
    ]
    ranked = sorted(
        [d for d in dealers if d["QTD_AchvPct"] is not None],
        key=lambda d: d["QTD_AchvPct"],
    )
    return {
        "total_dealers": len(dealers),
        "worst_5_qtd":   ranked[:5],
        "best_5_qtd":    list(reversed(ranked[-5:])),
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


# ── Per-dealer indexers (for DynamoDB — one item per dealer) ──────────────────

def _abc_by_dealer(rows: list[dict]) -> dict:
    """Returns {dealer_code: {dealer_name, M1/M2/M3/QTD KPIs}}."""
    return {
        row["dealer_code"]: {
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
            "QTD_Actual":  _safe_float(row.get("QTD_Actual")),
            "QTD_Target":  _safe_float(row.get("QTD_Target")),
            "QTD_AchvPct": _safe_float(row.get("QTD_AchvPct")),
        }
        for row in rows if row.get("dealer_code")
    }


def _mom_by_dealer(rows: list[dict]) -> dict:
    """Returns {dealer_code: {categories_with_decline_pct}}."""
    return {
        row["dealer_code"]: {
            "categories_with_decline_pct": row.get("Categories_With_Decline_Pct", "None"),
        }
        for row in rows if row.get("dealer_code")
    }


def _yoy_by_dealer(rows: list[dict]) -> dict:
    """Returns {dealer_code: {ComfortSafetyAccessories_YOY_PCT: float, ...}}."""
    result = {}
    for row in rows:
        dc = row.get("dealer_code", "")
        if not dc:
            continue
        result[dc] = {
            f"{cat}_YOY_PCT": _safe_float(row.get(f"{cat}_YOY_PCT"))
            for cat in CATEGORIES
        }
    return result


def _write_to_dynamodb(abc_by_d: dict, mom_by_d: dict, yoy_by_d: dict, run_date: str):
    """Batch-write one DynamoDB item per dealer (SK=LATEST, overwrites previous run)."""
    table        = dynamo.Table(DEALER_TABLE_NAME)
    all_dealers  = set(abc_by_d) | set(mom_by_d) | set(yoy_by_d)
    print(f"[result_aggregator] Writing {len(all_dealers)} dealer items to DynamoDB ({DEALER_TABLE_NAME})")

    with table.batch_writer() as batch:
        for dc in all_dealers:
            abc_kpis    = abc_by_d.get(dc, {})
            dealer_name = abc_kpis.get("dealer_name", "")
            abc_kpis_clean = {k: v for k, v in abc_kpis.items() if k != "dealer_name"}

            item = {
                "dealer_code": dc,
                "sk":          "LATEST",
                "run_date":    run_date,
                "dealer_name": dealer_name,
                "kpis": {
                    "abc_segmentation": abc_kpis_clean,
                    "mom_decline":      mom_by_d.get(dc, {}),
                    "yoy_comparison":   yoy_by_d.get(dc, {}),
                },
            }
            batch.put_item(Item=_to_dynamo(item))

    print(f"[result_aggregator] DynamoDB write complete — {len(all_dealers)} dealers")


# ── Handler ───────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    print(f"[result_aggregator] Received {len(event)} branch results")

    try:
        # Collect only successful branches — skip any that returned an error payload
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

        # Read CSVs only for branches that succeeded
        abc_rows = _read_csv(results_by_name["abc_segmentation"]["output_s3_path"]) \
                   if "abc_segmentation" in results_by_name else []
        mom_rows = _read_csv(results_by_name["mom_decline"]["output_s3_path"]) \
                   if "mom_decline" in results_by_name else []
        yoy_rows = _read_csv(results_by_name["yoy_comparison"]["output_s3_path"]) \
                   if "yoy_comparison" in results_by_name else []

        abc_data = _process_abc(abc_rows) if abc_rows else None
        mom_data = _process_mom(mom_rows) if mom_rows else None
        yoy_data = _process_yoy(yoy_rows) if yoy_rows else None

        combined = {
            "processed_at":     datetime.now(timezone.utc).isoformat(),
            "partial":          bool(failed_queries),
            "failed_queries":   failed_queries,
            "abc_segmentation": abc_data,
            "mom_decline":      mom_data,
            "yoy_comparison":   yoy_data,
        }
        payload = json.dumps(combined, ensure_ascii=False)

        # Use the first successful query's exec_id for the archive key
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

        # DynamoDB: only write dealers from whichever queries succeeded
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        _write_to_dynamodb(
            _abc_by_dealer(abc_rows) if abc_rows else {},
            _mom_by_dealer(mom_rows) if mom_rows else {},
            _yoy_by_dealer(yoy_rows) if yoy_rows else {},
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
