import boto3
import json
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))
from shared.config import RESULTS_BUCKET, SNS_TOPIC_ARN
from shared.utils import success_response, error_response, log_event, parse_body

s3  = boto3.client("s3")
sns = boto3.client("sns", region_name="eu-central-1")


def _fmt(v, decimals: int = 2) -> str:
    return f"{v:,.{decimals}f}" if v is not None else "N/A"


def _build_report(data: dict) -> str:
    abc = data.get("abc_segmentation", {})
    mom = data.get("mom_decline", {})
    yoy = data.get("yoy_comparison", {})
    ts  = data.get("processed_at", "N/A")

    lines = [
        "=" * 70,
        "  DIBMW DEALER ANALYTICS REPORT",
        f"  Generated : {ts}",
        "=" * 70,
        "",
        "── SECTION 1: QTD DEALER ACHIEVEMENT (ABC Segmentation) ─────────────",
        f"  Total dealers analysed : {abc.get('total_dealers', 0)}",
        "",
        "  WORST 5 — lowest QTD achievement %:",
    ]
    for rank, d in enumerate(abc.get("worst_5_qtd", []), 1):
        lines.append(
            f"  {rank}. {d['dealer_code']:<12} {d['dealer_name']:<30}"
            f"  QTD: {_fmt(d.get('QTD_AchvPct'), 1)}%"
            f"  (€{_fmt(d.get('QTD_Actual'))} / €{_fmt(d.get('QTD_Target'))} target)"
        )

    lines += ["", "  BEST 5 — highest QTD achievement %:"]
    for rank, d in enumerate(abc.get("best_5_qtd", []), 1):
        lines.append(
            f"  {rank}. {d['dealer_code']:<12} {d['dealer_name']:<30}"
            f"  QTD: {_fmt(d.get('QTD_AchvPct'), 1)}%"
            f"  (€{_fmt(d.get('QTD_Actual'))} / €{_fmt(d.get('QTD_Target'))} target)"
        )

    lines += [
        "",
        "── SECTION 2: MoM PARTS CATEGORY SALES DECLINE ─────────────────────",
        f"  Total dealers analysed           : {mom.get('total_dealers', 0)}",
        f"  Dealers with sustained decline   : {mom.get('dealers_with_declines', 0)}",
        f"  Dealers with no sustained decline: {mom.get('dealers_no_decline', 0)}",
        "",
        "  TOP 5 — most declining categories:",
    ]
    for rank, d in enumerate(mom.get("top_5_declining_dealers", []), 1):
        decline_str = d.get("categories_with_decline_pct", "")
        preview = (decline_str[:80] + "…") if len(decline_str) > 80 else decline_str
        lines.append(f"  {rank}. {d['dealer_code']:<12}  → {preview}")

    lines += [
        "",
        "── SECTION 3: YoY PARTS CATEGORY COMPARISON ─────────────────────────",
        f"  Total dealers analysed : {yoy.get('total_dealers', 0)}",
        "",
        "  TOP 5 GROWING CATEGORIES (avg YoY % across all dealers):",
    ]
    for rank, d in enumerate(yoy.get("best_5_categories_yoy", []), 1):
        lines.append(f"  {rank}. {d['category']:<45}  {d['avg_yoy_pct']:+.2f}%")

    lines += ["", "  TOP 5 DECLINING CATEGORIES (avg YoY %):"]
    for rank, d in enumerate(yoy.get("worst_5_categories_yoy", []), 1):
        lines.append(f"  {rank}. {d['category']:<45}  {d['avg_yoy_pct']:+.2f}%")

    lines += ["", "=" * 70, "  End of Report", "=" * 70]
    return "\n".join(lines)


def lambda_handler(event, context):
    log_event("report_dispatcher", event)

    try:
        body             = parse_body(event)
        processed_s3_key = body.get("processed_s3_key")

        if not processed_s3_key:
            return error_response("report_dispatcher: missing 'processed_s3_key' in event", 400)

        obj  = s3.get_object(Bucket=RESULTS_BUCKET, Key=processed_s3_key)
        data = json.loads(obj["Body"].read().decode("utf-8"))

        report_text = _build_report(data)
        print("[report_dispatcher] Report built:\n" + report_text)

        if SNS_TOPIC_ARN:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"DIBMW Dealer Analytics Report — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
                Message=report_text,
            )
            print(f"[report_dispatcher] Published to SNS: {SNS_TOPIC_ARN}")
        else:
            print("[report_dispatcher] SNS_TOPIC_ARN not set — skipping publish (report logged above)")

        return success_response({
            "message":        "Report dispatched successfully",
            "report_preview": report_text[:800] + "…",
        })

    except Exception as exc:
        return error_response(f"report_dispatcher failed: {exc}")
