# DIBMW Lambda Functions Inventory

**Project:** DIBMW Dealer Analytics (BMW Independent Aftermarket)  
**Region:** eu-central-1 (Frankfurt) — all queries scoped to **UK IR** region  
**Runtime:** Python 3.13 / arm64 / 256 MB / 900s default timeout  
**Shared Layer:** `shared/` (provides `shared.config` and `shared.utils`)

---

## Architecture Overview

A **Step Functions pipeline** (`dibmw-analytics-pipeline-{Stage}`) runs 9 Athena query Lambdas in parallel, feeds results into a result aggregator (CSV → JSON + DynamoDB), then dispatches an SNS report. A separate **API Gateway** serves a FastAPI app for the frontend, with visit-management and AI-insights endpoints alongside it.

**Pipeline schedule:** `cron(0 7 ? * MON-FRI *)` (weekdays 07:00 UTC) + manual `POST /trigger`

**DynamoDB tables:**
| Table | PK | SK | Purpose |
|-------|----|----|---------|
| `dibmw-dealer-cache-{Stage}` | `dealer_code` | `sk` | Per-dealer KPI cache |
| `bmw_visits-{Stage}` | `dealer_code` | `visit_id` | Visit form drafts/submissions |
| `bmw_visit_capture-{Stage}` | `dealer_code` | `visit_id` | Shared visit capture form |

---

## Pipeline Athena Query Functions (1–9)

All 9 run in parallel inside Step Functions. Each ignores its input event, executes an Athena query, polls until complete, and returns `{query_name, query_execution_id, output_s3_path}`. Output CSVs land in `s3://dibmw-dev-athena-results-bucket/results/`.

### 1. abc_segmentation

| Field | Detail |
|-------|--------|
| **Purpose** | YTD dealer achievement segmentation — NTILE(3) buckets dealers into A / B / C segments by sales |
| **Trigger** | Step Functions parallel branch |
| **Input / Output** | Event ignored → `{query_name: "abc_segmentation", output_s3_path}` |
| **Tables read** | `dibmw-dev-sellout.sellout_agg_dealer`, `dibmw-dev-sellout.dealer_master` |
| **Writes to** | S3 CSV |
| **Status** | ✅ Complete — production-ready, polling + error handling, no TODOs |

### 2. mom_decline

| Field | Detail |
|-------|--------|
| **Purpose** | Identifies month-over-month parts category sales decline per dealer (27 aftersales categories, 3-month window). Flags categories with 2+ consecutive months of decline and reports average decline % |
| **Trigger** | Step Functions parallel branch |
| **Input / Output** | Event ignored → `{query_name: "mom_decline", output_s3_path}` |
| **Tables read** | `dibmw-dev-sellout.sellout_view` |
| **Writes to** | S3 CSV |
| **Status** | ✅ Complete — most complex SQL (~454 lines), no TODOs |

### 3. yoy_comparison

| Field | Detail |
|-------|--------|
| **Purpose** | Compares current month vs same month last year per dealer across 27 aftersales categories. Computes YoY percentage change per category |
| **Trigger** | Step Functions parallel branch |
| **Input / Output** | Event ignored → `{query_name: "yoy_comparison", output_s3_path}` |
| **Tables read** | `dibmw-dev-sellout.sellout_view` |
| **Writes to** | S3 CSV |
| **Status** | ✅ Complete — large SQL (~460 lines), no TODOs |

### 4. revenue_yoy

| Field | Detail |
|-------|--------|
| **Purpose** | Compares current year vs last year MTD total revenue per dealer. Tags dealers as Growth / Decline / Flat and assigns health tags (Critical / Needs Monitoring / Stable) via NTILE(3) by country |
| **Trigger** | Step Functions parallel branch |
| **Input / Output** | Event ignored → `{query_name: "revenue_yoy", output_s3_path}` |
| **Tables read** | `dibmw-dev-sellout.sellout_agg_dealer`, `dibmw-dev-sellout.dealer_master` |
| **Writes to** | S3 CSV |
| **Status** | ✅ Complete — no TODOs |

### 5. customer_trend

| Field | Detail |
|-------|--------|
| **Purpose** | Tracks monthly customer count trend over last 3 months (M-4 through M-1). Labels each dealer as "Stable" or "Declining: X%" based on whether 2+ of 3 months show decline |
| **Trigger** | Step Functions parallel branch |
| **Input / Output** | Event ignored → `{query_name: "customer_trend", output_s3_path}` |
| **Tables read** | `dibmw-dev-sellout.sellout_view` |
| **Writes to** | S3 CSV |
| **Status** | ✅ Complete — no TODOs |

### 6. comp_customer_count

| Field | Detail |
|-------|--------|
| **Purpose** | Compares current month's distinct customer count vs same month last year per dealer. Assigns health tags via NTILE(3) by country |
| **Trigger** | Step Functions parallel branch |
| **Input / Output** | Event ignored → `{query_name: "comp_customer_count", output_s3_path}` |
| **Tables read** | `dibmw-dev-sellout.sellout_view` |
| **Writes to** | S3 CSV |
| **Status** | ✅ Complete — no TODOs |

### 7. high_turnover_low_activity

| Field | Detail |
|-------|--------|
| **Purpose** | Flags dealers with high sell-out turnover but declining purchase activity. Computes composite turnover_score (80% sales growth + 20% invoice growth) over 6 months. Assigns segments (High Turnover Low Activity, etc.), opportunity tags (Replenishment Opportunity, Growth Opportunity, etc.), and purchase risk levels |
| **Trigger** | Step Functions parallel branch |
| **Input / Output** | Event ignored → `{query_name: "high_turnover_low_activity", output_s3_path}` |
| **Tables read** | `dibmw-dev-sellout.sellout_view` |
| **Writes to** | S3 CSV |
| **Status** | ✅ Complete — most sophisticated business logic SQL (~375 lines), no TODOs |

### 8. sale_revenue_vs_target

| Field | Detail |
|-------|--------|
| **Purpose** | Compares actual sales revenue vs target for the last 3 months (M-1, M-2, M-3) per dealer. Computes achievement percentage per month |
| **Trigger** | Step Functions parallel branch |
| **Input / Output** | Event ignored → `{query_name: "sale_revenue_vs_target", output_s3_path}` |
| **Tables read** | `dibmw-dev-sellout.sellout_agg_dealer`, `dibmw-dev-sellout.dealer_master`, `dibmw-dev-sellout.sellout_view` |
| **Writes to** | S3 CSV |
| **Status** | ✅ Complete — no TODOs |

### 9. purchase_revenue_vs_target

| Field | Detail |
|-------|--------|
| **Purpose** | Compares actual purchase revenue vs target for the last 3 months per dealer. Same structure as sale_revenue_vs_target but uses `purchase_actual_MTD` / `purchase_target_MTD`. Also adds `M1_Achievement_Health_Tag` via NTILE(3) |
| **Trigger** | Step Functions parallel branch |
| **Input / Output** | Event ignored → `{query_name: "purchase_revenue_vs_target", output_s3_path}` |
| **Tables read** | `dibmw-dev-sellout.sellout_agg_dealer`, `dibmw-dev-sellout.dealer_master`, `dibmw-dev-sellout.sellout_view` |
| **Writes to** | S3 CSV |
| **Status** | ✅ Complete — no TODOs |

---

## Standalone Athena Query Functions (10–11)

Deployed but **not wired into the Step Functions pipeline**. The result_aggregator already has processing code ready for them.

### 10. market_basket_uk_ir

| Field | Detail |
|-------|--------|
| **Purpose** | Reads pre-computed CTAS table to identify market basket opportunities — categories where a dealer's cross-sell confidence is below the country benchmark |
| **Trigger** | None — must be invoked manually |
| **Input / Output** | Event ignored → `{query_name: "market_basket_uk_ir", output_s3_path}` |
| **Tables read** | `dibmw-dev-sellout.market_basket_below_benchmark_uk_ir` (CTAS) |
| **Writes to** | S3 CSV |
| **Status** | ⚠️ Code complete, **not connected to pipeline**. result_aggregator has `_process_market_basket` ready |

### 11. parts_category_mix_benchmark

| Field | Detail |
|-------|--------|
| **Purpose** | Reads pre-computed CTAS table containing per-dealer category mix with benchmark comparisons per category |
| **Trigger** | None — must be invoked manually |
| **Input / Output** | Event ignored → `{query_name: "parts_category_mix_benchmark", output_s3_path}` |
| **Tables read** | `dibmw-dev-sellout.parts_category_mix_benchmark_uk_ir` (CTAS) |
| **Writes to** | S3 CSV |
| **Status** | ⚠️ Code complete, **not connected to pipeline**. result_aggregator has `_process_parts_category_benchmark` ready |

---

## Pipeline Orchestration Functions (12–13)

### 12. result_aggregator

| Field | Detail |
|-------|--------|
| **Purpose** | Central aggregation. Receives results from all parallel Athena branches, reads each CSV from S3, processes into structured summaries (best/worst dealers, segment counts, etc.), writes combined JSON to S3 (timestamped + `latest_combined.json`), writes per-dealer items to DynamoDB with all KPIs nested under a `kpis` map |
| **Trigger** | Step Functions — runs after `RunQueriesInParallel` completes |
| **Input** | Array of `{statusCode, body: {query_name, query_execution_id, output_s3_path}}` |
| **Output** | `{statusCode: 200, body: {message, processed_s3_key, succeeded: [...], failed: [...]}}` |
| **Tables read** | S3 CSVs from `dibmw-dev-athena-results-bucket/results/` |
| **Writes to** | S3 JSON (`processed/` prefix) + DynamoDB `dibmw-dealer-cache-{Stage}` (SK=`LATEST`) |
| **Status** | ✅ Complete (~597 lines, largest handler). Handles partial failures gracefully. Has unused processing code for market_basket and parts_category_benchmark |

### 13. report_dispatcher

| Field | Detail |
|-------|--------|
| **Purpose** | Reads combined JSON from S3, builds a formatted text report, publishes to SNS |
| **Trigger** | Step Functions — runs after result_aggregator |
| **Input** | `{processed_s3_key: "processed/...json"}` from result_aggregator |
| **Output** | `{statusCode: 200, body: {message, report_preview}}` |
| **Tables read** | S3 combined JSON |
| **Writes to** | SNS topic (ARN from env var, defaults to empty = optional) |
| **Status** | ⚠️ Working, but **only covers 3 of 9 KPIs** (abc, mom, yoy). Newer KPIs not in the report |

---

## API & Frontend Functions (14–15)

### 14. api_handler

| Field | Detail |
|-------|--------|
| **Purpose** | REST API (FastAPI + Mangum) serving all frontend endpoints for dealer analytics |
| **Trigger** | API Gateway — 13 route events |
| **Timeout** | 30s |
| **Tables read** | DynamoDB `dibmw-dealer-cache-{Stage}`, S3 `processed/latest_combined.json` |
| **Writes to** | Invokes `dibmw-insights-generator-{Stage}` Lambda, starts Step Functions execution |
| **Status** | ✅ Complete — clean FastAPI with Swagger docs, no TODOs |

**API Routes:**

| Method | Path | Description |
|--------|------|-------------|
| GET | `/dealers` | List all dealers with KPIs (DynamoDB scan) |
| GET | `/dealers/{dealer_code}` | Single dealer profile |
| GET | `/dealers/{dealer_code}/insights` | AI insights (cache or generate) |
| GET | `/results` | Full combined JSON from S3 |
| GET | `/results/abc-segmentation` | ABC data slice |
| GET | `/results/mom-decline` | MoM data slice |
| GET | `/results/yoy-comparison` | YoY data slice |
| GET | `/results/revenue-yoy` | Revenue YoY slice |
| GET | `/results/customer-trend` | Customer trend slice |
| GET | `/results/high-turnover-low-activity` | HTLA data slice |
| GET | `/results/sale-revenue-vs-target` | Sale RvT slice |
| GET | `/results/purchase-revenue-vs-target` | Purchase RvT slice |
| POST | `/trigger` | Start Step Functions pipeline |

### 15. insights_generator

| Field | Detail |
|-------|--------|
| **Purpose** | Generates AI-powered pre-visit intelligence briefs. Fetches dealer KPIs from DynamoDB, loads benchmark data from S3 Parquet files, fetches last submitted visit, calls Bedrock (Claude Haiku 4.5) to generate structured JSON with `top_issues`, `summary`, and `pitch` |
| **Trigger** | Synchronous Lambda invoke from api_handler (on insights cache miss) |
| **Timeout** | 90s |
| **Input** | `{dealer_code: "..."}` |
| **Output** | `{statusCode: 200, body: JSON with dealer_code, top_issues, summary, pitch, last_visit_meta}` |
| **Tables read** | DynamoDB `dibmw-dealer-cache-{Stage}` (KPIs + cache), DynamoDB `bmw_visit_capture-{Stage}` (last visit), S3 Parquet benchmark files, shared layer metadata JSONs |
| **Writes to** | DynamoDB `dibmw-dealer-cache-{Stage}` (SK=`INSIGHTS_LATEST`) |
| **External calls** | AWS Bedrock — `eu.anthropic.claude-haiku-4-5-20251001-v1:0` |
| **Status** | ✅ Complete — sophisticated prompt engineering with benchmarks + last-visit context |

---

## Visit Management Functions (16–21)

Simple CRUD functions for two DynamoDB tables. All triggered via API Gateway with 30s timeout.

### 16. save_visit

| Field | Detail |
|-------|--------|
| **Purpose** | Upserts a visit form (draft or submitted) |
| **Trigger** | API Gateway `PUT /visits/{dealer_code}/{visit_id}` |
| **Tables** | Writes → `bmw_visits-{Stage}` |
| **Status** | ✅ Complete — no body validation beyond JSON parsing |

### 17. get_draft_visit

| Field | Detail |
|-------|--------|
| **Purpose** | Returns latest draft visit for a dealer, optionally filtered by `form_type` |
| **Trigger** | API Gateway `GET /visits/{dealer_code}/draft` |
| **Tables** | Reads → `bmw_visits-{Stage}` |
| **Status** | ✅ Complete |

### 18. search_visit

| Field | Detail |
|-------|--------|
| **Purpose** | Searches visits by dealer_code with optional `form_type` and `visit_date` filters |
| **Trigger** | API Gateway `GET /visits/{dealer_code}/search` |
| **Tables** | Reads → `bmw_visits-{Stage}` |
| **Status** | ⚠️ Complete, but **returns only first result** (not a list) |

### 19. save_visit_capture

| Field | Detail |
|-------|--------|
| **Purpose** | Upserts a shared visit capture form. On `status=submitted`, invalidates insights cache by deleting `INSIGHTS_LATEST` for that dealer |
| **Trigger** | API Gateway `PUT /visit-capture/{dealer_code}/{visit_id}` |
| **Tables** | Writes → `bmw_visit_capture-{Stage}`, Deletes from → `dibmw-dealer-cache-{Stage}` (cache invalidation) |
| **Status** | ✅ Complete — smart cache invalidation logic |

### 20. get_draft_visit_capture

| Field | Detail |
|-------|--------|
| **Purpose** | Returns latest draft visit capture for a dealer |
| **Trigger** | API Gateway `GET /visit-capture/{dealer_code}/draft` |
| **Tables** | Reads → `bmw_visit_capture-{Stage}` |
| **Status** | ✅ Complete |

### 21. search_visit_capture

| Field | Detail |
|-------|--------|
| **Purpose** | Searches visit captures by dealer_code with optional filters |
| **Trigger** | API Gateway `GET /visit-capture/{dealer_code}/search` |
| **Tables** | Reads → `bmw_visit_capture-{Stage}` |
| **Status** | ⚠️ Complete, but **returns only first result** (not a list) |

---

## Pending Work & Known Gaps

| # | Item | Affected Functions | Severity |
|---|------|--------------------|----------|
| 1 | **market_basket_uk_ir** and **parts_category_mix_benchmark** are deployed but not added to the Step Functions pipeline parallel branches | template.yml, market_basket_uk_ir, parts_category_mix_benchmark | Medium — aggregator code is ready, just needs pipeline wiring |
| 2 | **report_dispatcher** only reports on 3 of 9 KPIs (abc, mom, yoy) — newer KPIs missing from SNS report | report_dispatcher | Medium — reduces visibility of newer analytics |
| 3 | **search_visit** and **search_visit_capture** return only the first matching item, not a full list | search_visit, search_visit_capture | Low — may limit frontend search functionality |
| 4 | **Legacy `lambda_function.py`** at project root is obsolete (hardcoded weeks, no timeout guard) | lambda_function.py | Low — should be deleted for cleanliness |
| 5 | **save_visit** does no validation of the request body beyond JSON parsing | save_visit | Low — relies on frontend validation |

---

## Quick Reference: Data Flow

```
EventBridge (cron) ──┐
POST /trigger ───────┤
                     ▼
          ┌── Step Functions Pipeline ──┐
          │                             │
          │  ┌─────────────────────┐    │
          │  │  9 Athena Lambdas   │    │
          │  │  (parallel)         │    │
          │  │  → S3 CSVs          │    │
          │  └────────┬────────────┘    │
          │           ▼                 │
          │  ┌─────────────────────┐    │
          │  │  result_aggregator  │    │
          │  │  CSV → JSON + Dynamo│    │
          │  └────────┬────────────┘    │
          │           ▼                 │
          │  ┌─────────────────────┐    │
          │  │  report_dispatcher  │    │
          │  │  JSON → SNS         │    │
          │  └─────────────────────┘    │
          └─────────────────────────────┘

API Gateway ──► api_handler (FastAPI)
                  ├── GET /dealers → DynamoDB
                  ├── GET /results → S3
                  ├── GET /dealers/{id}/insights → insights_generator → Bedrock
                  └── POST /trigger → Step Functions

API Gateway ──► visit CRUD (6 functions) → DynamoDB
```
