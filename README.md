# DIBMW Dealer Analytics — Multi-Lambda Pipeline

## Architecture Overview

```
EventBridge (daily 07:00 UTC)
        │
        ▼
┌───────────────────┐     S3 CSV      ┌──────────────────────┐     S3 JSON     ┌───────────────────────┐
│  query_executor   │ ──────────────► │  result_processor    │ ──────────────► │  report_dispatcher    │
│                   │                 │                      │                 │                       │
│ • Runs Athena SQL │                 │ • Reads CSV from S3  │                 │ • Reads JSON from S3  │
│ • Polls for done  │                 │ • Casts + enriches   │                 │ • Builds text report  │
│ • Returns exec ID │                 │ • Segment counts     │                 │ • Publishes to SNS    │
└───────────────────┘                 │ • Top-5 rankings     │                 └───────────────────────┘
                                      │ • Writes JSON to S3  │
                                      └──────────────────────┘

All 3 functions are chained by AWS Step Functions (retry + error handling built in).
Shared code (config.py, utils.py) lives in a Lambda Layer — no duplication.
```

## Project Structure

```
dibmw-analytics/
├── functions/
│   ├── query_executor/
│   │   └── handler.py          # Lambda 1 — runs Athena SQL
│   ├── result_processor/
│   │   └── handler.py          # Lambda 2 — parses CSV, writes JSON
│   └── report_dispatcher/
│       └── handler.py          # Lambda 3 — builds & sends report
├── shared/
│   ├── config.py               # All env-var config in one place
│   └── utils.py                # Reusable helpers (responses, logging)
├── infra/
│   └── template.yaml           # SAM: all functions + layer + Step Functions
└── README.md
```

## Data Flow (per run)

| Step | What happens | Output |
|------|-------------|--------|
| 1 | EventBridge triggers Step Functions | — |
| 2 | `query_executor` submits SQL, polls Athena | `s3://…/results/<id>.csv` |
| 3 | `result_processor` reads CSV, enriches data | `s3://…/processed/<id>_processed.json` |
| 4 | `report_dispatcher` reads JSON, publishes report | SNS → email / Slack |

## Deployment

### Prerequisites
- AWS CLI configured (`aws configure`)
- SAM CLI installed (`brew install aws-sam-cli`)
- S3 results bucket already created

### Deploy to dev
```bash
cd infra
sam build
sam deploy --guided   # first time — creates samconfig.toml
```

### Deploy to prod
```bash
sam deploy --config-env prod \
  --parameter-overrides Stage=prod SnsTopicArn=arn:aws:sns:eu-central-1:123456789:dibmw-prod
```

### Test a single function locally
```bash
# Test query_executor
sam local invoke QueryExecutorFunction --event events/empty.json

# Test result_processor with a mock event
sam local invoke ResultProcessorFunction --event events/result_processor_event.json
```

## Environment Variables

All config flows through `shared/config.py` — **never hardcode** values in handlers.

| Variable | Description | Default |
|----------|-------------|---------|
| `ATHENA_DATABASE` | Athena schema name | `dibmw-dev-sellout` |
| `ATHENA_REGION` | AWS region | `eu-central-1` |
| `S3_OUTPUT_LOCATION` | Athena result bucket path | `s3://…/results/` |
| `RESULTS_BUCKET` | Bucket name (no `s3://`) | `dibmw-dev-athena-results-bucket` |
| `SNS_TOPIC_ARN` | SNS topic for report emails | `""` |
| `PROCESSED_PREFIX` | S3 prefix for enriched JSON | `processed/` |

## Key Design Decisions

- **One responsibility per Lambda** — easy to test, debug, and redeploy independently.
- **Lambda Layer for shared code** — `config.py` and `utils.py` are deployed once and reused.
- **Step Functions for orchestration** — automatic retry (×2), error catching, and visual debugging in the AWS console.
- **Pass state via S3, not Lambda payload** — large result sets go to S3; only metadata (keys, IDs) flows between functions.
- **Least-privilege IAM** — each function has only the policies it needs (Athena, S3 read, S3 write, SNS publish).