# shared/config.py
# Central configuration used by ALL Lambda functions
# Values are read from environment variables (set in template.yaml per stage)

import os

# ── Athena ────────────────────────────────────────────────────────────────────
ATHENA_DATABASE     = os.environ.get("ATHENA_DATABASE",     "dibmw-dev-sellout")
ATHENA_REGION       = os.environ.get("ATHENA_REGION",       "eu-central-1")
S3_OUTPUT_LOCATION  = os.environ.get("S3_OUTPUT_LOCATION",  "s3://dibmw-dev-athena-results-bucket/results/")

# ── S3 ────────────────────────────────────────────────────────────────────────
RESULTS_BUCKET      = os.environ.get("RESULTS_BUCKET",      "dibmw-dev-athena-results-bucket")
PROCESSED_PREFIX    = os.environ.get("PROCESSED_PREFIX",    "processed/")

# ── SNS ───────────────────────────────────────────────────────────────────────
SNS_TOPIC_ARN       = os.environ.get("SNS_TOPIC_ARN",       "")

# ── DynamoDB ──────────────────────────────────────────────────────────────────
DEALER_TABLE_NAME   = os.environ.get("DEALER_TABLE_NAME",   "dibmw-dealer-cache-dev")

# ── Step Functions / Orchestration ────────────────────────────────────────────
# Each Lambda returns a status so Step Functions (or the next Lambda) can chain
POLL_INTERVAL_SEC   = int(os.environ.get("POLL_INTERVAL_SEC", "5"))
MAX_POLL_ATTEMPTS   = int(os.environ.get("MAX_POLL_ATTEMPTS", "110"))  # ~9 min max