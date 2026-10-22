import json
import os

import boto3

VISIT_CAPTURE_TABLE_NAME = os.environ.get("VISIT_CAPTURE_TABLE_NAME", "bmw_visit_capture-dev")
DEALER_TABLE_NAME        = os.environ.get("DEALER_TABLE_NAME", "dibmw-dealer-cache-dev")

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,PUT,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Content-Type": "application/json",
}

dynamo = boto3.resource("dynamodb", region_name="eu-central-1")


def lambda_handler(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    try:
        dealer_code = event["pathParameters"]["dealer_code"]
        visit_id = event["pathParameters"]["visit_id"]

        raw_body = event.get("body") or "{}"
        body = json.loads(raw_body) if isinstance(raw_body, str) else raw_body

        item = {
            "dealer_code": dealer_code,
            "visit_id": visit_id,
            **body,
        }
        # recommendation_id (optional) links this visit to an AI-generated recommendation/pitch
        recommendation_id = body.get("recommendation_id")
        if recommendation_id is not None:
            item["recommendation_id"] = recommendation_id

        table = dynamo.Table(VISIT_CAPTURE_TABLE_NAME)
        table.put_item(Item=item)

        if body.get("status") == "submitted":
            try:
                dynamo.Table(DEALER_TABLE_NAME).delete_item(
                    Key={"dealer_code": dealer_code, "sk": "INSIGHTS_LATEST"}
                )
            except Exception as e:
                print(f"[save_visit_capture] Failed to invalidate insights cache for {dealer_code}: {e}")

        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"saved": True}),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)}),
        }
