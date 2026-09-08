import json
import os

import boto3

VISITS_TABLE_NAME = os.environ.get("VISITS_TABLE_NAME", "bmw_visits-dev")

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

        table = dynamo.Table(VISITS_TABLE_NAME)
        table.put_item(
            Item={
                "dealer_code": dealer_code,
                "visit_id": visit_id,
                **body,
            }
        )

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
