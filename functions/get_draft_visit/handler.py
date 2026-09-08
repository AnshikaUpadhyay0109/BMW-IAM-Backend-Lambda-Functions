import json
import os
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr, Key

VISITS_TABLE_NAME = os.environ.get("VISITS_TABLE_NAME", "bmw_visits-dev")

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,PUT,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Content-Type": "application/json",
}

dynamo = boto3.resource("dynamodb", region_name="eu-central-1")


def _sanitize(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(i) for i in obj]
    return obj


def lambda_handler(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    try:
        dealer_code = event["pathParameters"]["dealer_code"]
        form_type = (event.get("queryStringParameters") or {}).get("form_type")

        filter_expr = Attr("status").eq("draft")
        if form_type:
            filter_expr = filter_expr & Attr("form_type").eq(form_type)

        table = dynamo.Table(VISITS_TABLE_NAME)

        response = table.query(
            KeyConditionExpression=Key("dealer_code").eq(dealer_code),
            FilterExpression=filter_expr,
        )
        items = response.get("Items", [])
        draft = max(items, key=lambda x: x["updated_at"]) if items else None

        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"draft": _sanitize(draft)}),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)}),
        }
