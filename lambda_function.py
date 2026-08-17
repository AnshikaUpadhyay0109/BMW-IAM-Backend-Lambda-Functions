import boto3

import time

import json



# Initialize Athena client for eu-central-1 (Frankfurt)

athena = boto3.client('athena', region_name='eu-central-1')



# Configuration

ATHENA_DATABASE = 'dibmw-dev-sellout'  # Your Athena database schema name



# Replace with your EXACT default S3 output location from Athena Settings tab

# Example: 's3://aws-athena-query-results-1234567890-eu-central-1/'

S3_OUTPUT_LOCATION = 's3://dibmw-dev-athena-results-bucket/results/'



SQL_QUERY = """

WITH dealer_sales AS (

    SELECT dealer_code

         , ROUND(SUM(net_amount), 2) AS total_sales

    FROM "dibmw-dev-sellout"."sellout"

    GROUP BY 1

    ORDER BY 1 DESC

),

dealer_wow AS (

    SELECT dealer_code

         , ROUND(SUM(CASE WHEN week(invoice_date)=19 THEN net_amount END), 2) AS week_19_sales

         , ROUND(SUM(CASE WHEN week(invoice_date)=20 THEN net_amount END), 2) AS week_20_sales

         , ROUND(SUM(CASE WHEN week(invoice_date)=21 THEN net_amount END), 2) AS week_21_sales

         , ROUND(SUM(CASE WHEN week(invoice_date)=22 THEN net_amount END), 2) AS week_22_sales

         , ROUND(

            (

                SUM(CASE WHEN week(invoice_date)=22 THEN net_amount END)

                - SUM(CASE WHEN week(invoice_date)=21 THEN net_amount END)

            ) * 100.0

            / NULLIF(

                SUM(CASE WHEN week(invoice_date)=21 THEN net_amount END),

                0

            ),

            2

         ) AS wow_21_22_growth_pct

    FROM "dibmw-dev-sellout"."sellout"

    GROUP BY 1

)

SELECT *

     , CASE WHEN total_sales > 260000.0 THEN 'A'

            WHEN total_sales > 60000.0 AND total_sales <= 260000.0 THEN 'B'

            ELSE 'C' END AS dealer_ABC_seg

FROM (

    SELECT w.*

         , s.total_sales

    FROM dealer_wow w

    LEFT JOIN dealer_sales s ON w.dealer_code = s.dealer_code

    ORDER BY total_sales DESC

);

"""



def lambda_handler(event, context):

    try:

        # 1. Start Athena Query Execution

        response = athena.start_query_execution(

            QueryString=SQL_QUERY,

            QueryExecutionContext={'Database': ATHENA_DATABASE},

            ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION}

        )

       

        query_execution_id = response['QueryExecutionId']

        print(f"Query started with ID: {query_execution_id}")

       

        # 2. Poll for Query Completion (checks every 2 seconds)

        while True:

            query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)

            state = query_status['QueryExecution']['Status']['State']

           

            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:

                break

            time.sleep(2)

           

        if state != 'SUCCEEDED':

            reason = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')

            raise Exception(f"Athena Query Failed! Status: {state}. Reason: {reason}")

           

        output_file_s3_path = f"{S3_OUTPUT_LOCATION}{query_execution_id}.csv"

       

        return {

            'statusCode': 200,

            'body': json.dumps({

                'message': 'Query executed successfully',

                'query_execution_id': query_execution_id,

                'output_s3_file': output_file_s3_path

            })

        }



    except Exception as e:

        print(f"Error executing Athena query: {str(e)}")

        return {

            'statusCode': 500,

            'body': json.dumps({'error': str(e)})

        }