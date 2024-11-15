import json
import boto3
import os

client = boto3.client('stepfunctions')
STEP_FUNCTION_ARN = os.getenv('STEP_FUNCTION_ARN')

def lambda_handler(event, context):
    # TODO implement
    bucketName = event["Records"][0]["s3"]["bucket"]["name"]
    bucketKey = event["Records"][0]["s3"]["object"]["key"]
    
    print(bucketName)
    print(bucketKey)
    
    WorkflowInput = json.dumps({"bucketName" : bucketName, "bucketKey" : bucketKey})
    
    response = client.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        input=WorkflowInput
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully invoked Step Function!')
    }
