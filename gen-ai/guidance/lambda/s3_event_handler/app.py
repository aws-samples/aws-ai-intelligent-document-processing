from typing import Dict, Any
import urllib.parse
import os
import boto3
from boto3.dynamodb.types import TypeSerializer
from datetime import datetime
import logging
import traceback


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
textract = boto3.client('textract')
dynamodb = boto3.client('dynamodb')

TEXTRACT_NOTIFICATION_TOPIC_ARN = os.environ['TEXTRACT_NOTIFICATION_TOPIC_ARN']
TEXTRACT_NOTIFICATION_ROLE_ARN = os.environ['TEXTRACT_NOTIFICATION_ROLE_ARN']
IDP_TEXTRACT_JOBS_TABLE_NAME = os.environ['IDP_TEXTRACT_JOBS_TABLE_NAME']

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
	"""
	AWS Lambda handler function to process S3 events and initiate Textract analysis.

	Args:
		event (Dict[str, Any]): The event data containing S3 object information.
		context (Any): The Lambda context object.

	Returns:
		Dict[str, Any]: The result of saving the job information to DynamoDB.
	"""
	logger.info(f"Processing event: {event}")
	try:
		bucket_name =  event['detail']['bucket']['name']
		object_key = urllib.parse.unquote_plus(event['detail']['object']['key'], encoding='utf-8')

		case_number = object_key.split('/')[0]  # expecting case_id/files path structure

		textract_response = start_textract_analysis(bucket_name, object_key)
		dynamo_result = save_job_to_dynamodb(textract_response['JobId'], case_number, object_key, bucket_name)

		return dynamo_result
	except Exception as e:
		logger.info(f"Error processing S3 event: {e}")
		logger.info(traceback.format_exc())
		raise

def python_to_dynamo(python_object: Dict[str, Any]) -> Dict[str, Any]:
	"""
	Convert a Python dictionary to a DynamoDB-compatible format.

	Args:
		python_object (Dict[str, Any]): The Python dictionary to convert.

	Returns:
		Dict[str, Any]: The DynamoDB-compatible dictionary.
	"""
	serializer = TypeSerializer()
	return {k: serializer.serialize(v) for k, v in python_object.items()}

def start_textract_analysis(bucket_name: str, object_key: str) -> Dict[str, Any]:
	"""
	Start a Textract document analysis job for a given S3 object.

	Args:
		bucket_name (str): The name of the S3 bucket containing the document.
		object_key (str): The key of the S3 object to analyze.

	Returns:
		Dict[str, Any]: The response from the Textract start_document_analysis API call.
	"""
	return textract.start_document_analysis(
		DocumentLocation={'S3Object': {'Bucket': bucket_name, 'Name': object_key}},
		FeatureTypes=['LAYOUT'],
		NotificationChannel={
			'SNSTopicArn': TEXTRACT_NOTIFICATION_TOPIC_ARN,
			'RoleArn': TEXTRACT_NOTIFICATION_ROLE_ARN
		}
	)

def save_job_to_dynamodb(job_id: str, case_number: str, object_key: str, bucket_name: str) -> Dict[str, Any]:
	"""
	Save Textract job information to DynamoDB.

	Args:
		job_id (str): The Textract job ID.
		case_number (str): The case number associated with the document.
		object_key (str): The S3 object key of the document.
		bucket_name (str): The name of the S3 bucket containing the document.

	Returns:
		Dict[str, Any]: The response from the DynamoDB put_item operation.
	"""
	item = {
		'job_id': job_id,
		'case_number': case_number,
		'object_key': object_key,
		'bucket_name': bucket_name,
		'processed_date': datetime.now().isoformat()
	}
	dynamo_item = python_to_dynamo(item)
	return dynamodb.put_item(TableName=IDP_TEXTRACT_JOBS_TABLE_NAME, Item=dynamo_item)
