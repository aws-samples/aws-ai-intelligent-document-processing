import json
import os
import re
from datetime import datetime
from textractor.entities.lazy_document import LazyDocument
from textractor.data.constants import TextractAPI
import boto3
import json
from io import BytesIO
from botocore.exceptions import ClientError
from urllib.parse import urlparse
import logging
from typing import List, Dict, Any, Tuple, Optional
import traceback

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
textract = boto3.client('textract')
bedrock_agent_runtime = boto3.client('bedrock-agent-runtime')
dynamodb = boto3.client('dynamodb')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

FLOW_INPUT_NODE = "FlowInputNode"
FLOW_INPUT_NODE_OUTPUT = "document"

FLOW_IDENTIFIER = os.environ['FLOW_IDENTIFIER']
FLOW_ALIAS_IDENTIFIER = os.environ['FLOW_ALIAS_IDENTIFIER']
OUTPUT_BUCKET_NAME = os.environ['OUTPUT_BUCKET_NAME']
IDP_TEXTRACT_JOBS_TABLE_NAME = os.environ['IDP_TEXTRACT_JOBS_TABLE_NAME']
IN_QUEUE_URL = os.environ['IN_QUEUE_URL']
OUT_QUEUE_URL = os.environ['OUT_QUEUE_URL']
IDP_FLOW_CLASS_TABLE_NAME = os.environ['IDP_FLOW_CLASS_TABLE_NAME']

def lambda_handler(sqs_event: dict, context: Any) -> dict:
	"""
	AWS Lambda handler function to process SQS events containing Textract job results.

	Args:
		sqs_event (dict): The SQS event containing Textract job information.
		context (Any): The Lambda context object.

	Returns:
		dict: A response containing processed document information.
	"""
	logger.info(f"Processing event: {json.dumps(sqs_event)}")
	
	# 1. Validate and get inputs
	validate_sqs_event(sqs_event)
	event = extract_sns_message(sqs_event) # from sqs event
	validate_textract_job(event) 
	job_id, doc_bucket, doc_key = extract_job_details(event) # from event payload
	
	# 2. Get the document content as plain text and source file informaiton
	lazy_doc = load_textract_job(job_id)
	text_content = generate_text_content(lazy_doc)
	job_details = get_job_details(job_id) # from dynamodb
	
	# 3. Generate S3 file keys
	output_path, raw_document_text_file, manifest_document_file = generate_output_paths(job_details, job_id)

	# 4. Save the raw text for further processing later
	save_to_s3(text_content, OUTPUT_BUCKET_NAME, raw_document_text_file)
	
	# 5. Get the supported classes and invoke the prompt flow
	supported_class_list = get_supported_class_list_from_dynamodb()
	classification_result = invoke_classification_flow(text_content, supported_class_list)

	# 6. Save and parse result
	save_to_s3(classification_result, OUTPUT_BUCKET_NAME, manifest_document_file)
	json_response = get_text_in_tag(classification_result, 'json') # expect json to be inside a <json></json> tag in the result string
	doc_manifest = json.loads(json_response)

	# 7. save individual text files and format a message for the next step
	response_doc_list = save_document_parts(doc_manifest, lazy_doc, output_path, supported_class_list)
	final_response = {
		"case_id": job_details["lender_case_id"],
		"documents": response_doc_list
	}
	send_to_sqs(json.dumps(final_response))

	# 9. finally delete the processed queue item
	delete_sqs_message(sqs_event)
	return final_response



def save_to_s3(content: str, bucket_name: str, file_key: str) -> Optional[str]:
	"""
	Save a string content to an Amazon S3 bucket with a specified file key.

	Args:
		content (str): The content to be saved.
		bucket_name (str): The name of the S3 bucket.
		file_key (str): The file key (path) for the object in S3.

	Returns:
		Optional[str]: The file key if the operation is successful, None otherwise.
	"""
	try:
		s3.put_object(
			Bucket=bucket_name,
			Key=file_key,
			Body=content.encode('utf-8')
		)
		logger.info(f"Successfully saved content to S3 bucket '{bucket_name}' with key '{file_key}'")
		return file_key
	except Exception as e:
		logger.error(f"Error saving content to S3: {e}")
		return "None"

def validate_sqs_event(sqs_event: dict) -> None:
	"""Validate the SQS event structure."""
	if 'Records' not in sqs_event:
		raise Exception('No Records section')
	if len(sqs_event['Records']) != 1:
		raise Exception('Expected only 1 record')

def extract_sns_message(sqs_event: dict) -> dict:
	"""Extract and parse the SNS message from the SQS event."""
	sns_body = sqs_event['Records'][0]['body']
	sns_json = json.loads(sns_body)
	return json.loads(sns_json['Message'])

def validate_textract_job(event: dict) -> None:
	"""Validate the Textract job status."""
	if 'SUCCEEDED' not in event['Status']:
		raise Exception("Textract Processing Failed")

def extract_job_details(event: dict) -> Tuple[str, str, str]:
	"""Extract job details from the event."""
	job_id = event['JobId']
	doc_bucket = event['DocumentLocation']['S3Bucket']
	doc_key = event['DocumentLocation']['S3ObjectName']
	return job_id, doc_bucket, doc_key

def load_textract_job(job_id: str) -> LazyDocument:
	"""Load the completed Textract OCR job."""
	return LazyDocument(job_id=job_id, api=TextractAPI.ANALYZE)

def generate_text_content(lazy_doc: LazyDocument) -> str:
	"""Generate text content from the Textract job results and wrap each page in xml tags"""
	text_content = "<document-pages>\n"
	for page in lazy_doc.pages:
		text_content += f"<page>\n<page-index>{page.page_num - 1}</page-index>\n<page-content>\n{page.get_text()}</page-content>\n</page>\n\n"
	text_content += "</document-pages>"
	return text_content

def get_job_details(job_id: str) -> dict:
	"""Retrieve job details from DynamoDB."""
	job_details = dynamodb.get_item(TableName=IDP_TEXTRACT_JOBS_TABLE_NAME, Key={'job_id': {'S': job_id}})
	return {
		'lender_case_id': job_details['Item']['case_number']['S'],
		'source_pdf_bucket': job_details['Item']['bucket_name']['S'],
		'source_pdf_key': job_details['Item']['object_key']['S']
	}

def generate_output_paths(job_details: dict, job_id: str) -> Tuple[str, str, str]:
	"""Generate output file paths for resulting artifacts."""
	output_path = f"{job_details['lender_case_id']}/{job_id}"
	raw_document_text_file = f"{output_path}/input_doc.txt"
	manifest_document_file = f"{output_path}/classify_response.txt"
	return output_path, raw_document_text_file, manifest_document_file

def parse_classification_response(response: str) -> List[dict]:
	"""Parse the classification response JSON."""
	json_response = get_text_in_tag(response, 'json')
	return json.loads(json_response)

def delete_sqs_message(sqs_event: dict) -> None:
	"""Delete the processed message from the SQS queue."""
	receipt_handle = sqs_event['Records'][0]['receiptHandle']
	sqs.delete_message(QueueUrl=IN_QUEUE_URL, ReceiptHandle=receipt_handle)

def send_to_sqs(message: str) -> None:
	"""Send a message to the output SQS queue."""
	response = sqs.send_message(
		QueueUrl=OUT_QUEUE_URL,
		DelaySeconds=10,
		MessageBody=(message)
	)
	logger.info(response['MessageId'])


def invoke_classification_flow(text_content: str, supported_class_list: List[dict]) -> Any:
	classes_str = ""
	for item in supported_class_list:
		classes_str += f"<class_name>{item["class_name"]}<class_name> <expected_inputs>{item["expected_inputs"]}</expected_inputs>\n"
	
	classify_inputs = {
		"doc_text": text_content,
		"class_list": classes_str
	}

	try:
		flow_response = bedrock_agent_runtime.invoke_flow(
			flowIdentifier=FLOW_IDENTIFIER,
			flowAliasIdentifier=FLOW_ALIAS_IDENTIFIER,
			inputs=[{
				"content": {"document": classify_inputs},
				"nodeName": "FlowInputNode",
				"nodeOutputName": "document"
			}]
		)
		return process_flow_response(flow_response)
	except ClientError as e:
		logger.error(f"Error invoking classification flow: {e}")
		return {"error": str(e)}

def process_flow_response(flow_response: Dict[str, Any]) -> Any:
	result = {}
	for event in flow_response.get("responseStream"):
		result.update(event)

	if result['flowCompletionEvent']['completionReason'] == 'SUCCESS':
		return result['flowOutputEvent']['content']['document']
	else:
		raise Exception(f'Expected flow execution failed with: {result['flowCompletionEvent']['completionReason']}')


def save_document_parts(doc_manifest: List[Dict[str, Any]], lazy_doc, output_path: str, flow_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
	response_doc_list = []
	logger.info(f"manefist: {doc_manifest}")
	logger.info(f"flow_list: {flow_list}")

	for doc_class in doc_manifest:
		class_name = doc_class['class']
		pages = "_".join(map(str, doc_class['page-indexes']))
		
		run_flow_id, run_flow_alias = next(
			((item["flow_id"], item["flow_alias_id"]) 
			 for item in flow_list if class_name == item["class_name"]),
			("", "")
		)
		
		txt_file = f"{output_path}/{class_name}/pages_{pages}.txt"
		json_file = f"{output_path}/{class_name}/pages_{pages}.json"
		
		text = "".join(lazy_doc.pages[i].get_text() for i in doc_class['page-indexes'] if i < len(lazy_doc.pages))

		save_to_s3(text, OUTPUT_BUCKET_NAME, txt_file)
		
		response_doc_list.append({
			"doc_text_s3key": txt_file,
			"JSON_s3key": json_file,
			"run_flow_id": run_flow_id,
			"run_flow_alias": run_flow_alias
		})
	logger.info(f"response_doc_list: {response_doc_list}")

	return response_doc_list

def get_text_in_tag(string: str, tag: str) -> Optional[str]:
    pattern = f'<{tag}>\n(.+?)</{tag}>'
    match = re.search(pattern, string, re.DOTALL)
    return match.group(1) if match else None


def get_supported_class_list_from_dynamodb() -> List[Dict[str, str]]:
	"""Retrieve a list of supported flow classes with their details from DynamoDB."""
	response = dynamodb.scan(TableName=IDP_FLOW_CLASS_TABLE_NAME)
	return [
		{
			"class_name": item['class_name']['S'],
			"expected_inputs": item['expected_inputs']['S'],
			"flow_id": item['flow_id']['S'],
			"flow_alias_id": item['flow_alias_id']['S']
		}
		for item in response['Items']
	]


