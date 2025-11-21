import json
import boto3
from datetime import datetime
import os
import logging
from typing import Dict, Optional, List
import traceback

OUTPUT_BUCKET_NAME = os.environ['OUTPUT_BUCKET_NAME']
QUEUE_URL = os.environ['QUEUE_URL']
VALIDATION_QUEUE_URL = os.environ['VALIDATION_QUEUE_URL']

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
sqs = boto3.client('sqs')
bedrock_agent = boto3.client('bedrock-agent-runtime')

def validate_sqs_event(sqs_event: Dict):
    """
    Validate the SQS event data.

    Args:
        sqs_event (Dict): The SQS event data.

    Raises:
        Exception: If the SQS event data is invalid.
    """
    if "Records" not in sqs_event:
        raise Exception("No Records section")
    if len(sqs_event["Records"]) != 1:
        raise Exception("Expected only 1 record")

def extract_previous_result(sqs_event: Dict) -> Dict:
    """
    Extract the previous result from the SQS event data.

    Args:
        sqs_event (Dict): The SQS event data.

    Returns:
        Dict: The previous result data.
    """
    body = sqs_event["Records"][0]["body"]
    return json.loads(body)

def delete_sqs_message(sqs_event: Dict):
    """
    Delete the SQS message from the queue.

    Args:
        sqs_event (Dict): The SQS event data.
    """
    receipt_handle = sqs_event["Records"][0]["receiptHandle"]
    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)

def invoke_bedrock_flow(flow_id: str, flow_alias_id: str, document: Dict) -> Dict:
    """
    Invoke the Bedrock prompt flow.

    Args:
        flow_id (str): The flow ID.
        flow_alias_id (str): The flow alias ID.
        document (Dict): The document data.

    Returns:
        Dict: The Bedrock response.
    """
    response = bedrock_agent.invoke_flow(
        flowIdentifier=flow_id,
        flowAliasIdentifier=flow_alias_id,
        inputs=[
            {
                "content": {
                    "document": document
                },
                "nodeName": "FlowInputNode",
                "nodeOutputName": "document"
            }
        ]
    )
    
    result = {}
    for event in response.get("responseStream"):
        result.update(event)
    return result

def process_bedrock_result(result):
    """
    Process the Bedrock result and return the outcome.

    Args:
        result: The Bedrock response.

    Returns:
        str: The outcome of the Bedrock result.
    """
    outcome = ""

    if result['flowCompletionEvent']['completionReason'] == 'SUCCESS':
        logger.info("Prompt flow invocation was successful! The output of the prompt flow is as follows:\n")
        outcome += result['flowOutputEvent']['content']['document']
    else:
        logger.info("The prompt flow invocation completed because of the following reason:", result['flowCompletionEvent']['completionReason'])
        outcome += result['flowCompletionEvent']['completionReason']
    return outcome

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
        return None

def find_json_files_in_directory(bucket: str, directory: str) -> List[str]:
    """
    Find all JSON files in a given S3 directory and its subdirectories.
    
    Args:
        bucket: S3 bucket name
        directory: Directory prefix to search in
        
    Returns:
        List[str]: List of JSON file keys found
    """
    json_files = []
    try:
        logger.info(f"Starting S3 directory scan", extra={
            "bucket": bucket,
            "directory": directory,
            "starting_path": f"s3://{bucket}/{directory}"
        })

        # Validate inputs
        if not bucket or not directory:
            logger.error("Invalid bucket or directory", extra={
                "bucket": bucket,
                "directory": directory
            })
            return []

        # Try to list the directory first to verify access
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception as e:
            logger.error("Error accessing bucket", extra={
                "bucket": bucket,
                "error": str(e),
                "error_type": str(type(e).__name__)
            })
            return []

        paginator = s3.get_paginator('list_objects_v2')
        
        # Debug: Print pagination parameters
        logger.debug("Pagination parameters", extra={
            "bucket": bucket,
            "prefix": directory
        })

        try:
            page_count = 0
            total_objects = 0
            
            for page in paginator.paginate(Bucket=bucket, Prefix=directory):
                page_count += 1
                contents = page.get('Contents', [])
                total_objects += len(contents)
                
                logger.debug("Processing S3 page", extra={
                    "page_number": page_count,
                    "objects_in_page": len(contents)
                })
                
                for obj in contents:
                    obj_key = obj['Key']
                    
                    # Debug: Print each object being checked
                    logger.debug("Checking object", extra={
                        "key": obj_key,
                        "size": obj.get('Size', 0),
                        "last_modified": str(obj.get('LastModified', ''))
                    })
                    
                    if obj_key.endswith('.json'):
                        logger.info(f"Found JSON file", extra={
                            "json_file": obj_key,
                            "directory": directory,
                            "size": obj.get('Size', 0)
                        })
                        json_files.append(obj_key)

            # Summary logging
            logger.info(f"Completed directory scan", extra={
                "directory": directory,
                "files_found": len(json_files),
                "total_pages": page_count,
                "total_objects": total_objects,
                "json_files": json_files
            })

        except Exception as e:
            logger.error("Error during pagination", extra={
                "error": str(e),
                "error_type": str(type(e).__name__),
                "bucket": bucket,
                "directory": directory
            })
            logger.info(traceback.format_exc())
            return []
        
        return json_files
        
    except Exception as e:
        logger.error("Error scanning directory for JSON files", extra={
            "error": str(e),
            "error_type": str(type(e).__name__),
            "bucket": bucket,
            "directory": directory,
            "traceback": traceback.format_exc()
        })
        return []

def process_document(document: Dict, case_id: str):
    """
    Process a document by invoking the Bedrock prompt flow and saving the result to S3.

    Args:
        document (Dict): The document data.
        case_id (str): The case ID.
    """
    try:
        flow_id = document["run_flow_id"]
        flow_alias_id = document["run_flow_alias"]
        directory, f = os.path.split(document['doc_text_s3key'])
        report_s3key = os.path.join(directory, "report.txt")

        # Add case_id and current date to document
        document["todays_date"] = datetime.now().strftime("%Y-%m-%d")
        document["case_id"] = case_id

        # Log the document processing start
        logger.info(f"Starting document processing", extra={
            "case_id": case_id,
            "document_path": document['doc_text_s3key'],
            "document_type": document.get('document_type', 'UNKNOWN')
        })

        # Invoke Bedrock flow and process result
        result = invoke_bedrock_flow(flow_id, flow_alias_id, document)
        outcome = process_bedrock_result(result)
        
        # Save result to S3
        saved_key = save_to_s3(outcome, OUTPUT_BUCKET_NAME, report_s3key)
        if not saved_key:
            logger.error(f"Failed to save report to S3 for document", extra={
                "case_id": case_id,
                "doc_key": document['doc_text_s3key']
            })
            return

        # Get the case directory (two levels up from the document directory)
        # Split the path and rebuild it to ensure proper formatting
        path_parts = directory.split('/')
        if len(path_parts) >= 2:
            case_directory = '/'.join(path_parts[:-2]) if path_parts[:-2] else ''
        else:
            case_directory = ''
        
        logger.info(f"Scanning for JSON files", extra={
            "case_id": case_id,
            "case_directory": case_directory,
            "document_directory": directory,
            "path_parts": path_parts,
            "full_path": f"s3://{OUTPUT_BUCKET_NAME}/{case_directory}"
        })

        # Find all JSON files in the case directory and its subdirectories
        json_files = find_json_files_in_directory(OUTPUT_BUCKET_NAME, case_directory)
        
        if not json_files:
            logger.warning(f"No JSON files found", extra={
                "case_id": case_id,
                "case_directory": case_directory,
                "document_directory": directory
            })
            return

        logger.info(f"Found JSON files to process", extra={
            "case_id": case_id,
            "num_files": len(json_files),
            "files": json_files
        })

        # Process each JSON file found
        for json_key in json_files:
            try:
                logger.info(f"Sending validation message for JSON file", extra={
                    "case_id": case_id,
                    "json_key": json_key
                })
                
                send_validation_message(case_id, document, outcome, json_key)
                
            except Exception as e:
                logger.error(f"Error processing JSON file", extra={
                    "error": str(e),
                    "error_type": str(type(e).__name__),
                    "case_id": case_id,
                    "json_key": json_key
                })
                continue

    except Exception as e:
        # Log the full traceback at debug level for troubleshooting
        logger.debug(f"Full traceback for document processing error", extra={
            "traceback": traceback.format_exc()
        })
        
        # Log a warning with the essential error information
        logger.warning(f"Failed to process document", extra={
            "error": str(e),
            "error_type": str(type(e).__name__),
            "case_id": case_id,
            "doc_key": document.get('doc_text_s3key')
        })
        
        # Re-raise the exception to be handled by the caller
        raise

def send_validation_message(case_id: str, document: Dict, outcome: str, json_key: str):
    """
    Send a validation message for a specific JSON file.
    
    Args:
        case_id: The case ID
        document: The document data
        outcome: The processed outcome
        json_key: The S3 key of the JSON file to validate
    """
    try:
        validation_message = {
            'case_id': case_id,
            'document_type': document.get('document_type', 'UNKNOWN'),
            'processed_data': outcome,
            's3_location': {
                'bucket': OUTPUT_BUCKET_NAME,
                'key': json_key,
                'related_txt': document['doc_text_s3key']
            }
        }
        
        logger.info(f"Sending validation message", extra={
            "case_id": case_id,
            "json_key": json_key
        })
        
        response = sqs.send_message(
            QueueUrl=VALIDATION_QUEUE_URL,
            MessageBody=json.dumps(validation_message)
        )
        
        logger.info(f"Successfully sent validation message", extra={
            "case_id": case_id,
            "json_key": json_key,
            "message_id": response.get('MessageId')
        })
        
    except Exception as e:
        logger.error(f"Error sending validation message", extra={
            "error": str(e),
            "case_id": case_id,
            "json_key": json_key
        })
        logger.info(traceback.format_exc())

def lambda_handler(sqs_event: Dict, context) -> bool:
    """
    Lambda function handler for processing SQS events.

    Args:
        sqs_event (Dict): The SQS event data.
        context: The Lambda context object.

    Returns:
        bool: True if the function executed successfully, False otherwise.
    """
    logger.info(f"Processing event", extra={"event": json.dumps(sqs_event)})
    
    try:
        validate_sqs_event(sqs_event)
        previous_result = extract_previous_result(sqs_event)
        case_id = previous_result["case_id"]
        document_manifest = previous_result["documents"]

        logger.info(f"Processing documents", extra={
            "case_id": case_id,
            "num_documents": len(document_manifest)
        })

        processed_count = 0
        for document in document_manifest:
            try:
                process_document(document, case_id)
                processed_count += 1
            except Exception as e:
                logger.error(f"Error processing individual document", extra={
                    "error": str(e),
                    "case_id": case_id,
                    "doc_key": document.get('doc_text_s3key')
                })
                # Continue processing other documents even if one fails
                continue

        logger.info(f"Completed document processing", extra={
            "case_id": case_id,
            "processed_count": processed_count,
            "total_documents": len(document_manifest)
        })

        delete_sqs_message(sqs_event)
        return True
        
    except Exception as e:
        logger.error(f"Error processing SQS event", extra={
            "error": str(e)
        })
        logger.info(traceback.format_exc())
        return False