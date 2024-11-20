import json
import boto3
import os
import logging
from typing import Dict, List, Optional
import traceback
from datetime import datetime
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.validation import validator
from aws_lambda_powertools.utilities.validation.exceptions import SchemaValidationError
from aws_lambda_powertools.utilities.typing import LambdaContext

# Set up logging
logger = Logger(service="DocValidationService")

# Initialize AWS clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

# Schema definitions
DRIVERS_LICENSE_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "document_type": {"type": "string", "enum": ["DRIVER LICENSE"]},
        "expiration_date": {"type": "string", "pattern": "^\\d{2}/\\d{2}/\\d{4}$"},
        "license_number": {"type": "string"},
        "last_name": {"type": "string"},
        "first_name": {"type": "string"},
        "address": {
            "type": "object",
            "properties": {
                "street": {"type": "string"},
                "city": {"type": "string"},
                "state": {"type": "string", "pattern": "^[A-Z]{2}$"},
                "zip_code": {"type": "string", "pattern": "^\\d{5}$"}
            },
            "required": ["street", "city", "state", "zip_code"],
            "additionalProperties": False
        },
        "date_of_birth": {"type": "string", "pattern": "^\\d{2}/\\d{2}/\\d{4}$"},
        "ssn_status": {"type": "string"},
        "donor_status": {"type": "string"},
        "license_class": {"type": "string", "enum": ["A", "B", "C", "M"]},
        "restrictions": {"type": "string"},
        "sex": {"type": "string", "enum": ["M", "F", "X"]},
        "hair_color": {"type": "string"},
        "eye_color": {"type": "string"},
        "height": {"type": "string"},
        "weight": {"type": "string"},
        "issue_date": {"type": "string", "pattern": "^\\d{2}/\\d{2}/\\d{4}$"},
        "additional_info": {"type": "string"}
    },
    "required": [
        "document_type", "expiration_date", "license_number", 
        "last_name", "first_name", "address", "date_of_birth"
    ],
    "additionalProperties": False
}

URLA_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "applicant": {
            "type": "object",
            "properties": {
                "fullName": {"type": "string"},
                "ssn": {"type": "string"},
                "dateOfBirth": {"type": "string", "pattern": "^\\d{2}/\\d{2}/\\d{4}$"},
                "maritalStatus": {"type": "string"},
                "currentAddress": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"},
                        "state": {"type": "string", "pattern": "^[A-Z]{2}$"},
                        "zip": {"type": "string", "pattern": "^\\d{5}$"},
                        "yearsAtAddress": {"type": "number"}
                    },
                    "required": ["street", "city", "state", "zip"]
                }
            },
            "required": ["fullName", "ssn", "dateOfBirth"]
        },
        "employmentInfo": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "employerName": {"type": "string"},
                    "monthlyIncome": {"type": "number"}
                },
                "required": ["employerName", "monthlyIncome"]
            }
        }
    },
    "required": ["applicant", "employmentInfo"]
}

def validate_schema(schema: Dict, data: Dict) -> None:
    """
    Validate data against a JSON schema.
    
    Args:
        schema: JSON schema to validate against
        data: Data to validate
        
    Raises:
        SchemaValidationError: If validation fails
    """
    try:
        from jsonschema import validate
        validate(instance=data, schema=schema)
    except Exception as e:
        raise SchemaValidationError(f"Schema validation failed: {str(e)}")

# If the incomming message doesn't tell us about a json file but instead tells us about the txt file we can use this code to find it
def find_corresponding_json(bucket: str, txt_key: str) -> Optional[str]:
    """
    Find the corresponding JSON file in the same folder as the text file.
    
    Args:
        bucket: S3 bucket name
        txt_key: Key of the text file
        
    Returns:
        Optional[str]: Key of the corresponding JSON file if found, None otherwise
    """
    try:
        # Get the directory path from the text file key
        directory = os.path.dirname(txt_key)
        base_name = os.path.basename(txt_key)
        file_prefix = base_name.split('.')[0]  # Get 'pages_0' from 'pages_0.txt'
        
        logger.debug("Searching for JSON file", extra={
            "directory": directory,
            "base_name": base_name,
            "file_prefix": file_prefix
        })
        
        # List objects in the directory
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=directory):
            for obj in page.get('Contents', []):
                obj_key = obj['Key']
                if obj_key.endswith('.json') and file_prefix in obj_key:
                    logger.info("Found corresponding JSON file", extra={
                        "txt_file": txt_key,
                        "json_file": obj_key
                    })
                    return obj_key
                    
        logger.warning("No corresponding JSON file found", extra={
            "txt_file": txt_key,
            "directory": directory
        })
        return None
        
    except Exception as e:
        logger.exception("Error searching for JSON file", extra={
            "error_type": str(type(e).__name__),
            "txt_key": txt_key
        })
        return None

def read_s3_json(bucket: str, key: str) -> Optional[Dict]:
    """
    Read and parse JSON file from S3, handling potential text before/after JSON content.
    """
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            start_idx = content.find('{')
            end_idx = content.rfind('}') + 1
            
            if start_idx != -1 and end_idx != 0:
                json_content = content[start_idx:end_idx]
                return json.loads(json_content)
            
            logger.error("No valid JSON found in file", extra={"file_key": key})
            return None
                
    except Exception as e:
        logger.exception("Error reading JSON from S3", extra={
            "bucket": bucket,
            "key": key,
            "error_type": str(type(e).__name__)
        })
        return None

def determine_document_type(file_path: str, content: Dict) -> str:
    """
    Determine document type from file path and content.
    """
    if "DRIVERS_LICENSE" in file_path or content.get('document_type') == "DRIVER LICENSE":
        return "DRIVERS_LICENSE"
    elif "URLA_1003" in file_path:
        return "URLA"
    return "UNKNOWN"





def save_validation_results(results: Dict, case_id: str, document_type: str, 
                          s3_location: Dict) -> None:
    """
    Save validation results to S3.
    """
    try:
        directory = os.path.dirname(s3_location['key'])
        validation_key = os.path.join(directory, 'validation_results.json')
        
        results.update({
            'case_id': case_id,
            'original_document_location': s3_location,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        s3.put_object(
            Bucket=s3_location['bucket'],
            Key=validation_key,
            Body=json.dumps(results, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        
        logger.info("Validation results saved", extra={
            "results_location": f"s3://{s3_location['bucket']}/{validation_key}"
        })
        
    except Exception as e:
        # Log the full traceback at debug level for troubleshooting
        logger.debug(f"Full traceback for document processing error", extra={
            "traceback": traceback.format_exc()
        })
        
        # Log a warning with the essential error information
        logger.warning(f"Failed to save validation results", extra={
            "error_type": str(type(e).__name__),
            "case_id": case_id,
            "document_type": document_type
        })
        raise

def validate_document(document_type: str, doc_data: Dict) -> Dict:
    """
    Validate document data against schema and reference data.
    """
    validation_results = {
        'document_type': document_type,
        'validation_status': 'PASSED',
        'validation_checks': [],
        'schema_validation': 'PASSED',
        'needs_manual_review': False
    }

    try:
        # Perform schema validation
        if document_type == "DRIVERS_LICENSE":
            try:
                logger.debug("Validating drivers license", extra={
                    "data_sample": {k: doc_data[k] for k in list(doc_data.keys())[:3]} if doc_data else None
                })
                
                # Define validator outside of validate_document
                validate_schema(DRIVERS_LICENSE_SCHEMA, doc_data)
                
                # Add schema validation result
                validation_results['validation_checks'].append({
                    'check': 'schema_validation',
                    'passed': True,
                    'message': 'Document structure validates against schema'
                })
                
                
            except SchemaValidationError as e:
                logger.error("Drivers license schema validation failed", extra={
                    "error": str(e),
                    "document_data": doc_data
                })
                validation_results['schema_validation'] = 'FAILED'
                validation_results['validation_checks'].append({
                    'check': 'schema_validation',
                    'passed': False,
                    'message': f"Schema validation failed: {str(e)}"
                })
                validation_results['needs_manual_review'] = True
                
        elif document_type == "URLA":
            try:
                logger.debug("Validating URLA", extra={
                    "data_sample": {k: doc_data[k] for k in list(doc_data.keys())[:3]} if doc_data else None
                })
                
                # Validate schema
                validate_schema(URLA_SCHEMA, doc_data)
                
                # Add schema validation result
                validation_results['validation_checks'].append({
                    'check': 'schema_validation',
                    'passed': True,
                    'message': 'Document structure validates against schema'
                })
                
                
            except SchemaValidationError as e:
                logger.error("URLA schema validation failed", extra={
                    "error": str(e),
                    "document_data": doc_data
                })
                validation_results['schema_validation'] = 'FAILED'
                validation_results['validation_checks'].append({
                    'check': 'schema_validation',
                    'passed': False,
                    'message': f"Schema validation failed: {str(e)}"
                })
                validation_results['needs_manual_review'] = True
                
        else:
            validation_results['schema_validation'] = 'SKIPPED'
            validation_results['validation_checks'].append({
                'check': 'document_type',
                'passed': False,
                'message': f'Unknown document type: {document_type}'
            })
            validation_results['needs_manual_review'] = True

        # Set overall status based on all checks
        failed_checks = [check for check in validation_results['validation_checks'] 
                        if not check['passed']]
        if failed_checks:
            validation_results['validation_status'] = 'FAILED'
            validation_results['needs_manual_review'] = True

    except Exception as e:
        logger.exception("Unexpected error during document validation", extra={
            "error_type": str(type(e).__name__),
            "document_type": document_type,
            "error_details": str(e)
        })
        validation_results['validation_status'] = 'ERROR'
        validation_results['schema_validation'] = 'ERROR'
        validation_results['validation_checks'].append({
            'check': 'system_error',
            'passed': False,
            'message': f'System error during validation: {str(e)}'
        })
        validation_results['needs_manual_review'] = True

    return validation_results

@logger.inject_lambda_context
def lambda_handler(event: Dict, context: LambdaContext) -> Dict:
    """
    Process SQS messages containing document data for validation.
    """
    try:
        logger.info("Starting validation process", extra={
            "num_records": len(event.get('Records', []))
        })
        
        processed_documents = 0
        for record in event.get('Records', []):
            try:
                message_body = json.loads(record['body'])
                s3_location = message_body.get('s3_location', {})
                
                if not s3_location:
                    logger.error("Missing S3 location in message", extra={
                        "message_body": message_body
                    })
                    continue
                
                bucket = s3_location.get('bucket')
                txt_key = s3_location.get('key')
                
                if not (bucket and txt_key):
                    logger.error("Missing bucket or key in S3 location", extra={
                        "s3_location": s3_location
                    })
                    continue
                
                # Find corresponding JSON file
                json_key = find_corresponding_json(bucket, txt_key)
                if not json_key:
                    continue
                
                # Read and process JSON file
                json_content = read_s3_json(bucket, json_key)
                if not json_content:
                    continue
                
                document_type = determine_document_type(json_key, json_content)
                logger.info("Processing document", extra={
                    "document_type": document_type,
                    "txt_key": txt_key,
                    "json_key": json_key
                })
                
                logger.debug("Document content for validation", extra={
                    "document_type": document_type,
                    "content_keys": list(json_content.keys()) if json_content else None,
                    "sample_data": {k: json_content[k] for k in list(json_content.keys())[:3]} if json_content else None
                })

                validation_results = validate_document(document_type, json_content)
                
                save_validation_results(
                    validation_results,
                    message_body.get('case_id', 'unknown'),
                    document_type,
                    {'bucket': bucket, 'key': json_key}
                )
                
                processed_documents += 1
                
            except json.JSONDecodeError as e:
                logger.error("Invalid JSON in SQS message", extra={
                    "error": str(e),
                    "raw_message": record.get('body', '')
                })
                continue
            except Exception as e:
                logger.exception("Error processing record", extra={
                    "error_type": str(type(e).__name__),
                    "record": record
                })
                continue
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Validation processing complete',
                'processed_documents': processed_documents
            })
        }
        
    except Exception as e:
        logger.exception("Validation process failed", extra={
            "error_type": str(type(e).__name__)
        })
        return {
            'statusCode': 500,
            'body': json.dumps('Error during validation processing')
        }