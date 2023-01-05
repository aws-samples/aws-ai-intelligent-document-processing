"""
kicks off Step Function executions with Queries
"""
import json
import logging
import os
from urllib.parse import unquote_plus
import textractmanifest as tm
from datetime import datetime
import re

import boto3

logger = logging.getLogger(__name__)

step_functions_client = boto3.client(service_name='stepfunctions')
TRIGGER_TYPES = []


def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))

    state_machine_arn = os.environ.get('STATE_MACHINE_ARN', None)
    if not state_machine_arn:
        raise Exception("no STATE_MACHINE_ARN set")
    logger.info(f"STATE_MACHINE_ARN: {state_machine_arn}")

    s3_bucket = ""
    s3_key = ""

    for record in event['Records']:
        event_source = record["eventSource"]
        if event_source == "aws:s3":
            s3_bucket = record['s3']['bucket']['name']
            s3_key = unquote_plus(record['s3']['object']['key'])
        elif event_source == "aws:sqs":
            message = json.loads(record["body"])
            s3_bucket = message['bucket']
            s3_key = message['key']
        else:
            logger.error('unsupported event_source: {}'.format(event_source))

        filename = os.path.basename(s3_key) + datetime.utcnow().isoformat()

        filename = re.sub(r'[^A-Za-z0-9-_]', '', filename)
        filename = filename[:80]
        if s3_bucket and s3_key:
            manifest: tm.IDPManifest = tm.IDPManifest()
            queries_config = [
                # tm.Query(text="What is the address?", alias="ADDRESS"),
                # tm.Query(text="What is the name?", alias="NAME")
                tm.Query(text="What is the company address?", alias="COMPANY_ADDRESS"),
                tm.Query(text="What is the employee address?", alias="EMPLOYEE_ADDRESS"),
                tm.Query(text="What is the Pay Date?", alias="PAYSTUB_PERIOD_PAY_DATE"),
                tm.Query(text="What is the Pay Period Start Date?", alias="PAYSTUB_PERIOD_START_DATE"),
                tm.Query(text="What is the Pay Period End Date?", alias="PAYSTUB_PERIOD_END_DATE"),
                tm.Query(text="What is the Employee Name?", alias="PAYSTUB_PERIOD_EMPLOYEE_NAME"),
                tm.Query(text="What is the company Name?", alias="PAYSTUB_PERIOD_COMPANY_NAME"),
                tm.Query(text="What is the Current Gross Pay?", alias="PAYSTUB_PERIOD_CURRENT_GROSS_PAY"),
                tm.Query(text="What is the YTD Gross Pay?", alias="PAYSTUB_PERIOD_YTD_GROSS_PAY")
            ]
            manifest.s3_path = f"s3://{s3_bucket}/{s3_key}"
            manifest.queries_config = queries_config
            manifest.textract_features = ["QUERIES"]
            logger.debug(f"manifest: {tm.IDPManifestSchema().dumps(manifest)}")

            response = step_functions_client.start_execution(
                stateMachineArn=state_machine_arn,
                name=filename,
                input=tm.IDPManifestSchema().dumps(manifest))
            logger.info(response)
        else:
            raise ValueError(
                f"no s3_bucket: {s3_bucket} and/or s3_key: {s3_key} given.")
