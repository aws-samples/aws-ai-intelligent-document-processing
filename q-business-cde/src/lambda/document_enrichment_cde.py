import boto3
import logging
import json
import re
from textractor import Textractor
from textractor.data.constants import TextractFeatures
from textractor.data.text_linearization_config import TextLinearizationConfig
from textractcaller.t_call import call_textract, Textract_Features
from textractprettyprinter.t_pretty_print import get_text_from_layout_json
import os


os.environ["LD_LIBRARY_PATH"] = f"/opt/python/bin/:{os.environ['LD_LIBRARY_PATH']}"
os.environ["PATH"] = f"/opt/python/bin/:{os.environ['PATH']}"

# logging mechanism
logger = logging.getLogger()
logger.setLevel(logging.INFO)
 
# boto3 clients
textract = boto3.client('textract')
s3 = boto3.client('s3')
# change the region accordingly.
extractor = Textractor(region_name="us-east-1")


def lambda_handler(event, context):
    logger.info("Received event: %s" % json.dumps(event))
    s3Bucket = event.get("s3Bucket")
    s3ObjectKey = event.get("s3ObjectKey")
    metadata = event.get("metadata")
    
    # search logic for documents in S3 bucket
    match = re.search(r's3://(.*?)/(.*?\.(pdf|png|jpeg|jpg))', s3ObjectKey, re.IGNORECASE)
    if match:
        extracted_uri = match.group()
        bucket_name = match.group(1)
        key_name = match.group(2)
        s3_uri = "s3://"+ s3Bucket+ "/"+key_name
        # start_document_analysis() is an synchronous API provided by Textract that is used for handling multi-page PDFs. 
        # Switch to analyze_document() API to detect and analyze text in a single-page document synchronously in near real time. 
        document = extractor.start_document_analysis(
            file_source=s3_uri,
            save_image=False,
            features=[TextractFeatures.LAYOUT]
        )
    # store linearized text in output variable.
    output = ''
   
    # We will leverage the `TextLinearizationConfig <>`__ object which has over 40 options to tailor the text linearization to your use case.
    config = TextLinearizationConfig(
        hide_figure_layout=False,
        title_prefix="# ",
        section_header_prefix="## "
    )
  
    output = document.get_text(config=config)
    # All text in the text document is extracted and saved in ".txt" format that Amazon Q supports.

    new_key = 'cde_output/layout/' + key_name + '.txt'

    else:
        documentBeforeCDE = s3.get_object(Bucket = s3Bucket, Key = s3ObjectKey)
        beforeCDE = documentBeforeCDE['Body'].read();
        afterCDE = beforeCDE #Do Nothing for now
        new_key = 'cde_output/' + s3ObjectKey
    # Each S3 Object is considered a single document. All the .txt files are stored in S3 data-source.
    s3.put_object(Bucket = s3Bucket, Key = new_key, Body=output)
    return {
        "version" : "v0",
        "s3ObjectKey": new_key,
        "metadataUpdates": []
    }
