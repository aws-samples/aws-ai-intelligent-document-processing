import os
import boto3
import re
from langchain.schema import Document
        
def read_document(doc_path, return_as: str="plaintext") -> list:
    document = []
    s3 = boto3.client('s3')
    match = re.match(r's3://([^/]+)(?:/(.*))?', doc_path)
    bucket = match.group(1)
    prefix = match.group(2) if match.group(2) else ""

    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in objects.get('Contents', []):
        response = s3.get_object(Bucket=bucket, Key=obj['Key'])
        content = response['Body'].read().decode('utf-8')
        document.append(content)

    if return_as == "plaintext":
        return document

    if return_as == "langchain_doc":
        document = [Document(page_content = x) for x in document]
        return document
                