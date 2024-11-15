import json
import boto3
from rhubarb import DocAnalysis, SystemPrompts, LanguageModels

import random


s3 = boto3.client("s3")
session = boto3.Session()
def lambda_handler(event, context):
    
    prefix = str(random.randrange(1, 10000))
    suffix = str(random.randrange(1, 10000))
    filePrefix = prefix + suffix

    
    bucketName = event["bucketName"]
    bucketKey = event["bucketKey"]
    fileName = (bucketKey.split("/"))[1]
    outputFolder = "output/" + filePrefix + "/"
    outputKeyDocumentClass = outputFolder + fileName[:len(fileName) - 4] + "_DocumentClass"
    outputKeySummary = outputFolder + fileName[:len(fileName) - 4] + "_Summary"
    
    
    filepath = "s3://" + bucketName + "/" + bucketKey
  
    
    
    # single-class classification
    
    da = DocAnalysis(file_path=filepath,
                 boto3_session=session,
                 system_prompt=SystemPrompts().ClassificationSysPrompt)
                 
    documentClass = da.run(message="""Given the document, classify the pages into the following classes
                        <classes>
                        DRIVERS_LICENSE  # a driver's license
                        INSURANCE_ID     # a medical insurance ID card
                        RECEIPT          # a store receipt
                        BANK_STATEMENT   # a bank statement
                        W2               # a W2 tax document
                        MOM              # a minutes of meeting or meeting notes
                        </classes>""")
    
    s3.put_object(
     Body=json.dumps(documentClass),
     Bucket=bucketName,
     Key=outputKeyDocumentClass
    )
    
    
    da = DocAnalysis(file_path=filepath, 
                     boto3_session=session,
                     modelId=LanguageModels.CLAUDE_HAIKU_V1,
                     system_prompt=SystemPrompts().SummarySysPrompt)
                     
    summary = da.run(message="Give me a brief summary of this document.")
    
    s3.put_object(
     Body=json.dumps(summary),
     Bucket=bucketName,
     Key=outputKeySummary
    )
    
    return {"bucketName" : bucketName, "OriginalFilepath" : filepath, "OrginalBucketKey" : bucketKey, "outputFolder" : outputFolder, "DocumentClassObj" : outputKeyDocumentClass}
