import json
import boto3
import pprint
import os
from botocore.client import Config
from rhubarb import DocAnalysis, SystemPrompts, LanguageModels

pp = pprint.PrettyPrinter(indent=2)

bedrock_config = Config(connect_timeout=120, read_timeout=120, retries={'max_attempts': 0})
bedrock_client = boto3.client('bedrock-runtime')

boto3_session = boto3.session.Session()
region_name = boto3_session.region_name

#model_id = "anthropic.claude-3-haiku-20240307-v1:0" # try with both claude 3 Haiku as well as claude 3 Sonnet. for claude 3 Sonnet - "anthropic.claude-3-sonnet-20240229-v1:0"
region_id = region_name # replace it with the region you're running sagemaker notebook

bedrock_agent_runtime = boto3.client("bedrock-agent-runtime",
                              config=bedrock_config)

# Change in env var
KB_ID = os.getenv('KB_ID')


def lambda_handler(event, context):

  s3 = boto3.client("s3")
  session = boto3.Session()

  bucketName = event["bucketName"]
  DocumentClassKey = event["DocumentClassObj"]
  OrginalBucketKey = event["OrginalBucketKey"]
  OriginalfileName = (OrginalBucketKey.split("/"))[1]
  OriginalFilepath = event["OriginalFilepath"]

  outputFolder = event["outputFolder"]
  outputKeyComplianceReport = outputFolder + OriginalfileName[:len(OriginalfileName) - 4] + "_ComplianceReport"

  obj = s3.get_object(Bucket=bucketName, Key=DocumentClassKey)
  data = obj['Body'].read().decode('utf-8')
  json_data = json.loads(data)

  doc_cls = json_data['output'][0]['class']
  query = f"What are the rules for this document type: {doc_cls} "

  response = retrieve(query, KB_ID, 5)
  retrievalResults = response['retrievalResults']


  prompt_template = f"""


  Human: I need you to analyze a document for compliance with specific rules. Here's the context:

  Document Type: {doc_cls}

  Retrieved Rules:

  {retrievalResults}

  Based on the list of rules, pick out the rules that apply to the specific document type and only use those

  Document Content:


  Please perform the following tasks:
  1. Analyze each rule and how it applies to the document.
  2. For each rule, determine if the document complies or not.
  3. Provide a brief explanation for each compliance or non-compliance finding.
  4. Summarize the overall compliance status of the document.
  5. If there are any areas of ambiguity or where the rules might be interpreted in multiple ways, highlight these.

  Present your analysis in a structured format, using markdown for clarity where appropriate.

  1. Compliance Determination

  2. Explanations for Findings

  3. Overall Compliance Summary

  4. Areas of Ambiguity

  """

  schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "array",
    "items": {
      "type": "object",
      "properties": {
        "Rule": {
          "type": "string"
        },
        "Compliance": {
          "type": "boolean"
        }
      },
      "required": ["Rule", "Compliance"]
    }
  }

  da = DocAnalysis(file_path=OriginalFilepath, 
                boto3_session=boto3_session,
                modelId= LanguageModels.CLAUDE_HAIKU_V1,
                max_tokens= 4000,
                )

  complianceReport = da.run(message=prompt_template,output_schema = schema)

  s3.put_object(
    Body=json.dumps(complianceReport),
    Bucket=bucketName,
    Key=outputKeyComplianceReport
  )

  countTrue = 0
  countFalse = 0
  output = complianceReport['output']
  for i in range(len(output)):
      if output[i]['Compliance'] == True:
          countTrue += 1
      else:
          countFalse += 1

  total = countTrue + countFalse
  finalResult = ""
  if countTrue >= total / 2:
      finalResult = "Success, matches more than 50% of the rules"
  else:
      finalResult = "Failed, matches less than 50% of the rules"

  return {"Result" : finalResult, "OriginalFilepath" : OriginalFilepath, "outputFolder" : outputFolder, "bucketName" : bucketName, "OrginalBucketKey" : OrginalBucketKey,}
 
def retrieve(query, kbId, numberOfResults=5):
  return bedrock_agent_runtime.retrieve(
      retrievalQuery= {
          'text': query
      },
      knowledgeBaseId=kbId,
      retrievalConfiguration= {
          'vectorSearchConfiguration': {
              'numberOfResults': numberOfResults,
              'overrideSearchType': "HYBRID", # optional
          }
      }
  )

