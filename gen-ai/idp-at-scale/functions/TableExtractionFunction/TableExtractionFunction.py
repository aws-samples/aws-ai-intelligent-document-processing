import json
import json
import boto3
import pprint
import os
from botocore.client import Config
from rhubarb import DocAnalysis, SystemPrompts, LanguageModels

table_schema = {
  "additionalProperties": {
    "type": "object",
    "patternProperties": {
      "^(2022|2023)$": {
        "type": "object",
        "properties": {
          "Net Sales": {
            "type": "object",
            "properties": {
              "North America": {
                "type": "number"
              },
              "International": {
                "type": "number"
              },
              "AWS": {
                "type": "number"
              },
              "Consolidated": {
                "type": "number"
              }
            },
            "required": ["North America", "International", "AWS", "Consolidated"]
          },
          "Year-over-year Percentage Growth (Decline)": {
            "type": "object",
            "properties": {
              "North America": {
                "type": "number"
              },
              "International": {
                "type": "number"
              },
              "AWS": {
                "type": "number"
              },
              "Consolidated": {
                "type": "number"
              }
            },
            "required": ["North America", "International", "AWS", "Consolidated"]
          },
          "Year-over-year Percentage Growth, excluding the effect of foreign exchange rates": {
            "type": "object",
            "properties": {
              "North America": {
                "type": "number"
              },
              "International": {
                "type": "number"
              },
              "AWS": {
                "type": "number"
              },
              "Consolidated": {
                "type": "number"
              }
            },
            "required": ["North America", "International", "AWS", "Consolidated"]
          },
          "Net Sales Mix": {
            "type": "object",
            "properties": {
              "North America": {
                "type": "number"
              },
              "International": {
                "type": "number"
              },
              "AWS": {
                "type": "number"
              },
              "Consolidated": {
                "type": "number"
              }
            },
            "required": ["North America", "International", "AWS", "Consolidated"]
          }
        },
        "required": ["Net Sales", "Year-over-year Percentage Growth (Decline)", "Year-over-year Percentage Growth, excluding the effect of foreign exchange rates", "Net Sales Mix"]
      }
    }
  }
}

session = boto3.Session()


def lambda_handler(event, context):

    print(event)
    bucketName = event["bucketName"]
    OrginalBucketKey = event["OrginalBucketKey"]
    OriginalfileName = (OrginalBucketKey.split("/"))[1]
    OriginalFilepath = event["OriginalFilepath"]
    
    outputFolder = event["outputFolder"]
    outputKeyTableExtraction = outputFolder + OriginalfileName[:len(OriginalfileName) - 4] + "_TableExtraction"
    
    da = DocAnalysis(file_path=OriginalFilepath, 
                boto3_session=session,
                modelId=LanguageModels.CLAUDE_HAIKU_V1,

                pages=[1])
    tableExtraction = da.run(message="Give me data in the results of operation table from this 10-K SEC filing document. Use the schema provided.", 
                  output_schema=table_schema)

    s3 = boto3.client("s3")

    s3.put_object(
      Body=json.dumps(tableExtraction),
      Bucket=bucketName,
      Key=outputKeyTableExtraction
    )
    
    return "Success"
