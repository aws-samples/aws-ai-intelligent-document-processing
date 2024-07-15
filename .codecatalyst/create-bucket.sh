#!/bin/sh

if [ $# -ne 2 ]; then
  echo "Expected 2 arguments S3 bucket name and region"
  exit 1   
fi
S3_BUCKET_NAME=$1
REGION=$2
echo "aws s3api head-bucket --bucket "${S3_BUCKET_NAME}" --region "${REGION}" 2>&1"

bucketstatus=$(aws s3api head-bucket --bucket "${S3_BUCKET_NAME}" --region "${REGION}" 2>&1)

if [ $? -eq 0 ]
then
  echo "${S3_BUCKET_NAME} found\n"
  echo "$bucketstatus"
  exit 0
else
  echo "${S3_BUCKET_NAME} not found. Creating bucket.\n"
  echo "aws s3api create-bucket --bucket "${S3_BUCKET_NAME}" --region "${REGION}" --create-bucket-configuration LocationConstraint="${REGION}""
  bucketstatus=$(aws s3api create-bucket --bucket "${S3_BUCKET_NAME}" --region "${REGION}" --create-bucket-configuration LocationConstraint="${REGION}")
  if [ $? -eq 0 ]
  then
    echo "${S3_BUCKET_NAME} Created\n"
    echo "$bucketstatus"
    exit 0
  else
    echo "Unable to create bucket ${S3_BUCKET_NAME} \n"
    exit 1
  fi
fi
