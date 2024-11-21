#!/bin/bash
set -eo pipefail
STACK_NAME=document-guidance-for-idp-on-aws
ACCOUNT_NUMBER=$(aws sts get-caller-identity --query Account --output text)
BUCKET_NAME="$STACK_NAME"-"$ACCOUNT_NUMBER"

SOURCE_BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs[?OutputKey==`SourceS3Bucket`].OutputValue' --output text)
DEST_BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs[?OutputKey==`DestinationS3Bucket`].OutputValue' --output text)
aws s3 rm s3://"$SOURCE_BUCKET" --recursive
aws s3 rm s3://"$DEST_BUCKET" --recursive

#FUNCTION=$(aws cloudformation describe-stack-resource --stack-name "$STACK_NAME" --logical-resource-id function --output text)
#echo $FUNCTION
sam delete --s3-bucket "$BUCKET_NAME" --stack-name "$STACK_NAME"
