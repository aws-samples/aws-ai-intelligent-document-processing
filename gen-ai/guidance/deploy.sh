#!/bin/bash
set -x
set -eo pipefail
STACK_NAME=document-guidance-for-idp-on-aws

# Get accountnumber
ACCOUNT_NUMBER=$(aws sts get-caller-identity --query Account --output text)
BUCKET_NAME="$STACK_NAME"-"$ACCOUNT_NUMBER"
# Set AWS region
export AWS_DEFAULT_REGION="us-east-1"  # Latest features require us-east-1

#If the bucket doesn't exist, create it
if ! aws s3api head-bucket --bucket "$BUCKET_NAME" 2>/dev/null; then
    aws s3 mb s3://"$BUCKET_NAME"
fi

# upload prompt flow files
KEY_PATH=prompt_flows
aws s3 sync ./prompt_flows s3://"$BUCKET_NAME"/"$KEY_PATH"

# build and deploy
sam build
sam deploy --region ${AWS_DEFAULT_REGION} --capabilities CAPABILITY_NAMED_IAM --s3-bucket "$BUCKET_NAME" --parameter-overrides PromptFlowsBucket="$BUCKET_NAME" PromptFlowsKeyPath="$KEY_PATH" \
    ReferenceName="John" \
    ReferenceLastName="Doe" \
    ReferenceDOB="09/21/1970" \
    ReferenceSSN="1234" \
    ReferenceStreet="123 Any Street" \
    ReferenceCity="Any City" \
    ReferenceState="CA" \
    ReferenceZip="92127" --stack-name "$STACK_NAME"

