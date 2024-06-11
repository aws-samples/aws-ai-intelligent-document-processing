import json
import boto3
from botocore.exceptions import ClientError
bedrock = boto3.client(service_name="bedrock-runtime", region_name="us-east-1")

# Create a global function to call Bedrock. 
def get_response_from_claude(prompt):
	"""
	Invokes Anthropic Claude 3 Haiku to run a text inference using the input
	provided in the request body.

	:param prompt:            The prompt that you want Claude 3 to use.
	:return: Inference response from the model.
	"""

	# Invoke the model with the prompt and the encoded image
	model_id = "anthropic.claude-3-sonnet-20240229-v1:0"
	request_body = {
		"anthropic_version": "bedrock-2023-05-31",
		"max_tokens": 2048,
        "temperature":0.5,
		"messages": [
			{
				"role": "user",
				"content": [
					{
						"type": "text",
						"text": prompt,
					},
				],
			}
		],
	}

	try:
		response = bedrock.invoke_model(
			modelId=model_id,
			body=json.dumps(request_body),
		)

		# Process and print the response
		result = json.loads(response.get("body").read())
		input_tokens = result["usage"]["input_tokens"]
		output_tokens = result["usage"]["output_tokens"]
		# the current Bedrock Claude Messagees API only supports text content in responses
		text_response = result["content"][0]["text"]

        # return a tuple with 3 values
		return text_response, input_tokens, output_tokens
	except ClientError as err:
		logger.error(
			"Couldn't invoke Claude 3 Sonnet. Here's why: %s: %s",
			err.response["Error"]["Code"],
			err.response["Error"]["Message"],
		)
		raise