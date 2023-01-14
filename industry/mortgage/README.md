# Process Mortgage Documents with AWS AI Services

Mortgage companies often need to process large volumes of diverse document types to extract business-critical data. Today, lenders are faced with the challenge of managing a manual, slow, and expensive process to extract data and insights from documents.

To help address the issue, Amazon Textract Analyze Lending improves business process efficiency through automation and accuracy, reducing loan processing costs and providing the ability to scale quickly based on changing demand. Analyze Lending is feature of Amazon Textract's managed intelligent document processing API that fully automates the classification and extraction of information from loan packages. Customers simply upload their mortgage loan documents to the Analyze Lending API and its pre-trained machine learning models will automatically classify and split by document type, and extract critical fields of information from a mortgage loan packet.

![Architecture](./images/idp-mortgage-arch.png)

## Get Started

1. Setup an [Amazon SageMaker Studio domain](https://docs.aws.amazon.com/sagemaker/latest/dg/gs-studio-onboard.html).
2. Log-on to Amazon SageMaker Studio. Open a terminal from _File_ menu > _New_ > _Terminal_
   
<div align="center">
    <p align="center">
       <img src="./images/sm-studio-terminal.png" alt="sf"/>
    </p>
</div>

3. Clone this repository

```sh
git clone https://github.com/aws-samples/aws-ai-intelligent-document-processing idp_workshop
cd idp_workshop/industry/mortgage
```

4. Open the [01-document-classification.ipynb](./01-document-classification.ipynb) notebook and follow instructions in the notebook for Document Classification with Amazon Textract Analyze Lending API.

5. Open the [02-document-extraction.ipynb](./02-document-extraction.ipynb) notebook and follow instructions in the notebook for Document Extraction with Amazon Textract Analyze Lending API.
   
6. Open the [02-document-enrichment.ipynb](./02-document-enrichment.ipynb) notebook and follow instructions in the notebook for Document enrichment (document redaction) with Amazon Comprehend PII entity recognizer.

## Clean Up

1. Follow instructions in the notebook to cleanup the resources.
2. If you created an Amazon SageMaker Studio Domain manually then please [delete it](https://docs.aws.amazon.com/sagemaker/latest/dg/gs-studio-delete-domain.html) to avoid incurring charges.
   
---
## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

