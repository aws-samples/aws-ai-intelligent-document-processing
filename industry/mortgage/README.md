# Process Mortgage Documents with AWS AI Services

<div align="center">
    <p align="center">
        <img src="./images/idp-mortgage-arch.png" alt="idp-mortgage"/>
    </p>
</div>

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

4. Open the [01-document-classification.ipynb](./01-document-classification.ipynb) notebook and follow instructions in the notebook for Document Classification with Amazon Comprehend custom classifier.

5. Open the [02-document-extraction-1.ipynb](./02-document-extraction-1.ipynb) notebook and follow instructions in the notebook for Document Extraction with Amazon Textract.
   
6. Open the [02-document-extraction-2.ipynb](./02-document-extraction-2.ipynb) notebook and follow instructions in the notebook for Document Extraction with Amazon Comprehend custom entity recognizer.

7. Open the [02-document-enrichment.ipynb](./02-document-enrichment.ipynb) notebook and follow instructions in the notebook for Document enrichment (document redaction) with Amazon Comprehend PII entity recognizer.

## Clean Up

1. Follow instructions in the notebook to cleanup the resources.
2. If you created an Amazon SageMaker Studio Domain manually then please [delete it](https://docs.aws.amazon.com/sagemaker/latest/dg/gs-studio-delete-domain.html) to avoid incurring charges.
   
---
## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

