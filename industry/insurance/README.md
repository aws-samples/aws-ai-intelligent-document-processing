# Intelligent document processing with AI Services in Insurance Industry




<div align="center">
    <p align="center">
        <img src="./images/IDP_Insurance_Arch.png" alt="idp-insurance" width="1000" height="500"/>
    </p>
</div>


## Getting started



1. Setup an **Amazon SageMaker Studio domain.**
    
    To execute all the Jupyter Notebooks in this repository, we will first need to create a SageMaker Studio domain. 

    The CloudFormation template to create the SageMaker Studio domain and all the related resources, such as IAM Roles, S3 Bucket policies, Permissions for Services like Amazon Textract and Amazon Comprehend, etc. is included under the `/dist` directory in the template. 

    Follow the steps from **Getting Started** section in our general IDP [ GitHub repository](https://github.com/aws-samples/aws-ai-intelligent-document-processing) to create the CloudFormation stack using the `idp-deploy.yaml` file.

2. Log-on to Amazon SageMaker Studio. Open a terminal from _File_ menu > _New_ > _Terminal_
   
<div align="center">
    <p align="center">
       <img src="./images/sm-studio-terminal.png" alt="sf" width="900" height="500"/>
    </p>
</div>

3. Clone this repository

```sh
git clone https://github.com/aws-samples/aws-ai-intelligent-document-processing idp_workshop
cd idp_workshop/industry/insurance
```

4. Open the [01-document-classification.ipynb](./01-document-classification.ipynb) notebook and follow instructions in the notebook for Document Classification with Amazon Comprehend custom classification.

5. Open the [02-document-extraction.ipynb](./02-document-extraction-1.ipynb) notebook and follow instructions in the notebook for Document Extraction with Amazon Textract.
   
6. Open the [03-document-extraction-2.ipynb](./03-document-extraction-2.ipynb) notebook and follow instructions in the notebook for Document Extraction with Amazon Comprehend custom entity recognizer.

7. Open the [04-document-enrichment.ipynb](./04-document-enrichment.ipynb) notebook and follow instructions in the notebook for Document enrichment techniques with Amazon Comprehend Medical entity detection and Amazon Comprehend PII entity recognition.

## Clean Up

1. Follow instructions in the notebook to cleanup the resources.
2. If you created an Amazon SageMaker Studio Domain manually then please [delete it](https://docs.aws.amazon.com/sagemaker/latest/dg/gs-studio-delete-domain.html) to avoid incurring charges.
   
---
## Security
See [SageMaker Developer Guide](https://github.com/awsdocs/amazon-sagemaker-developer-guide/blob/master/doc_source/security_iam_id-based-policy-examples.md) for more information on IAM Policy Best Practices and to follow our guidelines and recommendations

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
