# Intelligent Document Processing with AWS AI Services

[![Launch](https://img.shields.io/badge/Launch%20with%20CodeCatalyst-%F0%9F%9A%80-8A2BE2)](https://codecatalyst.aws/launch?options=%7B%22sourceRepository%22%3A%22https%3A%2F%2Fgithub.com%2Fvprince1%2Faws-ai-intelligent-document-processing%22%2C%22options%22%3A%5B%5B%22region%22%2C%22us-west-2%22%5D%5D%7D)
[![Latest Version](https://img.shields.io/github/tag/aws-samples/aws-ai-intelligent-document-processing)](https://github.com/aws-samples/aws-ai-intelligent-document-processing/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/aws-samples/amazon-eks-arch-apache-solr/blob/main/LICENSE)

This repository is part of [Intelligent Document Processing with AWS AI Services](https://catalog.us-east-1.prod.workshops.aws/workshops/c2af04b2-54ab-4b3d-be73-c7dd39074b20) workshop.

Documents contain valuable information and come in various shapes and forms. In most cases, you are manually processing these documents which is time consuming, prone to error, and expensive. Not only do you want this information extracted quickly but you also want to automate business processes that presently rely on manual inputs and intervention across various file types and formats.

To help you overcome these challenges, AWS Machine Learning (ML) now provides you choices when it comes to extracting information from complex content in any document format such as insurance claims, mortgages, healthcare claims, contracts, and legal contracts. 

## Different phases of Intelligent Document Processing pipeline

<p align="center">
  <img src="./images/idp-phases.png" alt="cfn1"/>
</p>

In this workshop, we will deep-dive into each of these phases of the IDP Pipeline with solutions to automate each step.
We have hands-on labs to familiarize yourself with AWS AI services ( Amazon Textract, Amazon Comprehend) to build your solution

## Getting Started

In order to be able to execute all the Jupyter Notebooks in this sample, we will first need to create a SageMaker Studio domain. The CloudFormation template to create the SageMaker Studio domain and all the related resources, such as IAM Roles, S3 Bucket etc. is included under the `/dist` directory. Follow the steps below to create the CloudFormation stack using the `idp-deploy.yaml` file.

> :warning: Your AWS account **must have a default VPC** for this CloudFormation template to work.
> Your AWS account may incur some nominal charges for SageMaker Studio domain, Amazon Textract, and Amazon Comprehend. However, Amazon Textract, Comprehend, and SageMaker are free to try as part of [AWS Free Tier](https://aws.amazon.com/free/).

* Navigate to AWS Console
* Search for CloudFormation in the "Services" search bar
* Once in the CloudFormation console, click on the "Create Stack" button (use the "With new resources option")
* In the "Create Stack" wizard, chose "Template is ready", then select "Upload a template file"
<p align="center">
  <img src="./images/cfn1.png" alt="cfn1"/>
</p>

* Upload the [provided](./dist/idp-deploy.yaml) `yaml` file, click "Next"
* In the "Specify stack details" screen, enter "Stack name". Click "Next"
<p align="center">
  <img src="./images/cfn2.png" alt="cfn2"/>
</p>

* In the "Configure Stack options" screen, leave the configurations as-is. Click "Next"
* In the "Review" screen, scroll down to the bottom of the page to the "Capabilities" section and acknowledge the notice that the stack is going to create required IAM Roles by checking the check box. Click "Create stack".
<p align="center">
  <img src="./images/cfn3.png" alt="cfn3"/>
</p>

The stack creation can take upto 30 minutes. Once your SageMaker domain is created, you can navigate to the SageMaker console and click on "Amazon SageMaker Studio" on the left pane of the screen. Choose the default user created "SageMakerUser" and Click on "Launch Studio". This will open the SageMaker Studio IDE in a new browser tab. NOTE: If this is your first time using SageMaker Studio then it may take some time for the IDE to fully launch. 
<p align="center">
  <img src="./images/cfn4.png" alt="cfn4"/>
</p>

## Setup SageMaker Studio

Once the SageMaker Studio IDE has fully loaded in your browser, you can clone this repository into the SageMaker Domain instance and start working on the provided Jupyter Notebooks. To clone this repository-

* On the SageMaker Studio IDE, click on "File menu > New > Terminal". This will open a terminal window within SageMaker Studio.
<p align="center">
  <img src="./images/sm1.png" alt="sm1"/>
</p>

* By default, the terminal launches at the root of the SageMaker Studio IDE workspace.
* Next, clone this repository using 

```  
git clone https://github.com/aws-samples/aws-ai-intelligent-document-processing idp_workshop
```

* Once the repository is cloned, a direcotry named `idp_workshop` will appear in the "File Browser" on the left panel of SageMaker Studio IDE
* You can now access the Jupyter Notebooks inside the directory and start working on them.

You're all set to begin the workshop!

## License

This library is licensed under the MIT-0 License. See the LICENSE file.



