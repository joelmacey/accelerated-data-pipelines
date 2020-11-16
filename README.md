## Accelerated Data Pipelines

A packaged solution, that builds a highly functional Data Pipeline Framework ready to use with your Data Lake, with the option to push data to your data catalog queryable via Elasticsearch.

## License

This library is licensed under the Apache 2.0 License. 

# Accelerated Data Pipeline installation instructions
These are the steps required to provision the Data pipeline Solution and watch the ingress of data.
* Provision the Required Storage Structure (5 minutes)
* Provision the Curation Engine (5 minutes)
* Provision the Stream Schedules (5 minutes)
* Provision the Visualisation
    * Provision Visualisation Lambdas (5 minutes)
    * Initialise and use Elasticsearch / Kibana (5 minutes)
* Configure a sample curation entry and sample sql (5 minutes)

## 1. Provisioning the Storage Structure
* Go to the CloudFormation section of the AWS Console.
* Think of an environment prefix . This prefix will make your S3 buckets globally unique (so it must be lower case) and wil help identify your components - a good example is: wildrydes-dev-
* Create a new stack using the template `/CurationStorageStructure/curationStorageStructure.yml` 
* Enter the stack name. For example: `wildrydes-dev-curation-structure`
* Enter the environment prefix, in this case: `wildrydes-dev-`
* Add a KMS Key ARN if you want your S3 Buckets encrypted (recommended - also, there are further improvements with other encryption options imminent in this area)
* All other options are self explanatory, and the defaults are acceptable when testing the solution.
* If you would like to use an existing curation or logs bucket, you will have to alter the yml of both the storage structure and the curation engine.

## 2. Provisioning the Curation Engine
This is the core engine for the data pipelines - it creates lambdas and a step function, that takes the entry details from a dynamodb table, verifies that it will be able to query the data, updates the output with a filename, tags and metadata.

On both success and failure, the engine will updates the curationHistory table in DynamoDB. Allowing users to see the full history of all the attempted curations and see what output files are and the details used to generate this.

Execution steps:
(ignore these steps if already done in the visualisation step)
* Create a IAM user, with CLI access.
* Configure the AWS CLI with the user's access key and secret access key.
* Install AWS SAM.
(mandatory steps)
* Open a terminal / command line and move to the CurationEngine/ folder
* Execute the AWS SAM package and deploy commands:

For this example, the commands should be:
````
sam package --template-file ./curationEngine.yml --output-template-file curationEngineDeploy.yml --s3-bucket wildrydes-dev-accelerated-data-pipelines-codepackages

sam deploy --template-file curationEngineDeploy.yml --stack-name wildrydes-dev-curation-engine --capabilities CAPABILITY_NAMED_IAM --parameter-overrides EnvironmentPrefix=wildrydes-dev-
````

## 3. Provisioning the Schedule Stream
This is the schedule stream, any creation or alteration of an entry in the curationDetails dynamodb table will be streamed to a lambda function that add or updates the event rule used in the curation engine using the cron expression defined in the entry.

Execution steps:
(ignore these steps if already done in the visualisation step)
* Create a IAM user, with CLI access.
* Configure the AWS CLI with the user's access key and secret access key.
* Install AWS SAM.
(mandatory steps)
* Open a terminal / command line and move to the StreamSchedules/ folder
* Execute the AWS SAM package and deploy commands:

For this example, the commands should be:
````
sam package --template-file ./scheduleSetup.yml --output-template-file scheduleSetupDeploy.yml --s3-bucket wildrydes-dev-accelerated-data-pipelines-codepackages

sam deploy --template-file scheduleSetupDeploy.yml --stack-name wildrydes-dev-schedule-streams --capabilities CAPABILITY_NAMED_IAM --parameter-overrides EnvironmentPrefix=wildrydes-dev-
````