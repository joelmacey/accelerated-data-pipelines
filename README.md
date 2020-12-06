# Accelerated Data Pipelines

The Accelerated Data Pipelines (ADP) solution takes the next step in your data journey...data dev-ops. Enabling organisations to quickly configure and automate SQL queries to run against their data available within their AWS Glue catalog.

![High Level Architecture](Resources/High_Level.png)
---
![High Level Architecture Explanation](Resources/High_Level_Explanation.png)

# Table of Contents
- [Explanation](#explanation)
    - [Origin](#origin) 
    - [Purpose](#purpose) 
    - [Benefit](#benefit) 
    - [Infrastructure](#infrastructure) 
- [Installation](#installation)
    - [1. Provisioning the Storage Structure](#1-provisioning-the-storage-structure) 
    - [2. Provisioning the Curation Engine](#2-provisioning-the-curation-engine) 
    - [3. Provisioning the Visualisation Lambdas](#2-provisioning-the-visualisation-lambdas)
- [Usage](#usage)
    - [Configure a curation script](#configure-a-curation-script)
    - [Configure the sample data source](#configure-the-sample-data-source)
    - [Initialise and use Elasticsearch / Kibana](#initialise-and-use-elasticsearch--kibana)
- [Additional Resources](#additonal-resources)
    - [Curation Details DyanmoDB File Explanation][#curation-details-dyanmodb-file-xxplanation]
    - [Architecture](#rrchitecture)
- [License](#license)

# Explanation
## Origin

From talking with customers who use the accelerated data lake as the foundation for their data lake, once they are able to query the data within their staged data via Athena their eyes light up with the possibilities of extracting, joining and reporting using the data now available to them. 

Organisations, now with a wealth of data at their fingertips are able to start creating tangible benefit through the use of this data.
These organisations look for a way to automate the filtering, joining and selecting using their newly acquired data lake. These automations are usually made up of a basic lambda function configured for a specific use case that didn't allow much flexibility or reusability, and the infrastucture to support was held together with duct-tape.

Thus the accelerated data pipelines was created, with the ability to define a curation, the metadata and tags attached to the output file, renaming the output with a custom filename, automatically configuring the event schedule for when it should run, the ability to audit and see history of queries all while using the familiar tools and framework of the Accelerated Data Lake.

## Purpose

The purpose of this package is to extend the [accelerated data lake](https://github.com/aws-samples/accelerated-data-lake) solution to add the functionality to run queies via Amazon Athena on your Glue tables on a scheduled basis.
The platform also keeps an audit trail of when and what curation has ran for debugging purposes
The pipeline allows the user to define everything about the output of the curation from the metadata and tags to be attached to the filename and S3 bucket for it to be placed within.

## Benefit
* Run athena queries on a scheduled basis to a defined output location
* Integrates seamlessly with your existing Glue catalog
* Automatically creates scheduled events in EventBridge
* Record successful and failed curations in the Curation History DynamoDB table
* Track history of sql script changes using CodeCommit
* Error Notification via SNS
* Supports long running queries (>15 minutes aka the lambda timeout)
* Add custom metadata and tags to the output file
* Ability to rename the output file
* Stream events to existing Kibana Dashboard

## Infrastructure:
* 1 CodeCommit repository used for storing sql scripts
* 2 DynamoDB tables
    * To store details about the curation T
    * To store the history of the curation (success and failure and the details used)
* 1 Step Function used to manage and coordinate the pipelines
* 8 Lambda Functions used with the step function to coordinate the running of the query
    * Start the curation step function by provid
    * Retrieve the curation details from the details DynamoDB
    * Validate the details; the sql script in CodeCommit,the output S3 bucket, the Glue database exist
    * Start the query in Athena using the sql script
    * Get the status of the query
    * Update the output details; copy the object to a different S3 bucket, apply metadata, add tags
    * Record a successful curation in the history DynamoDB table and send an SNS notfication
    * Record an unsuccessful curation in the history DynamoDB table and send an SNS notfication
* 1 Lambda Function to stream new entries to event bridge to create the rule and the trigger
* 1 Lambda Function to stream successful and failed events to the elasticsearch cluster

# Installation
These are the steps required to provision the Data pipeline Solution and watch the ingress of data.
* Provision the Required Storage Structure (5 minutes)
* Provision the Curation Engine (5 minutes)
* Provision Visualisation Lambdas (5 minutes)
* Configure a sample curation entry and sample sql (5 minutes)
    * Upload curation script to CodeCommit (5 minutes)
    * Creating Curation Type in CurationDetails DynamoDB Table (5 minutes)
    * Initialise and use Elasticsearch / Kibana (5 minutes)

## 1. Provisioning the Storage Structure
* Go to the CloudFormation section of the AWS Console.
* Think of an environment prefix . This prefix will make your S3 buckets globally unique (so it must be lower case) and wil help identify your components - a good example is: wildrydes-dev-
* Create a new stack using the template `/CurationStorageStructure/curationStorageStructure.yml` 
* Enter the stack name. For example: `wildrydes-dev-curation-structure`
* Enter the environment prefix, in this case: `wildrydes-dev-`
* Add a KMS Key ARN if you want your S3 Buckets encrypted (recommended - also, there are further improvements with other encryption options imminent in this area)
* All other options are self explanatory, and the defaults are acceptable when testing the solution.

## 2. Provisioning the Curation Engine
This is the core engine for the data pipelines - it creates lambdas and a step function, that takes the entry details from a dynamodb table, verifies that it will be able to query the data, updates the output with a filename, tags and metadata.

On both success and failure, the engine will updates the curationHistory table in DynamoDB. Allowing users to see the full history of all the attempted curations and see what output files are and the details used to generate this.

Execution steps:
(ignore these steps if you have AWS SAM already configured)
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

## 3. Provisioning the Visualisation Lambdas
**NOTE** Requires the accelerated data pipelines elasticsearch cluster and its exported value from cloudformation

This step creates a lambda which is triggered by changes to the curation history DynamoDB table. The lambda takes the changes and sends them to the elasticsearch cluster created in the accelerated data lake.

Execution steps:
(ignore these steps if you have AWS SAM already configured)
* Create a IAM user, with CLI access.
* Configure the AWS CLI with the user's access key and secret access key.
* Install AWS SAM.
(mandatory steps)
* Open a terminal / command line and move to the StreamSchedules/ folder
* Execute the AWS SAM package and deploy commands:

For this example, the commands should be:
````
sam package --template-file ./visualisation.yml --output-template-file visualisationDeploy.yml --s3-bucket wildrydes-dev-visualisationcodepackages

sam deploy --template-file visualisationDeploy.yml --stack-name wildrydes-dev-elasticsearch-lambdas --capabilities CAPABILITY_NAMED_IAM --parameter-overrides EnvironmentPrefix=wildrydes-dev-
````

Congratulations! The Accelerated Data Pipeline is now fully provisioned! Now let's configure a curation detail and some sample query.

# Usage
---

**NOTE**
These next steps assume you have a glue database (wildrydes) and table (rydebookings) created from the accelerated data lake example, feel free to update these values with your own database and table in the `DataSources/rydebooking-curation.sql` and `ddbCurationDetailsConfig.json` files.

---

## Configure a curation script
Execution steps:
* Open the file `DataSources/rydebooking-curation.sql`
* Copy the file's contents to the clipboard.
* Go into the AWS Console, CodeCommit.
* Open the CodeCommit repository, which for the environment prefix used in this demonstration will be: `wildrydes-dev-curation-scripts`
* Click `Add File` then `Create File`
* Paste in the contents of the `DataSources/rydebooking-curation.sql` file into the top text box
* Name the file `wildrydes/rydebooking-curation.sql`
* Fill in your name 
* Fill in your email address
* Click `Commit Changes`

You have now uploaded your curation script to the code commit repository

## Configure the sample data source
Execution steps:
* Open the file `DataSources/ddbCurationDetailsConfig.json`
* Copy the file's contents to the clipboard.
* Go into the AWS Console, DynamoDB screen.
* Open the DataSource table, which for the environment prefix used in this demonstration will be: `wildrydes-dev-curationDetails`
* Go to the Items tab, click Create Item, switch to 'Text' view and paste in the contents of the `ddbCurationDetailsConfig.json` file. 
* Save the item.
 
You now have a fully configured your curationType.

## Initialise and use Elasticsearch / Kibana
The above steps will have resulted in the rydebookings curation being entered into the DynamoDB curation history table (`wildrydes-dev-curationHistory`). The visualisation steps subscribed to these table's stream and all updates are now sent to elasticsearch.

The data will already be in elasticsearch, we just need to create a suitable index.

Execution steps:

* Go to the kibana url (found in the AWS Console, under elasticsearch)
* You will see there is no data - this is because the index needs to be created (the data is present, so we will let kibana auto-create it)
* Click on the management tab, on the left.
* Click "Index Patterns"
* Paste in: `wildrydes-dev-curationHistory` (so <ENVIRONMENT_PREFIX>curationHistory). You will see this name in the available index patterns at the base of the screen.
* Click "Next step"
* Select `@Timestamp` in the "Time Filter field name" field - this is very important, otherwise you will not get the excellent kibana timeline.
* Click "Create Index Pattern" and the index will be created. Click on the Discover tab to see your data catalog and details of your failed and successful ingress.


# Additional Resources

* https://github.com/aws-samples/accelerated-data-lake

## Curation Details DyanmoDB File Explanation 
Also available [here](DataSources/curationDetailExplanation.json)
```
{
"curationType": "The unique key used to identify the curation (REQUIRED)",
"sqlFilePath": "The file path within the curation scripts CodeCommit repository (REQUIRED)",
"cronExpression": "The cron expression that will be added as an eventbridge rule as to when to trigger this curation (REQUIRED)",
"glueDetails": {
    "database": "The glue database of the data that the query will run within (REQUIRED)",
    "tables": [
    "The glue tables that the query references, the validation step will validate that these table exists, will accept tables within the above database, as well as tables external as long as they are in the database.table format (optional)"
    ]
},
"athenaDetails": {
    "athenaOutputBucket": "If you would like the file to be placed in a bucket before it is moved to a final location, specify it here (optional)",
    "athenaOutputFolderPath": "specify the folder path that you would like the athena query to use (optional)",
    "deleteAthenaQueryFile": "If you would like the curaiton engine to remove the inital query result after it has been moved to the final location, default is True (optional)",
    "deleteMetadataFile": "If you would like the engine to delete the .metadata file that is created along with the query, default is true (optional)"
},
"outputDetails": {
    "outputBucket": "The final output bucket location, the results will either be written here directly by athena, or be copied from the athenaDetails location (REQUIRED)",
    "outputFolderPath": "The folder you would like the final file to be placed within (optional)",
    "filename": "The filename you would like to use instead of the query id (optional)",
    "includeTimestampInFilename": "If you would like to include a timestamp of when the file was created in order to differentiate between runs, requires filename (optional)",
    "metadata": "metadata that you would like to attach to the final output file  (optional)",
    "tags": "Tags that you would like to attach to the final output file (optional)"
}
}
```
## Architecture
![Architecture Diagram](Resources/Architecture.png)

# License
This library is licensed under the Apache 2.0 License. 

