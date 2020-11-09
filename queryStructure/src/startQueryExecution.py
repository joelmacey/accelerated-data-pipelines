import traceback

import boto3
import json

class StartQueryExecutionException(Exception):
    pass


def lambda_handler(event, context):
    '''
    lambda_handler Top level lambda handler ensuring all exceptions
    are caught and logged.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    :raises StartQueryExecutionException: On any error or exception
    '''
    try:
        return start_query_execution(event, context)
    except StartQueryExecutionException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise StartQueryExecutionException(e)

def start_athena_query(query_string, database, output_location):
    athena = boto3.client('athena')

    response = athena.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )
    return response['QueryExecutionId']
def start_query_execution(event, context):
    """
    get_file_settings Retrieves the settings for the new file in the
    data lake.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    """
    sql_query = retrieve_sql_query(event['settings']['scriptsBucket'], event['scriptFilePath'])
    
    curation_bucket = event['settings']['curationBucket']
    curation_path = event['outputFolderPath']
    output_location = f's3://{curation_bucket}/{curation_path}' 
    print(output_location)
    query_execution_id = start_athena_query(sql_query, event['glueDetails']['database'], output_location)

    event.update({'queryExecutionId': query_execution_id})
    
    return event

def retrieve_sql_query(bucket, key):
    s3 = boto3.client('s3')
    
    #Create a file object using the bucket and object key. 
    fileobj = s3.get_object(
        Bucket=bucket,
        Key=key
    )
        
    # open the file object and read it into the variable filedata. 
    filedata = fileobj['Body'].read()
    
    #file data will be a binary stream.  We have to decode it 
    contents = filedata.decode('utf-8')
    return contents

event = json.loads('''{
  "curationDetails": {
    "curationType": "test-curation",
    "stagingExecutionName": "202011091201068375396YMD6Y_test_curation"
  },
  "settings": {
    "curationDetailsTableName": "testcurationDetails",
    "curationHistoryTableName": "testcurationHistory",
    "curationBucket": "tdfv1wadatahub-test-curation",
    "scriptsBucket": "tdfv1wadatahub-test-scripts",
    "failedCurationBucket": "tdfv1wadatahub-test-failed-curation"
  },
  "scriptFilePath": "test/test.sql",
  "glueDetails": {
    "database": "wadpc-db",
    "table": "tdfv5_wadatahub"
  },
  "outputFilename": "test",
  "outputFolderPath": "test-curation/",
  "requiredMetadata": {
    "sourcesystem": "DataLake Demonstration",
    "creator": "test",
    "quality": "HIGH"
  },
  "requiredTags": {
    "pii": "FALSE",
    "dataSource": "ABS",
    "dataOwner": "ABS"
  }
}''')
context = ''
lambda_handler(event, context)