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
    sql_query = get_code_commit_file(event['settings']['scriptsRepo'], event['scriptFilePath'])
    
    curation_bucket = event['settings']['curationBucket']
    curation_path = event['outputFolderPath'] #test-curation/
    output_location = f's3://{curation_bucket}/{curation_path}' 
    print(output_location)
    query_execution_id = start_athena_query(sql_query, event['glueDetails']['database'], output_location)

    event.update({'queryExecutionId': query_execution_id})
    
    return event

def get_code_commit_file(repo, filePath):
 
    client = boto3.client('codecommit')

    response = client.get_file(
        repositoryName=repo,
        filePath=filePath
    )
    return response['fileContent'].decode('utf-8')
