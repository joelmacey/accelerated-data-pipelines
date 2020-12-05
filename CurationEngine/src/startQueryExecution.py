import traceback

import boto3

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

def start_query_execution(event, context):
    """
    start_query_execution Retrieves the sql query from code commit and 
    the details from the event to execute the query on athena.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    """
    sql_query = get_code_commit_file(event['settings']['scriptsRepo'], event['scriptFilePath'])
    
    curation_bucket = event['outputDetails']['outputBucket']
    output_location = f's3://{curation_bucket}/'
    if 'outputFolderPath' in event['outputDetails'] and event['outputDetails']['outputFolderPath'] != None:
        curation_path = event['outputDetails']['outputFolderPath']
        output_location = f's3://{curation_bucket}/{curation_path}'     
        
    # Overwrite default bucket and path if athena details are set
    if event['athenaDetails']['athenaOutputBucket'] != None:
        curation_bucket = event['athenaDetails']['athenaOutputBucket']
        output_location = f's3://{curation_bucket}/'
    if event['athenaDetails']['athenaOutputFolderPath'] != None:
        curation_path = event['athenaDetails']['athenaOutputFolderPath']    
        output_location = f's3://{curation_bucket}/{curation_path}'     
        
    query_execution_id = start_athena_query(sql_query, event['glueDetails']['database'], output_location)
    
    queryDetails = {}
    queryDetails['queryExecutionId'] = query_execution_id
    event.update({'queryDetails': queryDetails})
    
    return event
    
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
    
def get_code_commit_file(repo, filePath):
 
    client = boto3.client('codecommit')

    response = client.get_file(
        repositoryName=repo,
        filePath=filePath
    )
    
    return response['fileContent'].decode('utf-8')