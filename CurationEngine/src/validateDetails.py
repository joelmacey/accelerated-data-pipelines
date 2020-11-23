import traceback

import boto3

class ValidateDetailsException(Exception):
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
    :raises ValidateDetailsException: On any error or exception
    '''
    try:
        return validate_details(event, context)
    except ValidateDetailsException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise ValidateDetailsException(e)

def validate_details(event, context):
    """
    validate_details Validates that the code commit file exists, 
    and the database and table exists.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    """

    get_code_commit_file(event['settings']['scriptsRepo'], event['scriptFilePath'])

    does_database_exist(event['glueDetails']['database'])

    does_table_exist(event['glueDetails']['database'], event['glueDetails']['table'])

    return event

def get_code_commit_file(repo, filePath):
 
    client = boto3.client('codecommit')
    response = client.get_file(
        repositoryName=repo,
        filePath=filePath
    )

def does_database_exist(database):
    
    client = boto3.client('glue')
    
    response = client.get_database(
        Name=database
    )

def does_table_exist(database, table):
    
    client = boto3.client('glue')
    
    response = client.get_table(
        DatabaseName=database,
        Name=table
    )