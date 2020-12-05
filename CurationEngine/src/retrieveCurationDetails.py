import traceback

import boto3

class RetrieveCurationDetailsException(Exception):
    pass

def get_code_commit_file(repo, filePath):
 
    client = boto3.client('codecommit')

    response = client.get_file(
        repositoryName=repo,
        filePath=filePath
    )
    return response

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
    :raises RetrieveCurationDetailsException: On any error or exception
    '''
    try:
        return get_curation_details(event, context)
    except RetrieveCurationDetailsException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise RetrieveCurationDetailsException(e)

def get_curation_details(event, context):
    """
    get_file_settings Retrieves the curation details from the 
    curation details dynamodb table and attaches them to the event
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    """
    attach_file_settings_to_event(event, context)
    return event

def attach_file_settings_to_event(event, context):
    '''
    attach_file_settings_to_event Attach the configured file settings
    to the lambda event.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''
    
    dynamodb = boto3.resource('dynamodb')

    table = event["settings"]["curationDetailsTableName"]
    ddb_table = dynamodb.Table(table)
    # Get the item. There can only be one or zero - it is the table's
    # partition key - but use strong consistency so we respond instantly
    # to any change. This can be revisited if we want to conserve RCUs
    # by, say, caching this value and updating it every minute.
    response = ddb_table.get_item(
        Key={'curationType': event['curationDetails']['curationType']}, ConsistentRead=True)
    item = response['Item']

    # Retrieve all the details around Athena
    athenaDetails = {}
    athenaDetails['athenaOutputBucket'] = item['athenaDetails']['athenaOutputBucket'] \
        if 'athenaOutputBucket' in item['athenaDetails'] \
        else None
    athenaDetails['athenaOutputFolderPath'] = item['athenaDetails']['athenaOutputFolderPath'] \
        if 'athenaOutputFolderPath' in item['athenaDetails'] \
        else None
    if 'deleteAthenaQueryFile' in item['athenaDetails'] and item['athenaDetails']['deleteAthenaQueryFile'] == True:
        athenaDetails['deleteAthenaQueryFile'] = True   
    else:
        athenaDetails['deleteAthenaQueryFile'] = False 
    
    if 'deleteMetadataFile' in item['athenaDetails'] and item['athenaDetails']['deleteMetadataFile'] == True:
        athenaDetails['deleteMetadataFileBool'] = True    
    else:
        athenaDetails['deleteMetadataFileBool'] = False   
    
    # Retrieve all the details around the output of the file
    outputDetails = {}
    outputDetails['outputFilename'] = item['outputDetails']['filename'] \
        if 'filename' in item['outputDetails'] \
        else None
    
    outputDetails['outputFolderPath'] = item['outputDetails']['outputFolderPath'] \
        if 'outputFolderPath' in item['outputDetails'] \
        else None

    if 'includeTimestampInFilename' in item['outputDetails'] and item['outputDetails']['includeTimestampInFilename'] == True:
        outputDetails['includeTimestampInFilenameBool'] = True    
    else:
        outputDetails['includeTimestampInFilenameBool'] = False
    
    outputDetails['metadata'] = item['outputDetails']['metadata'] \
        if 'metadata' in item['outputDetails'] \
        else None
        
    outputDetails['tags'] = item['outputDetails']['tags'] \
        if 'tags' in item['outputDetails'] \
        else None
        
    outputDetails['outputBucket'] = item['outputDetails']['outputBucket']
    
    event.update({'scriptFilePath': item['sqlFilePath']})
    event.update({'glueDetails': item['glueDetails']})
    event.update({'athenaDetails': athenaDetails})
    event.update({'outputDetails': outputDetails})
    
    code_commit_res = get_code_commit_file(event['settings']['scriptsRepo'], event['scriptFilePath'])
    event.update({'scriptFileCommitId':code_commit_res['commitId']})