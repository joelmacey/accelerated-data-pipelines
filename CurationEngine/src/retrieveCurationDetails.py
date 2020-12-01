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

    athenaOutputBucket = item['outputDetails']['athenaOutputBucket'] \
        if 'athenaOutputBucket' in item['outputDetails'] \
        else None
    athenaOutputFolderPath = item['outputDetails']['athenaOutputFolderPath'] \
        if 'athenaOutputFolderPath' in item['outputDetails'] \
        else None

    outputFilename = item['outputDetails']['filename'] \
        if 'filename' in item['outputDetails'] \
        else None
    
    outputFolderPath = item['outputDetails']['outputFolderPath'] \
        if 'outputFolderPath' in item['outputDetails'] \
        else None

    if 'deleteMetadataFile' in item['outputDetails'] and item['outputDetails']['deleteMetadataFile'] == True:
        deleteMetadataFileBool = True    
    else:
        deleteMetadataFileBool = False

    if 'includeTimestampInFilename' in item['outputDetails'] and item['outputDetails']['includeTimestampInFilename'] == True:
        includeTimestampInFilenameBool = True    
    else:
        includeTimestampInFilenameBool = False

    if 'deleteAthenaQueryFile' in item['outputDetails'] and item['outputDetails']['deleteAthenaQueryFile'] == True:
        deleteAthenaQueryFile = True   
    else:
        deleteAthenaQueryFile = False    
    
    event.update({'scriptFilePath': item['sqlFilePath']})
    event.update({'glueDetails': item['glueDetails']})
    event.update({'outputFilename': outputFilename})
    event.update({'deleteMetadataFileBool': deleteMetadataFileBool})
    event.update({'deleteAthenaQueryFile': deleteAthenaQueryFile})
    event.update({'athenaOutputBucket': athenaOutputBucket})
    event.update({'athenaOutputFolderPath': athenaOutputFolderPath})
    event.update({'includeTimestampInFilenameBool': includeTimestampInFilenameBool})
    event.update({'outputBucket': item['outputDetails']['outputBucket']})
    event.update({'outputFolderPath': outputFolderPath})
    event.update({'requiredMetadata': item['outputDetails']['metadata']})
    event.update({'requiredTags': item['outputDetails']['tags']})
    
    code_commit_res = get_code_commit_file(event['settings']['scriptsRepo'], event['scriptFilePath'])
    event.update({'scriptFileCommitId':code_commit_res['commitId']})