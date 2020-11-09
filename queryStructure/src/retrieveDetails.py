import traceback

import boto3


class RetrieveCurationDetailsException(Exception):
    pass


dynamodb = boto3.resource('dynamodb')


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
    get_file_settings Retrieves the settings for the new file in the
    data lake.
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
    table = event["settings"]["curationDetailsTableName"]
    ddb_table = dynamodb.Table(table)
    # Get the item. There can only be one or zero - it is the table's
    # partition key - but use strong consistency so we respond instantly
    # to any change. This can be revisited if we want to conserve RCUs
    # by, say, caching this value and updating it every minute.
    response = ddb_table.get_item(
        Key={'curationType': event['curationDetails']['curationType']}, ConsistentRead=True)
    item = response['Item']
    output_filename = item['outputDetails']['filename'] \
        if 'filename' in item['outputDetails'] \
        else None
    event.update({'scriptFilePath': item['sqlFilePath']})
    event.update({'glueDetails': item['glueDetails']})
    event.update({'outputFilename': output_filename})
    event.update({'outputFolderPath': item['outputDetails']['outputFolderPath']})
    event.update({'requiredMetadata': item['outputDetails']['metadata']})
    event.update({'requiredTags': item['outputDetails']['tags']})