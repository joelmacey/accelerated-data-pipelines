# Record a failed curation in history table
import time
import traceback
import json
import boto3


class RecordUnsuccessfulCurationException(Exception):
    pass


sns_client = boto3.client('sns')
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
    :raises RecordUnsuccessfulCurationException: On any error or exception
    '''
    try:
        return record_unsuccessfull_curation(event, context)
    except RecordUnsuccessfulCurationException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise RecordUnsuccessfulCurationException(e)


def record_unsuccessfull_curation(event, context):
    """
    record_unsuccessfull_curation Records the successful staging in the data
    catalog and raises an SNS notification.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    """
    record_unsuccessful_curation_in_curation_history(event, context)
    # send_successful_staging_sns(event, context)
    return event


def record_unsuccessful_curation_in_curation_history(event, context):
    '''
    record_unsuccessful_curation_in_curation_history Records the successful staging
    in the data catalog.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''
    try:
        

        curationType = event['curationDetails']['curationType']
        curation_execution_name = event['curationDetails']['curationExecutionName']
        error = event['error-info']['Error']
        error_cause = json.loads(event['error-info']['Cause'])
        curationBucket = event['settings']['curationBucket']
        curation_history_table = event["settings"]["curationHistoryTableName"]
        
        if 'stackTrace' in error_cause:
             del error_cause['stackTrace']
        
        dynamodb_item = {
            'curationType': curationType,
            'curationExecutionName': curation_execution_name,
            'timestamp': int(time.time() * 1000),
            'curationBucket': curationBucket,
            'error': error,
            'errorCause': error_cause
        }

        if 'queryOutputLocation' in event:
            dynamodb_item['curationKey'] = event['queryOutputLocation']
        if 'queryExecutionId' in event:
            dynamodb_item['athenaQueryExecutionId'] = event['queryExecutionId']
        
        dynamodb_table = dynamodb.Table(curation_history_table)
        dynamodb_table.put_item(Item=dynamodb_item)

    except Exception as e:
        traceback.print_exc()
        raise RecordUnsuccessfulCurationException(e)

