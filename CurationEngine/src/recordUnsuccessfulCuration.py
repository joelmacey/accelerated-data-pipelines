import time
import traceback
import json
import boto3
import os

class RecordUnsuccessfulCurationException(Exception):
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
    record_unsuccessfull_curation Records the unsuccessful curation 
    in the curation history table and sends an SNS notification.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    """
    record_unsuccessful_curation_in_curation_history(event, context)
    send_unsuccessful_curation_sns(event, context)
    
    return event

def record_unsuccessful_curation_in_curation_history(event, context):
    '''
    record_unsuccessful_curation_in_curation_history Records the unsuccessful 
    curation in the curation history table.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''
    
    dynamodb = boto3.resource('dynamodb')

    try:      
        curationType = event['curationDetails']['curationType']
        curation_execution_name = event['curationDetails']['curationExecutionName']
        error = event['error-info']['Error']
        error_cause = json.loads(event['error-info']['Cause'])
        curation_history_table = event["settings"]["curationHistoryTableName"]
        
        if 'stackTrace' in error_cause:
             del error_cause['stackTrace']
        
        dynamodb_item = {
            'curationType': curationType,
            'timestamp': int(time.time() * 1000),
            'curationExecutionName': curation_execution_name,
            'error': error,
            'errorCause': error_cause
        }
        if 'scriptFileCommitId' in event:
            dynamodb_item['scriptFileCommitId'] = event['scriptFileCommitId']
        if 'queryOutputLocation' in event['queryDetails']:
            dynamodb_item['curationKey'] = event['queryDetails']['queryOutputLocation']
        if 'queryExecutionId' in event['queryDetails']:
            dynamodb_item['athenaQueryExecutionId'] = event['queryDetails']['queryExecutionId']
        if 'curationLocation' in event['curationDetails']:
            dynamodb_item['curationOutputLocation'] = event['curationDetails']['curationLocation']
        
        dynamodb_table = dynamodb.Table(curation_history_table)
        dynamodb_table.put_item(Item=dynamodb_item)

    except Exception as e:
        traceback.print_exc()
        raise RecordUnsuccessfulCurationException(e)

def send_unsuccessful_curation_sns(event, context):
    '''
    send_unsuccessful_curation_sns Sends an SNS notifying subscribers
    that curation has failed.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''
    curationType = event['curationDetails']['curationType']
    error = event['error-info']['Error']
    error_cause = json.loads(event['error-info']['Cause'])
        
    subject = f'Data Pipeline - curation for {curationType} has failed'
    message = f'The curation for {curationType} has failed due to {error} with detail:\n{error_cause}'

    if 'SNS_FAILURE_ARN' in os.environ:
        failureSNSTopicARN = os.environ['SNS_FAILURE_ARN']
        send_sns(failureSNSTopicARN, subject, message)

def send_sns(topic_arn, subject, message):
    '''
    send_sns Sends an SNS with the given subject and message to the
    specified ARN.
    :param topic_arn: The SNS ARN to send the notification to
    :type topic_arn: Python String
    :param subject: The subject of the SNS notification
    :type subject: Python String
    :param message: The SNS notification message
    :type message: Python String
    '''
    client = boto3.client('sns')

    client.publish(TopicArn=topic_arn, Subject=subject, Message=message)