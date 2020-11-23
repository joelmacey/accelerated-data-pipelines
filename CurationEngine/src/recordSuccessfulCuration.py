import time
import traceback
import os

import boto3

class RecordSuccessfulCurationException(Exception):
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
    :raises RecordSuccessfulCurationException: On any error or exception
    '''
    try:
        return record_successfull_curation(event, context)
    except RecordSuccessfulCurationException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise RecordSuccessfulCurationException(e)

def record_successfull_curation(event, context):
    """
    record_successfull_curation Records the successful curation in the
    curation history table and sends an SNS notification.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    """
    record_successful_curation_in_curation_history(event, context)
    send_successful_curation_sns(event, context)
    
    return event

def record_successful_curation_in_curation_history(event, context):
    '''
    record_successful_curation_in_curation_history Records the successful 
    curation in the curation history table.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''

    dynamodb = boto3.resource('dynamodb')

    try:
        curation_execution_name = event['curationDetails']['curationExecutionName']
        curationType = event['curationDetails']['curationType']
        queryOutputLocation = event['queryOutputLocation']
        queryExecutionId = event['queryExecutionId']
        scriptFileCommitId = event['scriptFileCommitId']
        curationBucket = event['settings']['curationBucket']
        curation_history_table = event["settings"]["curationHistoryTableName"]

        tags = event['requiredTags']
        metadata = event['requiredMetadata']

        dynamodb_item = {
            'curationType': curationType,
            'timestamp': int(time.time() * 1000),
            'athenaQueryExecutionId': queryExecutionId,
            'curationKey': queryOutputLocation,
            'scriptFileCommitId': scriptFileCommitId,
            'curationBucket': curationBucket,
            'curationExecutionName': curation_execution_name,
            'tags': tags,
            'metadata': metadata
        }
        dynamodb_table = dynamodb.Table(curation_history_table)
        dynamodb_table.put_item(Item=dynamodb_item)

    except Exception as e:
        traceback.print_exc()
        raise RecordSuccessfulCurationException(e)

def send_successful_curation_sns(event, context):
    '''
    send_successful_curation_sns Sends an SNS notifying subscribers
    that curation was successful.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    '''
    curationType = event['curationDetails']['curationType']
    queryOutputLocation = event['queryOutputLocation']
        
    subject = f'Data Pipeline - curation for {curationType} success'
    message = f'The output of your curation can be found: {queryOutputLocation}'

    if 'SNS_SUCCESS_ARN' in os.environ:
        successSNSTopicARN = os.environ['SNS_SUCCESS_ARN']
        send_sns(successSNSTopicARN, subject, message)

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