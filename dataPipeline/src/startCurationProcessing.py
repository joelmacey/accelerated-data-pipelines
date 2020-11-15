import json
import os
import random
import re
import string
import time
import traceback
import urllib
from datetime import datetime

import boto3
from botocore.exceptions import ClientError


class StartCurationProcessingException(Exception):
    pass


sns = boto3.client('sns')
sfn = boto3.client('stepfunctions')
dynamodb = boto3.resource('dynamodb')
curation_details_table = os.environ['CURATION_DETAILS_TABLE_NAME']
curation_history_table = os.environ['CURATION_HISTORY_TABLE_NAME']
state_machine_arn = os.environ['STEP_FUNCTION']


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
    :raises StartCurationProcessingException: On any error or exception
    '''
    try:
        return start_curation_processing(event, context)
    except StartCurationProcessingException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise StartCurationProcessingException(e)


def start_curation_processing(event, context):
    '''
    start_processing Confirm the lambda context request id is
    not already being processed, check this is not just a folder being 
    created, then start file processing.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    '''

    start_step_function_for_event(event['curationType'])
    
    return event


def start_step_function_for_event(curationType):
    '''
    start_step_function_for_file Starts the data lake staging engine
    step function for this file.
    :param bucket:  The S3 bucket name
    :type bucket: Python String
    :param key: The S3 object key
    :type key: Python String
    '''
    try:
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        keystring = re.sub('\W+', '_', curationType)  # Remove special chars
        step_function_name = timestamp + id_generator() + '_' + keystring

        step_function_name = step_function_name[:80]

        sfn_Input = {
            'curationDetails': {
                'curationType': curationType,
                'curationExecutionName': step_function_name,
                'curationTimestamp': timestamp
            },
            'settings': {
                'curationDetailsTableName':
                    os.environ['CURATION_DETAILS_TABLE_NAME'],
                'curationHistoryTableName':
                    os.environ['CURATION_HISTORY_TABLE_NAME'],
                'curationBucket':
                    os.environ['CURATION_BUCKET_NAME'],
                'scriptsRepo':
                    os.environ['SCRIPTS_REPO_NAME']
            }
            
        }

        # Start step function
        step_function_input = json.dumps(sfn_Input)
        sfn.start_execution(
            stateMachineArn=state_machine_arn,
            name=step_function_name, input=step_function_input)

        print('Started step function with input:{}'
              .format(step_function_input))
    except Exception as e:
            record_failure_to_start_step_function(
                curationType, e)
            raise


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    '''
    id_generator Creates a random id to add to the step function
    name - duplicate names will cause errors.
    :param size: The required length of the id, defaults to 6
    :param size: Python Integer, optional
    :param chars: Chars used to generate id, defaults to uppercase alpha+digits
    :param chars: Python String
    :return: The generated id
    :rtype: Python String
    '''
    return ''.join(random.choice(chars) for _ in range(size))


def record_failure_to_start_step_function(curationType, exception):

    '''
    record_failure_to_start_step_function Record failure to start the
    staging engine step function in the data catalog. Any exceptions
    raised by this method are caught.
    :param curationType:  The curation Type that is being ran
    :type curationType: Python String
    :param exception: The exception raised by the failure
    :type exception: Python Exception
    '''
    try:
        curation_history_table = os.environ['CURATION_HISTORY_TABLE_NAME']

        dynamodb_item = {
            'curationType': curationType,
            'timestamp': int(time.time() * 1000),
            'error': "Failed to start processing",
            'errorCause': {
                'errorType': type(exception).__name__,
                'errorMessage': str(exception)
            }
        }

        dynamodb_table = dynamodb.Table(curation_history_table)
        dynamodb_table.put_item(Item=dynamodb_item)

    except Exception:
        traceback.print_exc()
