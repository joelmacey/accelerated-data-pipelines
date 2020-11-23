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

class StartCurationProcessingException(Exception):
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
    start_curation_processing Passes the event and additional 
    details defined to start off the curation engine.
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
    start_step_function_for_file Starts the accelerated 
    data pipelines curation engine step function for this curationType.
    :param curationType:  The unique Id of the curation defined in the curaiton details dynamodb table
    :type curationType: Python String
    '''
    try:
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        keystring = re.sub('\W+', '_', curationType)  # Remove special chars
        step_function_name = timestamp + id_generator() + '_' + keystring

        sfn = boto3.client('stepfunctions')
        
        state_machine_arn = os.environ['STEP_FUNCTION']

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
                'scriptsRepo':
                    os.environ['SCRIPTS_REPO_NAME']
            }
            
        }

        step_function_input = json.dumps(sfn_Input)
        sfn.start_execution(
            stateMachineArn=state_machine_arn,
            name=step_function_name, input=step_function_input)

        print(f'Started step function with input:{step_function_input}')

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
    curation engine step function in the curation history. Any exceptions
    raised by this method are caught.
    :param curationType:  The curation Type that is being ran
    :type curationType: Python String
    :param exception: The exception raised by the failure
    :type exception: Python Exception
    '''
    try:
        dynamodb = boto3.resource('dynamodb')
        
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