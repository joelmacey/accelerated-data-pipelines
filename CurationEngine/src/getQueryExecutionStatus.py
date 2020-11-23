import traceback

import boto3

class GetQueryExecutionStatusException(Exception):
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
    :raises GetQueryExecutionStatusException: On any error or exception
    '''
    try:
        return get_query_execution_status(event, context)
    except GetQueryExecutionStatusException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise GetQueryExecutionStatusException(e)

def get_query_execution_status(event, context):
    """
    get_query_execution_status Retrieves the status of the athena query
    in order to handle the status; Failed, success, or still running.
    :param event: AWS Lambda uses this to pass in event data.
    :type event: Python type - Dict / list / int / string / float / None
    :param context: AWS Lambda uses this to pass in runtime information.
    :type context: LambdaContext
    :return: The event object passed into the method
    :rtype: Python type - Dict / list / int / string / float / None
    """
    status, output_location = get_status(event['queryExecutionId'])
    event.update({'queryStatus': status})
    event.update({'queryOutputLocation': output_location})
   
    return event

def get_status(query_execution_id):
	client = boto3.client('athena')
	
	response = client.get_query_execution(
		QueryExecutionId=query_execution_id
	)
	return response['QueryExecution']['Status']['State'], response['QueryExecution']['ResultConfiguration']['OutputLocation'] 