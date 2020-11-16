import traceback
import os
import json
import boto3
import botocore
from boto3.dynamodb.types import TypeDeserializer

class CreateNewEventRuleException(Exception):
	pass

# Subclass of boto's TypeDeserializer for DynamoDB to adjust
# for DynamoDB Stream format.
class StreamTypeDeserializer(TypeDeserializer):
    def _deserialize_n(self, value):
        return float(value)

    def _deserialize_b(self, value):
        return value  # Already in Base64

def put_rule(curation_type, schedule_expression):
	
	client = boto3.client('events')

	response = client.put_rule(
		Name=f'{curation_type}-event-rule',
		ScheduleExpression=schedule_expression,
		State='ENABLED',
		Description=f'Event rule for curation type {curation_type}'
	)
def delete_rule(curation_type):
	client = boto3.client('events')

	response = client.delete_rule(
    	Name=f'{curation_type}-event-rule',
	)
def put_target(curation_type, function_arn):
	client = boto3.client('events')
	input = {"curationType": curation_type}
	response = client.put_targets(
	    Rule=f'{curation_type}-event-rule',
	    Targets=[
	        {
	            'Id': f'{curation_type}-event-target',
	            'Arn': function_arn,
	            'Input': json.dumps(input)
	        }
	    ]
	)
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
	:raises CreateNewEventRuleException: On any error or exception
	'''
	try:
		return create_new_event_rule(event, context)
	except CreateNewEventRuleException:
		raise
	except Exception as e:
		traceback.print_exc()
		raise CreateNewEventRuleException(e)


def create_new_event_rule(event, context):
	"""
	create_new_event_rule Creates a new event rule and event target in
	event bridge to be used in the accelerated data pipelines.
	:param event: AWS Lambda uses this to pass in event data.
	:type event: Python type - Dict / list / int / string / float / None
	:param context: AWS Lambda uses this to pass in runtime information.
	:type context: LambdaContext
	:return: The event object passed into the method
	:rtype: Python type - Dict / list / int / string / float / None
	"""
	ddb_deserializer = StreamTypeDeserializer()
	records = event['Records']
	start_curation_process_function_arn = os.environ['START_CURATION_PROCESS_FUNCTION_ARN']
	for record in records:
		ddb = record['dynamodb']
		# Get the event type and curation type for the record
		event_name = record['eventName'].upper()  # INSERT, MODIFY, REMOVE
		doc_fields = ddb_deserializer.deserialize({'M': ddb['NewImage']})
		curation_type = doc_fields['curationType']
		
		print(f'Handling for event {event_name} for curationType {curation_type}')
		
		if (event_name == 'INSERT') or (event_name == 'MODIFY'):
			put_rule(curation_type, doc_fields['cronExpression'])
			put_target(curation_type, start_curation_process_function_arn)
		
	return 'Success'
