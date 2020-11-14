import traceback

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
	get_file_settings Retrieves the settings for the new file in the
	data lake.
	:param event: AWS Lambda uses this to pass in event data.
	:type event: Python type - Dict / list / int / string / float / None
	:param context: AWS Lambda uses this to pass in runtime information.
	:type context: LambdaContext
	:return: The event object passed into the method
	:rtype: Python type - Dict / list / int / string / float / None
	"""
	ddb_deserializer = StreamTypeDeserializer()
	records = event['Records']
	for record in records:
		ddb = record['dynamodb']
		# Get the event type
		event_name = record['eventName'].upper()  # INSERT, MODIFY, REMOVE
		doc_fields = ddb_deserializer.deserialize({'M': ddb['NewImage']})
		if (event_name == 'INSERT'):
			print(doc_fields['curationType'])
			put_rule(doc_fields['curationType'], doc_fields['cronExpression'])
			
		# elif (event_name == 'MODIFY'):


	return event
