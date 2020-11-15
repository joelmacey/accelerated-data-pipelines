# Add tags, metadata, and filename (if applicable)

import traceback

import boto3
import botocore

s3 = boto3.client('s3')

class UpdateOutputDetailsException(Exception):
	pass

def delete_object(key, bucket):
	client = boto3.client('s3')
	response = client.delete_object(
		Bucket=bucket,
		Key=key
	)

def get_bucket(s3_path):
	bucket = s3_path.split('/')[2]
	return bucket

def get_existing_path(s3_path):
	folders = s3_path.split('/')[3:]
	folders ="/".join(folders)
	return folders	

def update_filename(bucket, key, filename):    
	path = ('/').join(key.split('/')[:-1]) 
	filename = f'{filename}.csv'
	s3 = boto3.resource('s3')
	s3.Object(bucket,f'{path}/{filename}').copy_from(CopySource=f'{bucket}/{key}')
	s3.Object(bucket,key).delete()
	
	return f'{path}/{filename}'

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
	:raises UpdateOutputDetailsException: On any error or exception
	'''
	try:
		return update_output_details(event, context)
	except UpdateOutputDetailsException:
		raise
	except Exception as e:
		traceback.print_exc()
		raise UpdateOutputDetailsException(e)


def update_output_details(event, context):
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
	queryOutputKey = get_existing_path(event['queryOutputLocation'])
	queryOutputBucket = get_bucket(event['queryOutputLocation'])
		
	if event['deleteMetadataFileBool'] == True:
		delete_object(f'{queryOutputKey}.metadata', queryOutputBucket)
	
	if event['outputFilename'] != None:
		filename = event['outputFilename']
		if event['includeTimestampInFilenameBool'] == True:
			filename = f'{filename}-{event['curationDetails']['curationTimestamp']}'
		queryOutputKey = update_filename(queryOutputBucket, queryOutputKey, filename)
		event.update({"queryOutputLocation": queryOutputKey})
	
	metadata = event['requiredMetadata']

	# Copy the file into location in order to apply metadata
	copy_source = {'Bucket': queryOutputBucket, 'Key': queryOutputKey}
	s3.copy(
		copy_source,
		queryOutputBucket,
		queryOutputKey,
		ExtraArgs={"Metadata": metadata, "MetadataDirective": "REPLACE"})
	
	# Generate the tag list.
	tagList = []
	for tagKey in event['requiredTags']:
		tag = {'Key': tagKey, 'Value': event['requiredTags'][tagKey]}
		tagList.append(tag)

	# Apply the tag list.
	s3.put_object_tagging(
		Bucket=queryOutputBucket,
		Key=queryOutputKey,
		Tagging={'TagSet': tagList})

	return event
