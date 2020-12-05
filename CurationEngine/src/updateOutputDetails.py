import traceback

import boto3

class UpdateOutputDetailsException(Exception):
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
	update_output_details Once the query is successful, update 
	with additional details such as tags, metadata and defined filename.
	:param event: AWS Lambda uses this to pass in event data.
	:type event: Python type - Dict / list / int / string / float / None
	:param context: AWS Lambda uses this to pass in runtime information.
	:type context: LambdaContext
	:return: The event object passed into the method
	:rtype: Python type - Dict / list / int / string / float / None
	"""
	queryOutputKey = get_existing_path(event['queryDetails']['queryOutputLocation'])
	queryOutputBucket = get_bucket(event['queryDetails']['queryOutputLocation'])

	# Delete the metadata file that is created	
	if event['athenaDetails']['deleteMetadataFileBool'] == True:
		delete_object(f'{queryOutputKey}.metadata', queryOutputBucket)

	new_key = queryOutputKey # Defaults to the key
	new_bucket = event['outputDetails']['outputBucket']
	filename = new_key.split('/')[-1]
	# If there is a filename specified, use this
	if event['outputDetails']['outputFilename'] != None:
		filename = event['outputDetails']['outputFilename']
		
		# If the filename should include a timestamp update the filename to include
		if event['outputDetails']['includeTimestampInFilenameBool'] == True:
			timestamp = event['curationDetails']['curationTimestamp']
			filename = f'{filename}{timestamp}'
			
	#If there is a defined path, use this, and update the key
	if event['outputDetails']['outputFolderPath'] != None:
		path = ('/').join(event['outputDetails']['outputFolderPath'].split('/'))
		new_key = f'{path}/{filename}.csv'
	elif (len(queryOutputKey.split('/')) > 1):
		path = ('/').join(queryOutputKey.split('/')[:-1])
		new_key = f'{path}/{filename}.csv'
	else:
		new_key = f'{filename}.csv'

	curationDetails = event['curationDetails']
	curationDetails['curationLocation'] = f's3://{new_bucket}/{new_key}'
	
	event.update({'curationDetails': curationDetails})
	
	metadata = event['outputDetails']['metadata']
	if metadata != None:
		# Copy the file into the new location and apply metadata
		copy_and_update_metadata_on_object(queryOutputBucket, queryOutputKey, new_bucket, new_key, metadata)
	elif (queryOutputKey != new_key):
		# Don't copy with metadata, just update with new filename
		copy_object(queryOutputBucket, queryOutputKey, new_bucket, new_key)
	else:
		copy_object(queryOutputBucket, queryOutputKey, new_bucket, new_key)
	# Only delete the file as long as its not the same file
	if (event['athenaDetails']['deleteAthenaQueryFile'] == True and queryOutputKey != new_key):
		delete_object(queryOutputKey, queryOutputBucket)
	
	# Generate the tag list.
	tagList = []
	if event['outputDetails']['tags'] != None:
		for tagKey in event['outputDetails']['tags']:
			tag = {'Key': tagKey, 'Value': event['outputDetails']['tags'][tagKey]}
			tagList.append(tag)
	
		# Apply the tag list.
		put_tags_on_object(new_bucket, new_key, tagList)

	return event

def copy_and_update_metadata_on_object(bucket, key, new_bucket, new_key, metadata):
	client = boto3.client('s3')
	
	copy_source = {'Bucket': bucket, 'Key': key}

	client.copy(
		copy_source,
		new_bucket,
		new_key,
		ExtraArgs={"Metadata": metadata, "MetadataDirective": "REPLACE"})

def copy_object(bucket, key, new_bucket, new_key):
	client = boto3.client('s3')
	
	copy_source = {'Bucket': bucket, 'Key': key}

	client.copy(
		copy_source,
		new_bucket,
		new_key
	)
		
def put_tags_on_object(bucket, key, tagList):
	client = boto3.client('s3')

	client.put_object_tagging(
		Bucket=bucket,
		Key=key,
		Tagging={'TagSet': tagList})

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