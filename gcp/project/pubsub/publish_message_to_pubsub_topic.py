#!/usr/bin/env python3

import os
import sys
import argparse
import subprocess
import inspect
import logging
import time
from datetime import date
from xml.dom.minidom import parse, parseString
from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY
from multiprocessing import Pool, Process, JoinableQueue, Queue, current_process, freeze_support, get_logger, set_start_method

SERVICE_ACCOUNT_KEY="key.json"
LOGGING_LEVEL = logging.DEBUG
LOG_FORMAT = '{"timestamp": "%(asctime)s", "libname": "%(name)s", "loglevel": "%(levelname)s", "functname": "%(funcName)s", "lineno": "%(lineno)d", "message": "%(message)s"}'
#LOG_FILE_LOCATION = '/var/log/bqbackup'
LOG_FILE_LOCATION = '/tmp'


# utilities
def get_storage_client(project_id):
	# Explicitly use service account credentials by specifying the private key
	# file.
	#if bigquery_client == "":
	# bigquery_client = storage.Client.from_service_account_json(
	# 	SERVICE_ACCOUNT_KEY,project=project_id)

	storage_client = storage.Client(project=project_id)

	return storage_client

def remove_prefix(text, prefix):
	if text.startswith(prefix):
		return text[len(prefix):]
	return text  # or whatever

# returns True if successful
# raise exception if failed
def upload_blob_from_string(project_id, gcs_bucket, source_string, content_type, destination_blob_name):
	"""Uploads a file to the bucket."""
	# The ID of your GCS bucket
	# bucket_name = "your-bucket-name"
	# The string that will become the file at the destination_blob_name
	# source_string = "{somefile: somecontent}"
	# The ID of your GCS object
	# destination_blob_name = "storage-object-name"
	logger = create_logger(project_id)

	try:
		storage_client = get_storage_client(project_id)

		bucket_name = remove_prefix(gcs_bucket,"gs://")
		bucket = storage_client.bucket(bucket_name)
		blob = bucket.blob(destination_blob_name)

		# Customize retry with a deadline of 500 seconds (default=120 seconds).
		modified_retry = DEFAULT_RETRY.with_deadline(500.0)
		# Customize retry with an initial wait time of 1.5 (default=1.0).
		# Customize retry with a wait time multiplier per iteration of 1.2 (default=2.0).
		# Customize retry with a maximum wait time of 45.0 (default=60.0).
		modified_retry = modified_retry.with_delay(initial=1.5, multiplier=2.0, maximum=60.0)

		blob.upload_from_string(source_string, content_type=content_type, retry=modified_retry)

		curframe = inspect.currentframe()
		calframe = inspect.getouterframes(curframe, 2)
		caller = calframe[1][3]

		logger.info("{} - called upload_blob_from_string string uploaded to {}/{} successfully".format(caller, gcs_bucket, destination_blob_name))

		return True
	except:
		e_type = sys.exc_info()[0]
		e_value = sys.exc_info()[1]
		curframe = inspect.currentframe()
		calframe = inspect.getouterframes(curframe, 2)
		caller = calframe[1][3]
		logger.error('{} - called upload_blob_from_string got Exception uploading string to {} | Error Type: {} | Error Value: {}'.format(caller, destination_blob_name, e_type, e_value))
		raise

	# print(
	# 	"String: {} uploaded to {}.".format(
	# 		source_string, destination_blob_name
	# 	)
	# )

def download_string_from_bucket(project_id, gcs_bucket, source_blob_name):
	"""Uploads a file to the bucket."""
	# The ID of your GCS bucket
	# bucket_name = "your-bucket-name"
	# The string that will become the file at the destination_blob_name
	# source_string = "{somefile: somecontent}"
	# The ID of your GCS object
	# source_blob_name = "storage-object-name"
	logger = create_logger(project_id)

	storage_client = get_storage_client(project_id)

	bucket_name = remove_prefix(gcs_bucket,"gs://")
	bucket = storage_client.bucket(bucket_name)
	blob = bucket.get_blob(source_blob_name)

	curframe = inspect.currentframe()
	calframe = inspect.getouterframes(curframe, 2)
	caller = calframe[1][3]

	logger.info("{} - called download_string_from_bucket string requested: {}/{} successfully".format(caller, gcs_bucket, source_blob_name))

	if blob == None:
		return False, None
	else:
		json_data_bytes = blob.download_as_string()
		json_data = json_data_bytes.decode('utf8')
		return True, json_data

def delete_blob(project_id, bucket_name, blob_name):
	logger = create_logger()
	try:
		storage_client = get_storage_client(project_id)

		bucket_name = remove_prefix(bucket_name,"gs://")

		bucket = storage_client.bucket(bucket_name)

		blob = bucket.blob(blob_name)
		blob.delete()

		print("Blob {} deleted.".format(blob_name))	
		return True
	except:
		e_type = sys.exc_info()[0]
		e_value = sys.exc_info()[1]
		curframe = inspect.currentframe()
		calframe = inspect.getouterframes(curframe, 2)
		caller = calframe[1][3]
		logger.error('{} - called delete_blob got Exception deleting blob to {} | Error Type: {} | Error Value: {}'.format(caller, blob_name, e_type, e_value))
		raise		

def set_retention_policy(project_id, gcs_bucket, retention_period):
	"""Defines a retention policy on a given bucket"""
	# bucket_name = "my-bucket"
	# retention_period = 10

	storage_client = get_storage_client(project_id)

	bucket_name = remove_prefix(gcs_bucket,"gs://")	
	bucket = storage_client.bucket(bucket_name)

	bucket.retention_period = retention_period
	bucket.patch()

	print(
		"Bucket {} retention period set for {} seconds".format(
			bucket.name, bucket.retention_period
		)
	)

def get_retention_policy(project_id, gcs_bucket):
	"""Gets the retention policy on a given bucket"""
	# bucket_name = "my-bucket"

	storage_client = get_storage_client(project_id)

	bucket_name = remove_prefix(gcs_bucket,"gs://")
	bucket = storage_client.bucket(bucket_name)
	bucket.reload()

	print("Retention Policy for {}".format(bucket_name))
	print("Retention Period: {}".format(bucket.retention_period))
	if bucket.retention_policy_locked:
		print("Retention Policy is locked")

	if bucket.retention_policy_effective_time:
		print(
			"Effective Time: {}".format(bucket.retention_policy_effective_time)
		)

def publish_message(project_id, topic_name, data):
    """Publishes a message to a Pub/Sub topic."""
    # [START pubsub_publish]
    # Initialize a Publisher client.
    publisher = pubsub_v1.PublisherClient()

    # Create a fully qualified identifier in the form of
    # `projects/{project_id}/topics/{topic_name}`
    topic_path = publisher.topic_path(project_id, topic_name)

    # Data must be a bytestring
    data = data.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data=data)
    print(future.result())

    print(f"Published messages to {topic_path}.")
    # [END pubsub_publish]

# Parse filename GCS path
def parse_gcs_path(gcs_path):
    # Split the path into components
    path_components = gcs_path.split("/")
    # The first component is the bucket name
    bucket_name = path_components[2]
    # The rest of the components are the object name
    blob_name = "/".join(path_components[3:])
    return bucket_name, blob_name

def main(args):
    if args.projectid:
        project_id = args.projectid
    else:
        project_id = None

    if args.pubsub_topic:
        pubsub_topic = args.pubsub_topic
    else:
        pubsub_topic = None

    if args.message_data:
        message_data = args.message_data
    else:
        message_data = None

    if args.message_data_file:
        message_data_file = args.message_data_file
    else:
        message_data_file = None

    if args.message_data_csv:
        message_data_csv = args.message_data_csv
    else:
        message_data_csv = None

    if args.csv_payload_index:
        csv_payload_index = int(args.csv_payload_index)
    else:
        if message_data_csv:
            print("Please specify the index of the payload in the CSV file")
            sys.exit(1)

    if args.csv_column_headers:
        csv_column_headers = args.csv_column_headers
    else:
        csv_column_headers = False

    if args.csv_payload_gcs_link:
        csv_payload_gcs_link = args.csv_payload_gcs_link
    else:
        csv_payload_gcs_link = False

    if args.message_data_folder:
        message_data_folder = args.message_data_folder
    else:
        message_data_folder = None

    if args.serviceaccountkey:
        # logger.debug("Using service account key: {}".format(args.serviceaccountkey))
        SERVICE_ACCOUNT_KEY=args.serviceaccountkey
    elif os.getenv('serviceaccountkey'):
        # logger.debug("Using service account key from environment variable: {}".format(os.getenv('serviceaccountkey')))
        SERVICE_ACCOUNT_KEY=os.getenv('serviceaccountkey')		
    else:
        # logger.debug("Using default service account key.")
        SERVICE_ACCOUNT_KEY="key.json"

    logger = create_logger(project_id)

    # topic_name = 'projects/' + project_id + '/topics/' + pubsub_topic

    message_count = 0
    if message_data_csv:
        with open(message_data_csv, 'r') as f:
            # Get the payload from the CSV denoted by the csv_payload_index
            # Need to split on ',' and then get the index of the payload 
            # is the start of the payload which will span multiple line breaks
            # The end of the payload will be the line ending with a comma
            # and with the next field containing "|EOR|"
            # which denotes the end of the record
            # The payload will be the data between the start and end of the record
            message_data = []

            for line in f:
                # Skip the column headers
                if csv_column_headers:
                    csv_column_headers = False
                    continue


                if csv_payload_gcs_link:
                    # Retrieve the CSV payload from GCS
                    # The CSV payload will be the last field in the CSV
                    # The CSV payload will be a link to the GCS object
                    # The CSV payload will be in the format gs://bucket/object
                    gcs_link = line.split(',')[csv_payload_index]
                    # strip whitespace
                    gcs_link = gcs_link.strip()
                    # Parse into bucket and object
                    bucket_name, blob_name = parse_gcs_path(gcs_link)
                    # message_data = get_gcs_object(project_id, gcs_link)

                    # backup_date_str = backup_date.strftime("%Y%m%d")
                    # backup_file_name = "project_backup_plan_details_{}.json".format(project_backup_plan_id)
                    
                    retries=0
                    status=None
                    while status is None:
                        try:
                            status, message_data = download_string_from_bucket(project_id, bucket_name, blob_name)  
                        except:
                            retries+=1
                            e_type = sys.exc_info()[0]
                            e_value = sys.exc_info()[1]
                            if retries <= 5:
                                retry_time=10*retries
                                logger.debug('RETRIES: {} Retrying retrieving project backup plan details for {} with project_backup_plan_id {} for backup_id: {} after waiting {} seconds! Type: {} | Value: {}'.format(retries, project_id, project_backup_plan_id, backup_date, retry_time,  e_type, e_value))        			
                                time.sleep(retry_time)
                            else:
                                logger.debug('MAX RETRIES: {} Retrying retrieving project backup plan details for {} with project_backup_plan_id {} for backup_id: {} after waiting {} seconds! Type: {} | Value: {}'.format(retries, project_id, project_backup_plan_id, backup_date, retry_time,  e_type, e_value)) 
                                logger.error('Error retrieving project backup plan details for %s with project_backup_plan_id %s for backup_id: %s! Type: %s | Value: %s' % (project_id, project_backup_plan_id, backup_date, e_type, e_value))        			
                                break
                    
                    message_count += 1

                    # if status == True:
                    #     project_backup_plan_status = json.loads(tmp_backup_plan_details)
                    # else:
                    #     project_backup_plan_status = None
                    # return status, project_backup_plan_status

                else:
                    # Message Parsing and Processing logic

                    # Split on the comma to get the start of the payload
                    # The payload will span multiple lines
                    # The end of the payload will be the line ending with a comma
                    # and with the next field containing "|EOR|"
                    # which denotes the end of the record
                    # The payload will be the data between the start and end of the record

                    if line.endswith(",|EOR|\n"):
                        message_parts = line.split(",|EOR|\n")
                        message_data.append(message_parts[0])
                        complete_message = ''.join(message_data)
                        # strip " from the start and end of the message
                        complete_message = complete_message.strip('"')
                        complete_message = complete_message.replace('""', '"')
                        print(complete_message)
                        message_count += 1
                        print("Message Count: " + str(message_count))


                        #publish_message(project_id, pubsub_topic, complete_message)

                        message_data = []
                    else:
                        if ("," in line and line.count(',') == 3 and "InstructionText" not in line) and not line.endswith(",|EOR|\n"):
                            message_part = line.split(',')[csv_payload_index]
                            message_data.append(message_part)
                        else:
                            # Continue adding the payload to the message_data list
                            # to complete the payload to be sent to Pub/Sub                    
                            message_data.append(line)                                
                

                publish_message(project_id, pubsub_topic, message_data)

            print("Total Messages: " + str(message_count))

            # for line in f:
            #     # Skip the column headers
            #     if csv_column_headers:
            #         csv_column_headers = False
            #         continue
                

            #     message_data = line.split(',')[csv_payload_index]

            #     publish_message(project_id, pubsub_topic, message_data)
    elif message_data_folder is not None:
        # Publish all the files in the folder
        # Get the list of files in the folder
        count = 0
        for file in sorted(os.listdir(message_data_folder)):
            with open(message_data_folder + "/" + file, 'r') as f:
                # Read the whole file and publish the message to Pub/Sub
                count += 1
                message_data = f.read()
                message_data = message_data.strip('"')
                message_data = message_data.strip("'")
                # message_data_xml = parseString(message_data)
                # elements = message_data_xml.getElementsByTagNameNS("*", "custom-attribute")
                # for element in elements:
                #     if element.getAttribute("attribute-id") == "proratedPrices":
                #         proratedPrices_data = element.firstChild.data
                #         print("Prorated Prices: " + proratedPrices_data)
                        #publish_message(project_id, pubsub_topic, message_data)
                print("Publishing #" + str(count) + " file: " + file)
                publish_message(project_id, pubsub_topic, message_data)
    elif message_data_file is not None:
        # Read the message data from file and publish the message to Pub/Sub
        # Read the whole file and publish the message to Pub/Sub
        with open(message_data_file, 'r') as f:
            message_data = f.read()
            message_data = message_data.strip('"')
            message_data = message_data.strip("'")

            # Publish a single message
            publish_message(project_id, pubsub_topic, message_data)

        
                 
    else:
        # Publish a single message
        publish_message(project_id, pubsub_topic, message_data)

def create_logger(project_id):
	logger = get_logger()

	# # logger configuration
	# logging.raiseExceptions = True
	# logging.lastResort = None
	logfile = '%s/pubsub_publish_%s.log' % (LOG_FILE_LOCATION, project_id)
	# #logging.basicConfig(level=LOGGING_LEVEL, filename=logfile, format=LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
	# logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
	# # set up logging to console
	# console = logging.StreamHandler()
	# console.setLevel(level=logging.DEBUG)
	formatter = logging.Formatter(LOG_FORMAT)
	# console.setFormatter(formatter)
	# logging.getLogger('').addHandler(console)
	logger = logging.getLogger(__name__)    
	# #logger = logging.getLogger('')

	stream_handler = logging.StreamHandler()
	stream_handler.setFormatter(formatter)
	stream_handler.setLevel(logging.DEBUG)
	if not len(logger.handlers): 
		logger.addHandler(stream_handler)

	file_handler = logging.FileHandler(filename=logfile)
	file_handler.setFormatter(formatter)
	file_handler.setLevel(logging.ERROR)
	if not len(logger.handlers): 
		logger.addHandler(file_handler)

	logger.setLevel(logging.DEBUG)

	# logger.setLevel(logging.INFO)
	# formatter = logging.Formatter(\
	#     '[%(asctime)s| %(levelname)s| %(processName)s] %(message)s')
	# handler = logging.FileHandler('logs/your_file_name.log')
	# handler.setFormatter(formatter)

	# this bit will make sure you won't have 
	# duplicated messages in the output
	# if not len(logger.handlers): 
	# 	logger.addHandler(handler)
	return logger

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Search for Dataflow jobs by name.')
    # parser.add_argument('dataflow_name', type=str, help='the name of the Dataflow job to search for')
    parser.add_argument('--projectid', type=str, required=True, help='The GCP project ID to publish messages to')
    parser.add_argument('--pubsub-topic', type=str, required=True, action="store", dest="pubsub_topic", help='the type of project to search (RAPI or DRTAPI)')
    parser.add_argument('--message-data', type=str, required=False, action="store", dest="message_data", help='Message to publish to Pub/Sub')
    parser.add_argument('--message-data-file', type=str, required=False, action="store", dest="message_data_file", help='File containing the message to publish to Pub/Sub')
    parser.add_argument('--message-data-csv', type=str, required=False, action="store", dest="message_data_csv", help='CSV containing the message data to publish to Pub/Sub')
    parser.add_argument('--message-data-csv-payload-index', type=str, required=False, action="store", dest="csv_payload_index", help='CSV containing the message data to publish to Pub/Sub')
    parser.add_argument('--message-data-csv-payload-gcs-link', required=False, action="store_true", dest="csv_payload_gcs_link", help='CSV contains GCP GCS links that refer to the message data to publish to Pub/Sub')
    parser.add_argument('--message-data-csv-column-headers', required=False, action="store_true", dest="csv_column_headers", help='CSV containing the message data has column headers')
    parser.add_argument('--message-data-folder', type=str, required=False, action="store", dest="message_data_folder", help='File system folder containing the message data to publish to Pub/Sub')
    parser.add_argument("--serviceaccountkey", action="store", dest="serviceaccountkey", default="", help="OPTIONAL. Path to service account key")    

    args = parser.parse_args()
    
    main(args)
