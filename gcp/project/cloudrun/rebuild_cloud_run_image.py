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
# Import GCP discovery
from googleapiclient import discovery
from multiprocessing import Pool, Process, JoinableQueue, Queue, current_process, freeze_support, get_logger, set_start_method
import json
import configparser

SERVICE_ACCOUNT_KEY="key.json"
LOGGING_LEVEL = logging.DEBUG
LOG_FORMAT = '{"timestamp": "%(asctime)s", "libname": "%(name)s", "loglevel": "%(levelname)s", "functname": "%(funcName)s", "lineno": "%(lineno)d", "message": "%(message)s"}'
#LOG_FILE_LOCATION = '/var/log/bqbackup'
LOG_FILE_LOCATION = '/tmp'


# Utilities

# Retrieve the GCP Cloud Run Services, optionally also managed platform
# equivalent of: gcloud run services list --platform managed
def get_cloud_run_services(project_id, service_name=None):
    logger = create_logger(project_id)

    try:
        # Create the Cloud Run API Service object
        service = discovery.build('run', 'v1', cache_discovery=False)

        # Retrieve the list of Cloud Run Services
        request = service.namespaces().services().list(parent='namespaces/{}'.format(project_id))
        response = request.execute()

        # Filter the list of Cloud Run Services
        if service_name:
            services = [s for s in response['items'] if s['metadata']['name'] == service_name]
        else:
            services = response['items']

        return services
    except:
        e_type = sys.exc_info()[0]
        e_value = sys.exc_info()[1]
        logger.error('get_cloud_run_services got Exception | Error Type: {} | Error Value: {}'.format(e_type, e_value))
        raise

# Retrieve the GCP Cloud Run Endpoints for a Service
def get_cloud_run_endpoints(project_id, service_name):
    logger = create_logger(project_id)

    try:
        # Create the Cloud Run API Service object
        service = discovery.build('run', 'v1', cache_discovery=False)

        # Retrieve the list of Cloud Run Services
        request = service.namespaces().services().get(name='namespaces/{}/services/{}'.format(project_id, service_name))
        response = request.execute()

        # Filter the list of Cloud Run Services
        endpoints = response['status']['url']

        return endpoints
    except:
        e_type = sys.exc_info()[0]
        e_value = sys.exc_info()[1]
        logger.error('get_cloud_run_endpoints got Exception | Error Type: {} | Error Value: {}'.format(e_type, e_value))

# Retrieve the Cloud Endpoints Configs for a Service
# Equivalent of gcloud endpoints configs list --service=<FQDN>
def get_cloud_run_endpoint_configs(project_id, service_url):
    logger = create_logger(project_id)

    try:
        # Create the Cloud Endpoints API Service object
        service = discovery.build('servicemanagement', 'v1', cache_discovery=False)

        # Retrieve the list of Cloud Endpoints Configs
        request = service.services().configs().list(serviceName=service_url)
        response = request.execute()

        # Filter the list of Cloud Endpoints Configs
        configs = response['serviceConfigs']

        return configs
    except:
        e_type = sys.exc_info()[0]
        e_value = sys.exc_info()[1]
        logger.error('get_cloud_run_endpoint_configs got Exception | Error Type: {} | Error Value: {}'.format(e_type, e_value))


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
    config = configparser.ConfigParser()
    config.read('gcp/project/cloudrun/cloud_run.ini')

    gcloud_build_image_path = config['DEFAULT']['gcloud_build_image_path']

    if args.projectid:
        project_id = args.projectid
    else:
        project_id = None
    if args.service_name:
        service_name = args.service_name
    else:
        service_name = None

    # if args.pubsub_topic:
    #     pubsub_topic = args.pubsub_topic
    # else:
    #     pubsub_topic = None

    # if args.message_data:
    #     message_data = args.message_data
    # else:
    #     message_data = None

    # if args.message_data_file:
    #     message_data_file = args.message_data_file
    # else:
    #     message_data_file = None

    # if args.message_data_csv:
    #     message_data_csv = args.message_data_csv
    # else:
    #     message_data_csv = None

    # if args.csv_payload_index:
    #     csv_payload_index = int(args.csv_payload_index)
    # else:
    #     if message_data_csv:
    #         print("Please specify the index of the payload in the CSV file")
    #         sys.exit(1)

    # if args.csv_column_headers:
    #     csv_column_headers = args.csv_column_headers
    # else:
    #     csv_column_headers = False

    # if args.csv_payload_gcs_link:
    #     csv_payload_gcs_link = args.csv_payload_gcs_link
    # else:
    #     csv_payload_gcs_link = False

    # if args.message_data_folder:
    #     message_data_folder = args.message_data_folder
    # else:
    #     message_data_folder = None

    # if args.serviceaccountkey:
    #     # logger.debug("Using service account key: {}".format(args.serviceaccountkey))
    #     SERVICE_ACCOUNT_KEY=args.serviceaccountkey
    # elif os.getenv('serviceaccountkey'):
    #     # logger.debug("Using service account key from environment variable: {}".format(os.getenv('serviceaccountkey')))
    #     SERVICE_ACCOUNT_KEY=os.getenv('serviceaccountkey')		
    # else:
    #     # logger.debug("Using default service account key.")
    #     SERVICE_ACCOUNT_KEY="key.json"

    logger = create_logger(project_id)

    # Retrieve GCP Cloud Run Services
    if service_name:
        services = get_cloud_run_services(project_id, service_name=service_name)
    else:
        services = get_cloud_run_services(project_id)
	
    print("Found {} Cloud Run Services".format(len(services)))
    # # load as JSON
    # services_json = json.loads(services)

    # Iterate through services
    for service in services:
	    # Retrieve Cloud Run Service details
        service_name = service['metadata']['name']
        service_url = service['status']['url']
        service_url_fqdn = service_url.split('https://')[-1]

        # Print Cloud Run Service details
        print("Service Name: {}".format(service_name))
        print("Service URL: {}".format(service_url))
        print("Service URL FQDN: {}".format(service_url_fqdn))


        service_image = service['spec']['template']['spec']['containers'][0]['image']
        service_image_name = service_image.split('/')[-1]
        service_image_name = service_image_name.split(':')[0]
        service_image_tag = service_image.split(':')[-1]
        service_image_tag = service_image_tag.split('@')[0]
        service_image_path = service_image.split(':')[0]
        # service_image_path = service_image_path.split('@')[0]
        # service_image_path = service_image_path.split('/')[-1]
        # service_image_path = service_image_path.split('.')[0]
        # service_image_path = service_image_path.split('-')[0]
        # service_image_path = service_image_path.split('_')[0]
        # service_image_path = service_image_path.split('+')[0]
        # service_image_path = service_image_path.split('=')[0]
        # service_image_path = service_image_path.split('~')[0]
        # service_image_path = service_image_path.split(';')[0]
        # service_image_path = service_image_path.split(',')[0]
        # service_image_path = service_image_path.split('!')[0]
        # service_image_path = service_image_path.split('@')[0]
        # service_image_path = service_image_path.split('#')[0]
        # service_image_path = service_image_path.split('$')[0]
        # service_image_path = service_image_path.split('%')[0]
        # service_image_path = service_image_path.split('^')[0]
        # service_image_path = service_image_path.split('&')[0]
        # service_image_path = service_image_path.split('*')[0]
        # service_image_path = service_image_path.split('(')[0]
        # service_image_path = service_image_path.split(')')[0]
        # service_image_path = service_image_path.split('-')[0]
        # service_image_path = service_image_path.split('_')[0]
        # service_image_path = service_image_path.split('=')[0]
        # service_image_path = service_image_path.split('+')[0]
        # service_image_path = service_image_path.split('~')[0]
        # service_image_path = service_image_path.split(';')[0]
        # service_image_path = service_image_path.split(',')[0]
        # service_image_path = service_image_path.split('!')[0]
        # service_image_path = service_image_path.split('@')[0]
        # service_image_path = service_image_path.split('#')[0]
        # service_image_path = service_image_path.split('$')[0]
        # service_image_path = service_image_path.split('%')[0]
        # service_image_path = service_image_path.split('^')[0]
        # service_image_path = service_image_path.split('&')[0]
        # service_image_path = service_image_path.split('*')[0]
        # service_image_path = service_image_path.split('(')[0]
        # service_image_path = service_image_path.split(')')[0]
        # service_image_path = service_image_path.split('-')[0]
        # service_image_path = service_image_path.split('_')[0]
        # service_image_path = service_image_path.split('=')[0]
        # service_image_path = service_image_path.split('+')[0]
        # service_image_path = service_image_path.split('~')[0]
        # service_image_path = service_image_path.split(';')[0]
        # service_image_path = service_image_path.split(',')[0]
        # service_image_path = service_image_path.split('!')[0]
        # service_image_path = service_image_path.split('@')[0]
        # service_image_path = service_image_path.split('#')[0]
        # service_image_path = service_image_path.split('$')[0]
        # service_image_path = service_image_path.split('%')[0]

        # Retrieve Cloud Run Service Endpoints
        service_endpoints = get_cloud_run_endpoint_configs(project_id, service_url_fqdn)

        # Print endpoints returned
        print("Found {} Cloud Run Endpoints".format(len(service_endpoints)))

        # Iterate through endpoints
        for endpoint in service_endpoints:
            # Retrieve Cloud Run Endpoint details
            endpoint_name = endpoint['name']
            endpoint_title = endpoint['title']
            endpoint_id = endpoint['id']

            # Print Cloud Run Endpoint details
            print("Endpoint Name: {}".format(endpoint_name))
            print("Endpoint Title: {}".format(endpoint_title))
            print("Endpoint ID: {}".format(endpoint_id))




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
    parser = argparse.ArgumentParser(description='Rebuild Cloud Run Images')
    # parser.add_argument('dataflow_name', type=str, help='the name of the Dataflow job to search for')
    parser.add_argument('--projectid', type=str, required=True, help='The GCP project ID containing the Cloud Run service')
    parser.add_argument('--service-name', type=str, required=False, action="store", dest="service_name", help='The Cloud Run service')
    parser.add_argument('--region', type=str, required=True, help='The Cloud Run region')
    # parser.add_argument('--pubsub-topic', type=str, required=True, action="store", dest="pubsub_topic", help='the type of project to search (RAPI or DRTAPI)')
    # parser.add_argument('--message-data', type=str, required=False, action="store", dest="message_data", help='Message to publish to Pub/Sub')
    # parser.add_argument('--message-data-file', type=str, required=False, action="store", dest="message_data_file", help='File containing the message to publish to Pub/Sub')
    # parser.add_argument('--message-data-csv', type=str, required=False, action="store", dest="message_data_csv", help='CSV containing the message data to publish to Pub/Sub')
    # parser.add_argument('--message-data-csv-payload-index', type=str, required=False, action="store", dest="csv_payload_index", help='CSV containing the message data to publish to Pub/Sub')
    # parser.add_argument('--message-data-csv-payload-gcs-link', required=False, action="store_true", dest="csv_payload_gcs_link", help='CSV contains GCP GCS links that refer to the message data to publish to Pub/Sub')
    # parser.add_argument('--message-data-csv-column-headers', required=False, action="store_true", dest="csv_column_headers", help='CSV containing the message data has column headers')
    # parser.add_argument('--message-data-folder', type=str, required=False, action="store", dest="message_data_folder", help='File system folder containing the message data to publish to Pub/Sub')
    # parser.add_argument("--serviceaccountkey", action="store", dest="serviceaccountkey", default="", help="OPTIONAL. Path to service account key")    

    args = parser.parse_args()

    main(args)
