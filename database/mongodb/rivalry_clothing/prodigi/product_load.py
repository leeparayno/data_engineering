#!/usr/bin/env python3

import argparse
import os
import subprocess
import uuid
import json
import ast
import logging
import time
import datetime
import sys
import psutil
from google.cloud import dataflow_v1beta3
from multiprocessing import Pool, Process, JoinableQueue, Queue, current_process, freeze_support, get_logger, set_start_method
from queue import Empty, Full
from my_joinable_queue import MyJoinableQueue

batch = False
LOGGING_LEVEL = logging.DEBUG
LOG_FORMAT = '{"timestamp": "%(asctime)s", "libname": "%(name)s", "loglevel": "%(levelname)s", "functname": "%(funcName)s", "lineno": "%(lineno)d", "message": "%(message)s"}'
#LOG_FILE_LOCATION = '/var/log/bqbackup'
LOG_FILE_LOCATION = '/tmp'

# GCP Dataflow Client functions

def sample_aggregated_list_jobs():
    # Create a client
    client = dataflow_v1beta3.JobsV1Beta3Client()

    # Initialize request argument(s)
    request = dataflow_v1beta3.ListJobsRequest(
    )

    # Make the request
    page_result = client.aggregated_list_jobs(request=request)

    # Handle the response
    for response in page_result:
        print(response)

async def sample_aggregated_list_jobs_async():
    # Create a client
    client = dataflow_v1beta3.JobsV1Beta3AsyncClient()

    # Initialize request argument(s)
    request = dataflow_v1beta3.ListJobsRequest(
    )

    # Make the request
    page_result = client.aggregated_list_jobs(request=request)

    # Handle the response
    async for response in page_result:
        print(response)

def import_single_file(connection_uri, db, collection, file_path, index=None, total=None):
    try:
        # mongoimport --db=gcp --collection=dataflows --authenticationDatabase=admin mongodb://leeparayno:n10SCheart@127.0.0.1:27017
        # file_path = source_path + "/" + json_file
        command = "mongoimport --db=" + db + " --collection=" + collection + " --authenticationDatabase=admin " + connection_uri + " " + file_path

        # execute command and retrieve output
        output = subprocess.check_output(command, shell=True)        

        if index and total:
            print("Imported " + str(index) + " of " + str(total) + " files")
        else:
            print("Imported " + file_path + " successfully")

        print(output)    
    except Exception as e:
        print(e)

def parse_product_file(product_file):
    with open(product_file) as f:
        product_data = json.load(f)

    product = product_data["product"]

    # Generate a random UUID
    unique_id = uuid.uuid4()

    product["id"] = str(unique_id)

    variants = product["variants"]

    # for variant in variants:
    #     variant["product_id"] = product["id"]

    product_data_str = json.dumps(product, indent=4)

    print(product_data_str)

    return product

def parse_product_variants(product_file):
    with open(product_file) as f:
        product_data = json.load(f)

    product = product_data["product"]
    sku = product["sku"]
    description = product["description"]


    variants = product["variants"]

    enriched_variants = []
    for variant in variants:
        # Generate a random UUID
        unique_id = uuid.uuid4()

        variant["id"] = str(unique_id)        
        # variant["product_id"] = product["id"]
        variant["vendorId"] = "1bd50e94-205a-40a6-9106-a82f020bdeb5"
        variant["componentTypeId"] = "74d856b2-9728-428e-b2e7-c109f02157f9"
        variant["componentTypeClassificationId"] = "010c3cc1-0b1f-4953-9f7a-64d8f35c6256"
        variant["vendorSKU"] = sku
        variant["description"] = description
        enriched_variants.append(variant)
    #     variant["product_id"] = product["id"]

    product_data_str = json.dumps(product, indent=4)

    print(product_data_str)

    return enriched_variants

def import_into_mongodb(connection_uri, db, collection, product_file, authentication_db="admin"):
    

    # # source directory
    # path = "gcp/project/dataflow/" + project_id + "/json"

    # Parse product and insert
    product_data = parse_product_file(product_file)

    # Write to MongoDB
    from pymongo import MongoClient
    # connection_uri+="/?authSource=" + authentication_db
    client = MongoClient(connection_uri)
    db = client[db]
    collection = db[collection]
    product_data = parse_product_file(product_file)
    
    product_data["vendorId"] = "1bd50e94-205a-40a6-9106-a82f020bdeb5"
    product_data["componentTypeId"] = "74d856b2-9728-428e-b2e7-c109f02157f9"
    product_data["componentTypeClassificationId"] = "010c3cc1-0b1f-4953-9f7a-64d8f35c6256"    
    collection.insert_one(product_data)

    # Parse variants and insert
    variants = parse_product_variants(product_file)

    # Write to MongoDB
    collection = "components"
    variant_collection = db[collection]
    variant_collection.insert_many(variants)


    # get list of all files in directory
    # dir_list = os.listdir(source_path)
    # count = 0
    # import_data_list = []
    # for json_file in dir_list:
    #     # mongoimport --db=gcp --collection=dataflows --authenticationDatabase=admin mongodb://leeparayno:n10SCheart@127.0.0.1:27017
    #     file_path = source_path + "/" + json_file
    #     command = "mongoimport --db=" + db + " --collection=" + collection + " --authenticationDatabase=admin " + connection_uri + " " + file_path

    #     # execute command and retrieve output
    #     output = subprocess.check_output(command, shell=True)        

    #     count += 1
    #     print("Imported " + str(count) + " of " + str(len(dir_list)) + " files")

    #     print(output)

def prepare_import_jobs(connection_uri, db, collection, project_id, source_path):
    

    # # source directory
    # path = "gcp/project/dataflow/" + project_id + "/json"

    # get list of all files in directory
    dir_list = os.listdir(source_path)
    count = 0
    import_data_list = []
    for json_file in dir_list:
        # mongoimport --db=gcp --collection=dataflows --authenticationDatabase=admin mongodb://leeparayno:n10SCheart@127.0.0.1:27017
        file_path = source_path + "/" + json_file
        # command = "mongoimport --db=" + db + " --collection=" + collection + " --authenticationDatabase=admin " + connection_uri + " " + file_path

        # # execute command and retrieve output
        # output = subprocess.check_output(command, shell=True)        

        # count += 1
        # print("Imported " + str(count) + " of " + str(len(dir_list)) + " files")

        # print(output)

        count += 1
        print("File #" + str(count) + " of " + str(len(dir_list)) + " prepared for import")
        print("\tFile Path: " + file_path)
        print("\tConnection URI: " + connection_uri)
        print("\tDatabase: " + db)
        print("\tCollection: " + collection)

        import_data = build_import_data(connection_uri, db, collection, file_path, index=count, total=len(dir_list))
        import_data_list.append(import_data)

    print("Found " + str(count) + " Dataflows in Project: " + project_id)

    if len(import_data_list) > 0:
        submitted_jobs = process_job_data(import_data_list, process_import)

    return submitted_jobs

def build_import_data(connection_uri, db, collection, file_path, index=None, total=None):
    import_data = {}
    import_data['connection_uri'] = connection_uri
    import_data['db'] = db
    import_data['collection'] = collection
    import_data['file_path'] = file_path
    if index is not None:
        import_data['index'] = index
    if total is not None:
        import_data['total'] = total

    return import_data

def process_import(import_data):
    try:
        connection_uri = import_data['connection_uri']
        db = import_data['db']
        collection = import_data['collection']
        file_path = import_data['file_path']
        if "index" in import_data:
            index = import_data['index']
        if "total" in import_data:
            total = import_data['total']

        logger = create_logger()

        import_single_file(connection_uri, db, collection, file_path, index=index, total=total)

    except:
        e_type = sys.exc_info()[0]
        e_value = sys.exc_info()[1]
        e_traceback = sys.exc_info()[2]
        print("Error: " + str(e_type) + " " + str(e_value))
        print("Traceback: " + str(e_traceback))

# job management functions
def process_job_data(data, func, num_cpus=None):
    logger = create_logger()

    # Get the number of cores
    if num_cpus is None:
        num_cpus = int(psutil.cpu_count(logical=False))

    print('* Parallel processing')
    print('* Running on {} cores'.format(num_cpus))

    # Set-up the queues for sending and receiving data to/from the workers
    # queue_dataflow_jobs = MyJoinableQueue()
    # queue_dataflow_pending = MyJoinableQueue()
    # queue_dataflow_completed = MyJoinableQueue()



    queue_written = MyJoinableQueue()
    queue_retry = MyJoinableQueue()

    # Gather processes and results here
    dataflow_metadata_manager_processes = []

    results = []

    # Count tasks
    num_tasks = 0

    # # Add the tasks to the queue
    # for backup_table_data in data:
    # 	queue_pending.put(backup_table_data)
    # for job in job_list:
    # 	for task in job['tasks']:
    # 		expanded_job = {}
    # 		num_tasks = num_tasks + 1
    # 		expanded_job.update({'func': pickle.dumps(job['func'])})
    # 		expanded_job.update({'task': task})
    # 		queue_pending.put(expanded_job)

    # Use as many workers as there are cores (usually chokes the system so better use less)
    num_dataflow_metadata_manager = num_cpus
    num_writers = 1
    num_retriers = num_cpus
    num_job_managers = num_cpus

    print('* Number of tasks: {}'.format(num_tasks))

    # logger.debug("queue_jobs INITIAL SIZE: has {} items in the queue".format(queue_dataflow_jobs.qsize()))

    count=0

    # Add the intial tasks (without job_id) to the queue
    # backup_status_manager workers can check for table/view backup logs and update the status 
    # if no async_job_finished callback was made
    # for dataset_data in data:
    #     queue_dataflow_jobs.put(dataset_data)
        # queue_pending.put(backup_table_data)

    # logger.debug("queue_jobs STARTING SIZE: has {} items in the queue".format(queue_dataflow_jobs.qsize()))

    # Set-up and start the workers
    # for c in range(num_dataflow_metadata_manager):
    #     p = Process(target=dataflow_metadata_manager, args=(project_id, queue_dataflow_jobs, queue_dataflow_completed))
    #     p.name = 'dataflow_metadata_manager-' + str(c)
    #     dataflow_metadata_manager_processes.append(p)
    #     p.start()

    # # Split the data into multiple chunks of 1000
    chunk_size = 250
    chunks = [data[x:x+chunk_size] for x in range(0, len(data), chunk_size)]
    total_count = len(chunks)

    # create a file to store the completed chunks for this process dated
    date_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    chunks_completed_file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mongodb_load_chunks_completed_' + date_str + '.txt')

    # if chunks_completed_file_name exists, read it and remove the completed chunks from the chunks list
    if os.path.isfile(chunks_completed_file_name):
        with open(chunks_completed_file_name, 'r') as f:
            completed_chunks = f.read().splitlines()
            completed_chunks = [int(x) for x in completed_chunks]
            for completed_chunk in completed_chunks:
                del chunks[completed_chunk]
    for count, chunk in enumerate(chunks):
        logger.info("Starting pool chunk %s out of %s." % (count, total_count))
        pool = Pool(int(num_cpus))
        submitted_jobs=pool.map(func, chunk)
        pool.close()
        pool.join()

        # write completed chunks to status file
        with open(chunks_completed_file_name, 'a') as f:
            f.write(str(count) + '\n')

        # for job in submitted_jobs:
        #     queue_dataflow_completed.put(job)


    # Gather the results
    # completed_tasks_counter = 0
    # while completed_tasks_counter < num_tasks:
    # 	results.append(queue_completed.get())
    # 	completed_tasks_counter = completed_tasks_counter + 1

    logger.debug("process_job_data COMPLETED function: {}; data: {} - total {} jobs".format(func, data, len(data)))

    # return submitted_jobs
    return True 

def create_logger(project_id=None):
    logger = get_logger()

    # # logger configuration
    # logging.raiseExceptions = True
    # logging.lastResort = None
    
    if project_id is not None:
        logfile = '%s/dataflows_%s.log' % (LOG_FILE_LOCATION, project_id)
    else:
        logfile = '%s/dataflows_%s.log' % (LOG_FILE_LOCATION, "undefined")
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

def main():
    if __name__ == '__main__':
        freeze_support()

        parser = argparse.ArgumentParser(
            description='Retrieve metadata on deployed Dataflows'
        )

        parser.add_argument('--connection-uri', required=False, action="store", dest="connection_uri", help='MongoDB connection uri')
        
        parser.add_argument('--db', required=False, help='MongoDB database to import into')
        parser.add_argument('--collection', required=False, help='MongoDB collection to import into')
        parser.add_argument('--projectid', required=False, help='GCP ProjectID')
        parser.add_argument('--product-file', required=False, action="store", dest="product_file", help='Product file to import into MongoDB')
        parser.add_argument('--source-directory', required=False, action="store", dest="source_directory", help='Source Directory for JSON files')
        parser.add_argument('--authentication-db', required=False, action="store", dest="authentication_db", help='mongoDB Augthentication Database')

        args = parser.parse_args()

        if args.projectid:
            project_id = args.projectid

        if args.connection_uri:
            connection_uri = args.connection_uri

        if args.db:
            db = args.db
        
        if args.collection:
            collection = args.collection

        if args.source_directory:
            source_directory = args.source_directory

        if args.product_file:
            product_file = args.product_file

        if args.authentication_db:
            authentication_db = args.authentication_db

        # # source directory
        # path = "gcp/project/dataflow/" + project_id + "/json"
        
        if product_file:
            result = import_into_mongodb(connection_uri, db, collection, product_file)
        else:
            result = prepare_import_jobs(connection_uri, db, collection, source_directory)

main()
