#!/usr/bin/env python3

import argparse
import os
import subprocess
import uuid
import json
import ast
import datetime
from google.cloud import dataflow_v1beta3
from google.cloud import bigquery
import google.auth
import requests

batch = False

# GCP Dataflow Client functions

def sample_aggregated_list_jobs():
    # Create a client
    credentials, project = google.auth.default()

    client_options = {

        "credentials_file" : "/Users/Lee.Parayno/.config/gcloud/application_default_credentials.json"
    }

    client = dataflow_v1beta3.JobsV1Beta3Client(client_options=client_options)

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

# https://dataflow.googleapis.com/v1b3/projects/{projectId}/jobs/{jobId}
def get_dataflow_metadata(project_id, job_id):
    auth_command="gcloud auth application-default print-access-token"
    print(auth_command)
    # execute command and retrieve output
    output = subprocess.check_output(auth_command, shell=True)

    access_token = output.decode('utf-8')

    headers = {}
    headers['Authorization'] =  ("Bearer " + str(access_token)).strip()
    api_url = "https://dataflow.googleapis.com/v1b3/projects/" + project_id + "/jobs/" + job_id
    r = requests.get(api_url, headers=headers)
    print(r.text)

    return r.text

# curl -X GET \
#     -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
#     "https://cloudresourcemanager.googleapis.com/v3/projects/PROJECT_ID"
# https://dataflow.googleapis.com/v1b3/projects/{projectId}/jobs/{jobId}
def get_dataflow_metrics(project_id, dataflow_name, location, job_id):
    auth_command="gcloud auth application-default print-access-token"
    print(auth_command)
    # execute command and retrieve output
    output = subprocess.check_output(auth_command, shell=True)

    access_token = output.decode('utf-8')

    headers = {}
    headers['Authorization'] =  ("Bearer " + str(access_token)).strip()
    api_url = "https://dataflow.googleapis.com/v1b3/projects/" + project_id + "/jobs/" + job_id + "/metrics"
    r = requests.get(api_url, headers=headers)
    print(r.text)

    metric_json = json.loads(r.text)

    metric_json["projectId"] = project_id
    metric_json["name"] = dataflow_name
    metric_json["id"] = job_id
    metric_json["location"] = location

    json_string = json.dumps(metric_json, sort_keys=True, indent=4, separators=(',', ": "))

    return json_string

def get_billing_data(billing_project_id, project_prefix, project_id, dataflow_name, location, job_id):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=billing_project_id)

    query = f"""
        SELECT project.id, l.value AS JobID,  ROUND(SUM(cost),3) AS JobCost   FROM `di-billingexp-us-prodtool-1.billing_export.gcp_billing_export_resource_v1_008BA6_063404_F354D2` bill, UNNEST(bill.labels) l
        WHERE service.description = 'Cloud Dataflow' and l.key = 'goog-dataflow-job-id'
        and project.id like '%{project_prefix}%'
        and l.value = '{job_id}'
        GROUP BY project.id, JobID
        order by  JobCost desc
    """
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    billing_data = {}
    for row in query_job:
        # Row values can be accessed by field name or index.
        print("project_id={}, job_id={}, job_cost={}".format(row[0], row["JobID"], row["JobCost"]))
        billing_data["projectId"] = project_id
        billing_data["name"] = dataflow_name
        billing_data["location"] = location
        billing_data["job_id"] = job_id
        billing_data["job_cost"] = row["JobCost"]
    
    json_string = json.dumps(billing_data, sort_keys=True, indent=4, separators=(',', ": "))

    return json_string    

def retrieve_dataflows_metadata(project_id, project_dataflow_json, metadata_format="json", billing_project_id=None):
    project_dataflows = {}
    # open json file
    with open(project_dataflow_json) as f:
        project_dataflows = json.load(f)

    dataflow_names = []
    dataflow_locations = []
    # if "bindings" in project_dataflows:
    count=0
    for dataflow in project_dataflows:
        job_id = dataflow["id"]
        name = dataflow["name"]
        location = dataflow["location"]
        state = dataflow["state"]

        count += 1
        print("Dataflow #" + str(count))
        print("\tDataflow Name: " + name)
        print("\tDataflow Location (region): " + location)
        print("\tState: " + state)

        if metadata_format == "csv":
            job_fields = "id,name,location,projectId,createTime,currentState,currentStateTime,environment.version.job_type,environment.version.major"
            format = "'csv(" + job_fields + ")'"
        else:
            format = metadata_format

        # execute gcloud command to retrieve dataflow metadata with process

        # command = "gcloud dataflow jobs describe --project=" + project_id + " --region=" + location + " " + job_id + " --format=" + format
        # print(command)
        # # execute command and retrieve output
        # output = subprocess.check_output(command, shell=True)

        # # make directory if it doesn't exist
        # if not os.path.exists("gcp/project/dataflow/" + project_id + "/" + metadata_format):
        #     os.makedirs("gcp/project/dataflow/" + project_id + "/" + metadata_format)

        # # write output to file
        # with open("gcp/project/dataflow/" + project_id + "/" + metadata_format +  "/dataflow_" + name + "_" + job_id + "." + metadata_format, "wb") as f:
        #     f.write(output)

        # # Pull metric information from REST
        # rest_output = get_dataflow_metrics(project_id, name, location, job_id)

        # # make directory if it doesn't exist
        # rest_path = "gcp/project/dataflow/" + project_id + "/" + "metrics"
        # if not os.path.exists(rest_path):
        #     os.makedirs(rest_path)

        # # write output to file
        # with open(rest_path +  "/dataflow_" + name + "_" + job_id + "." + metadata_format, "w") as f:
        #     f.write(rest_output)

        if billing_project_id is not None:
            project_prefix = project_id
            billing_costs = get_billing_data(billing_project_id, project_prefix, project_id, name, location, job_id)

            # make directory if it doesn't exist
            rest_path = "gcp/project/dataflow/" + project_id + "/" + "billing"
            if not os.path.exists(rest_path):
                os.makedirs(rest_path)

            # write output to file
            with open(rest_path +  "/dataflow_" + name + "_" + job_id + "." + metadata_format, "w") as f:
                f.write(billing_costs)

        dataflow_names.append(name)
        dataflow_locations.append(location)

        # gcloud dataflow jobs describe --project=project-id --location=us-central1 job-id
        # gcloud dataflow jobs describe --project=project-id --location=us-central1 job-id --format=json

        # group = None
        # service_account = None
        # role = None
        # if "members" in binding:
        #     for member in binding["members"]:
        #         if member.startswith("group:"):
        #             group = member.replace("group:", "")
        #             print("Group: " + group)

        #             # # Check if group exists
        #             # if group in groups:
        #             #     # Add role to group
        #             #     groups[group].append(binding["role"])
        #             # else:
        #             #     # Create group
        #             #     groups[group] = [binding["role"]]

        #             if "role" in binding:
        #                 print("\t" + binding["role"])
        #                 role = binding["role"]
        #                 if group in group_roles:
        #                     roles = group_roles[group]
        #                     roles.append(binding["role"])
        #                     group_roles[group] = roles                                
        #                 else:
        #                     roles = []
        #                     roles.append(binding["role"])
        #                     group_roles[group] = roles
        #         elif member.startswith("serviceAccount:"):
        #             service_account = member.replace("serviceAccount:", "")
        #             print("Service Account: " + service_account)

        #             if "role" in binding:
        #                 print("\t" + binding["role"])
        #                 role = binding["role"]
        #                 if service_account in service_account_roles:
        #                     roles = service_account_roles[service_account]
        #                     roles.append(binding["role"])
        #                     service_account_roles[service_account] = roles
        #                 else:
        #                     roles = []
        #                     roles.append(binding["role"])
        #                     service_account_roles[service_account] = roles

    print("Found " + str(count) + " Dataflows in Project: " + project_id)

    return dataflow_names, dataflow_locations

    return None, None

def main():
    parser = argparse.ArgumentParser(
        description='Retrieve metadata on deployed Dataflows'
    )

    parser.add_argument('--projectid', required=False, help='GCP Project ID')
    parser.add_argument('--project-dataflow-json', required=False, action="store", dest="project_dataflow_json", help='GCP Project Dataflow index as JSON format')
    parser.add_argument('--metadata-format', required=False, action="store", dest="metadata_format", help='Format passed to commands')
    parser.add_argument('--billing-projectid', required=False, action="store", dest="billing_project_id", help='GCP Project ID')

    args = parser.parse_args()

    if args.projectid:
        project_id = args.projectid

    if args.project_dataflow_json:
        project_dataflow_json = args.project_dataflow_json

    if args.metadata_format:
        metadata_format = args.metadata_format
    
    if args.billing_project_id:
        billing_project_id = args.billing_project_id

    if args.metadata_format is not None:
        if args.billing_project_id:
            dataflow_names, dataflow_locations =  retrieve_dataflows_metadata(project_id, project_dataflow_json, metadata_format=metadata_format, billing_project_id=billing_project_id)
        else:
            dataflow_names, dataflow_locations =  retrieve_dataflows_metadata(project_id, project_dataflow_json, metadata_format=metadata_format)
    else:
        if args.billing_project_id:
            dataflow_names, dataflow_locations =  retrieve_dataflows_metadata(project_id, project_dataflow_json, billing_project_id=billing_project_id)
        else:
            dataflow_names, dataflow_locations =  retrieve_dataflows_metadata(project_id, project_dataflow_json)

    #sample_aggregated_list_jobs()

    # get_dataflow_metadata(project_id)


    # Print the list of all the dataflow_names
    # print(dataflow_names)

    # open file to write dated audit file
    # date_audit = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    # audit_file = open("gcp/project/permissions/" + project_id + ".member_audit." + date_audit + ".csv", "w")

    # # write header
    # audit_file.write("GCP ID,Full Name,Email,Roles\n")

    # for group in group_roles:
    #     print(group)
    #     # open group member list file
    #     group_members = open("gcp/project/permissions/" + group, "r")
    #     for member in group_members:
    #         print(member)    
    #         roles = ""
    #         for role in group_roles[group]:
    #             print("\t" + role)
    #             if roles == "":
    #                 roles = role
    #             else:
    #                 roles = roles + ";" + role

    #         audit_file.write(member.strip() + "," + roles + "\n")

    # audit_file.close()




main()



