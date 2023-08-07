
import time
import json
import jwt
import requests

SERVICE_ACCservice_account_file=json.loads(open(SERVICE_ACCOUNT_KEY).read())

private_key = service_account_file['private_key']

iat = time.time()
exp = iat + 3600
payload = {'iss': 'svc-dataflowmgr@dpr-rapi-us-qa-1.iam.gserviceaccount.com',
           'sub': 'svc-dataflowmgr@dpr-rapi-us-qa-1.iam.gserviceaccount.com',
           'aud': 'https://dataflow.googleapis.com/',
           'iat': iat,
           'exp': exp}
additional_headers = {'kid': private_key}
signed_jwt = jwt.encode(payload, private_key, headers=additional_headers,
                       algorithm='RS256')OUNT_KEY='gcp/project/dataflows/dpr-rapi-us-qa-1/key.json'



project_id = 'dpr-rapi-us-qa-1'
location = 'us-central1'
job_id = '2023-01-25_12_30_04-10365565250158519578'


headers = {}
headers['Authorization'] =  ("Bearer " + str(signed_jwt)).strip()
# api_url = "https://dataflow.googleapis.com/v1b3/projects/" + project_id + "/jobs/" + job_id
api_url = "https://dataflow.googleapis.com/v1b3/projects/" + project_id + "/locations/" + location +  "/jobs/" + job_id
r = requests.get(api_url, headers=headers)
print(r.text)

metadata_json = json.loads(r.text)
                    