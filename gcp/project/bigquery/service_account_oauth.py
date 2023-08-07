from google.oauth2 import service_account
from google.cloud import bigquery

SCOPES = ['https://www.googleapis.com/auth/bigquery']
SERVICE_ACCOUNT_FILE = 'gcp/project/bigquery/di-billingexp-us-prodtool-1-11a3feab97f5.json'

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Construct a BigQuery client object.
client = bigquery.Client(credentials=credentials)

query = """
    SELECT name, SUM(number) as total_people
    FROM `bigquery-public-data.usa_names.usa_1910_2013`
    WHERE state = 'TX'
    GROUP BY name, state
    ORDER BY total_people DESC
    LIMIT 20
"""
query_job = client.query(query)  # Make an API request.

print("The query data:")
for row in query_job:
    # Row values can be accessed by field name or index.
    print("name={}, count={}".format(row[0], row["total_people"]))        