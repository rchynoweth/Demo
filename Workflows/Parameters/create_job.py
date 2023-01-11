import requests 
import json 



create_job_endpoint = "/api/2.1/jobs/create"
job_definition_file = "Workflows/Parameters/APIJobDefinition.json"
pat_token = "<DATABRICKS PERSONAL ACCESS TOKEN>"
workspace_url = "https://adb-123456789123456.78.azuredatabricks.net" # i.e. https://adb-123456789123456.78.azuredatabricks.net


auth = {"Authorization": "Bearer {}".format(pat_token)}


with open(job_definition_file) as f:
    payload = json.load(f)


### Create the job 
response = requests.post("{}{}".format(workspace_url, create_job_endpoint), json=payload, headers=auth)
assert response.status_code == 200


### Run the job now with DIFFERENT parameters 
job_id = json.loads(response.content.decode('utf-8')).get('job_id')
print(f"JOB ID ------------> {job_id}")

job_params = {
    "job_id": job_id,
    "notebook_params": {
        "schema": "rac_demo_db",
        "table": "iot_bronze_2",
        "target_table_name": "iot_silver_2"
        }
    }


run_now_endpoint = "/api/2.1/jobs/run-now"

response = requests.post("{}{}".format(workspace_url, run_now_endpoint), json=job_params, headers=auth)
assert response.status_code == 200

json.loads(response.content.decode('utf-8'))