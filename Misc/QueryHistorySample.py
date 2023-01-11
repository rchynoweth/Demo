import requests 
import json 



warehouse_id = "<sql warehouse id>" # remove this if you don't want to filter if you want 
pat = "<personal access token>"
databricks_instance = '<databricks instance>' # i.e 'adb-984752964297111.11.azuredatabricks.net'


endpoint = f'https://{databricks_instance}/api/2.0/sql/history/queries' 

auth = {"Authorization": f"Bearer {pat}"} 

response = requests.get(endpoint, headers=auth, json={'include_metrics': True, 'filter_by':{"warehouse_ids": [warehouse_id]}}) # remove warehouse filter if you want

data = json.loads(response.content.decode('utf-8'))
data



## I just wrote it out to a json file to inspect as a Spark dataframe

with open('/dbfs/tmp/query_history.json', 'w') as f:
    json.dump(data.get('res'), f)
    
    
df = (spark.read.json("/tmp/query_history.json"))
display(df)