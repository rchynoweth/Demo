## 
## This is an extremely simple airflow DAG that excutes a single notebook
## Export this notebook as a .py file and import it into airflow
## 
## 


from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow'
}

with DAG('databricks_dag',
    start_date = days_ago(2),
    schedule_interval = None,
    default_args = default_args
    ) as dag:

    opr_run_now = DatabricksRunNowOperator(
        task_id = 'notebook_run',
        databricks_conn_id = 'databricks_default',
        job_id = 8400 # specific to the job id in databricks
    )


print("Done")