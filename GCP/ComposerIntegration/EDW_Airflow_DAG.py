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

with DAG('databricks_edw_dag',
    start_date = days_ago(2),
    schedule_interval = None,
    default_args = default_args
    ) as dag:

  
    ## Databricks Run Now Operator Submits existing DBX jobs! 
    ddl_run_now = DatabricksRunNowOperator(
        task_id = 'CREATE_DDLs', # airflow task
        databricks_conn_id = 'databricks_default',
        job_id = 246796,  # specific to the job id in databricks
        notebook_params = {'DatabaseName': 'demo_omop_db'}
    )
    
    ingest_run_now = DatabricksRunNowOperator(
        task_id = 'INGEST',
        databricks_conn_id = 'databricks_default',
        job_id = 246793,  # specific to the job id in databricks
        notebook_params = {'DatabaseName': 'demo_omop_db'}
    )
    
    transform_run_now = DatabricksRunNowOperator(
        task_id = 'TRANSFORM',
        databricks_conn_id = 'databricks_default',
        job_id = 246840,  # specific to the job id in databricks
        notebook_params = {'DatabaseName': 'demo_omop_db'}
    )
    
    split_dag_task = DatabricksRunNowOperator(
        task_id = 'SPLIT_TASK',
        databricks_conn_id = 'databricks_default',
        job_id = 246960,  # specific to the job id in databricks
        notebook_params = {'DatabaseName': 'rac_demo_db', 'UserName': 'ryan.chynoweth@databricks.com'}
    )

    ddl_run_now >> ingest_run_now >> transform_run_now
    ddl_run_now >> split_dag_task

print("DAG Completed Successfully ")

