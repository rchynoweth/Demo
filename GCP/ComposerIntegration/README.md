# Databricks Integration with Composer and/or Airflow

For demo purposes we will leverage the [GCP Managed Airflow instance](https://console.cloud.google.com/composer/environments/detail/us-west2/rac-demo-composer/). To access the airflow UI please authenticate and click the "Open Airflow UI" button on the top of the page. 

## Cloud Composer

Cloud composer is simply a managed Apache Airflow offering in GCP. There are some custom integrations that make it easier to use in the GCP environment but there are not necessarily huge differences between it and the open source version but I thought there is good value add for using this service.  

As for integration with Databricks, users should reference the open source airflow documentation. Engineers will have two options:  
- [Apache Spark Operators](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html)  (available after 1.0.0)
- [Databricks Operators](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators.html) (available after 1.9.0)

Because Databricks support is for versions `1.9.0+`, This means that native Databricks integration with cloud composer is currently in preview, as the current releases are lagging according to the Apache Airflow release schedule.  
<br></br>
<img src="https://racadlsgen2.blob.core.windows.net/public/ComposerImageVersion.png" />  
<br></br>
In the end users have the following options to run jobs in Databricks.   
- Databricks Integration
    - [Submit Run](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/_api/airflow/providers/databricks/operators/databricks/index.html#airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator)  
        - Submits a Spark job to Databricks using the [api/2.0/jobs/runs/submit](https://docs.databricks.com/api/latest/jobs.html#runs-submit) API endpoint.
        - Which submits a single job to be executed one time. This will not create a job and will not display in the UI. 
    - [Run Now](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators.html#databricksrunnowoperator)  
        - Runs an existing Spark job run to Databricks using the [api/2.0/jobs/run-now](https://docs.databricks.com/api/latest/jobs.html#run-now) API endpoint.
- Spark Integration
    - [Spark JDBC](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/operators/spark_jdbc/index.html#module-airflow.providers.apache.spark.operators.spark_jdbc)  
        - Submits code via a JDBC connection to an existing Spark cluster (Interactive clusters in Databricks)
    - [Spark SQL](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/operators/spark_sql/index.html)  
        - Executes a SQL Query on an existing Spark cluster (Interactive clusters in Databricks)
    - [Spark Submit](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/operators/spark_submit/index.html) 
        - Executes a spark binary to an existing Spark cluster (Interactive clusters in Databricks)
- JDBC Connections 
  - Users can use the [Databricks Simba connector](https://docs.databricks.com/integrations/bi/jdbc-odbc-bi.html) to submit commands directly to Databricks clusters and endpoints via JDBC/ODBC. Please reference this [Scala code](https://github.com/ryanchynoweth44/DatabricksContent/blob/master/jdbc_connection/ScalaSqlAnalyticsConnect.scala) for an example. Please note that Python would have a similar implementation.   
  - This is most likely the worst of the three options. 


Users should almost always rely on the Databricks integration as it allows users to take advantage of job compute. 

Honestly, multi-task jobs are a better option than airflow at this time. Unfortunately, extremely complex DAGs are not supported in multi-task jobs at this time. Airflow accomplishes a lot of conditional logic and dependency mapping using things like [Trigger Rules](https://airflow.incubator.apache.org/concepts.html#trigger-rules).

There is an early project within Databricks called [Airbreeze](https://databricks.atlassian.net/wiki/spaces/UN/pages/2232388819/Managed+Airflow+Jobs) that Chris presented on that would allow for users to author Airflow DAGs and host them within our multi-task jobs interface.  
<br></br>

### Resources 

- Documentation - [Managing Dependecies in Data Pipelines](https://docs.databricks.com/dev-tools/data-pipelines.html)
- [DIAS 2021 Talk](https://databricks.com/session_na21/scaling-your-data-pipelines-with-apache-spark-on-kubernetes) - uses data proc and not databricks. This is a Spark integration not a Databricks integration.  
<br></br>


## Airflow 

Commands to create a local airflow instance inside a conda environment:   
```
conda create -n databricks_gcp_composer python==3.8 -y

conda activate databricks_gcp_composer

pip install apache-airflow==2.1.1 

pip install apache-airflow-providers-databricks

airflow db init

airflow users create --username admin --firstname ryan --lastname Chynoweth --role Admin --email ryan.chynoweth@databricks.com

airflow webserver
```

These commands:

1. Create a directory named airflow and change into that directory.  
1. Use conda to create and spawn a Python virtual environment. Databricks recommends using a Python virtual environment to isolate package versions and code dependencies to that environment. This isolation helps reduce unexpected package version mismatches and code dependency collisions.  
1. Install Airflow and the Airflow Databricks provider packages.  
1. Initialize a SQLite database that Airflow uses to track metadata. In a production Airflow deployment, you would configure Airflow with a standard database. The SQLite database and default configuration for your Airflow deployment are initialized in the airflow directory.  
1. Create an admin user for Airflow.  


NOTE - when using cloud composer you will only need to add the additional pip library `apache-airflow-providers-databricks`.  



## Running the Demo


This demo is built to run on the [Azure Databricks Demo workspace](https://eastus2.azuredatabricks.net/?o=5206439413157315) and uses [GCP Composer](https://console.cloud.google.com/composer/environments/detail/us-west2/rac-demo-composer/) for a hosted airflow solution. 

The following is a screenshot of the sample Airflow DAG that we will be running. As a high level summary we will complete the following tasks. Please note that the links to the tasks will take you to the specific Databricks job that we are executing via the Run Now Operator. 

1. [Create DDLs to define a schema](https://eastus2.azuredatabricks.net/?o=5206439413157315#job/246796)
1. [Ingest data from bronze into the silver schema](https://eastus2.azuredatabricks.net/?o=5206439413157315#job/246793)
1. [Create a gold layer i.e. transformations](https://eastus2.azuredatabricks.net/?o=5206439413157315#job/246840)
1. A task that shows how you can [split tasks in airflow](https://eastus2.azuredatabricks.net/?o=5206439413157315#job/246960) but the code is unrelated to the overall ingestiong process. 

<br></br>
<img src="https://racadlsgen2.blob.core.windows.net/public/Airflow_Dag.png" width=1000/>  
<br></br>
















