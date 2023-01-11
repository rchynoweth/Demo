# Pipeline Example 

The is example shows several different options for reading data from an API, ingesting into Delta Lake, and ETL/ELT into Silver and Gold tables. 

Ingesting data is the process of extracting the data from source systems and loading into target locations. For this process users have almost an unlimited number of options: Python/Scala/R code on Databricks, FiveTran, Azure Data Factory, Azure Functions, AWS Lambda, etc. In our case I will write a Python notebook that extracts data from an API and writes directly to Delta or to JSON files using a single node cluster to limit costs (plus it is not a distributed process). 


To implement ETL and ELT processes on Databricks, engineers have several different options. Users can leverage Delta Live Tables for a managed pipeline experienc, Databricks workflows using batch processes, or Databricks workflows with streaming processes. 

We will have several examples of the different options:
1. Ingest notebook runs then triggers a DLT pipeline 
1. Ingest notebook runs then triggers dependent Databricks notebooks using workflows as a batch process 
1. Ingest notebook runs and files are automatically loaded using a streaming process  




Here is a screenshot of the workflow that handles the three different options. 

<img src="https://racadlsgen2.blob.core.windows.net/public/API_Pipeline_Workflows_Capture.png" width=1000 />

