# Delta Lake - Uniform 

In this demo we will create Delta Tables using the new Uniform capabilities available in Delta Lake 3.0. We will complete the following: 
1. Create Delta Tables in Databricks 
1. Read the data as an Iceberg Table with BigQuery 

Demo Requirements:
1. DBR 13.2 or greater 
1. A Google Service Account attached to the cluster in order to list GCS objects    
    - See "Advanced options"
1. GCP Databricks 
1. Install the `google-cloud-bigquery` PyPi package on the cluster 


## Create a Uniform Delta Lake Table 

Please refer to the this [notebook](./DeltaLake-3-Example.py) for a more detailed examples of how to create Delta Lake tables taking advantage of the Uniform feature. In that notebook we create a new table with Uniform activated and we alter an existing table to begin using the Universal Format. 

1. To begin set the following settings in your notebook:
    ```python
    spark.sql("SET spark.databricks.delta.properties.defaults.minReaderVersion = 2;")
    spark.sql("SET spark.databricks.delta.properties.defaults.minWriterVersion = 7;")
    spark.sql("SET spark.databricks.delta.write.dataFilesToSubdir = true")
    ```

1. Then Create a new empty table:
    ```sql
    CREATE TABLE products (
        product_id STRING,
        product_category STRING,
        product_name STRING,
        sales_price STRING,
        EAN13 STRING,
        EAN5 STRING,
        product_unit STRING
    )
    TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')
    ;
    ```

1. Then using Python you can run the following to insert the data to the table.  
    ```python
    df = spark.read.format('csv').option('header', 'true').option('delimiter', ';').load('/databricks-datasets/retail-org/products/*.csv')
    df.write.mode('append').saveAsTable('products')
    ```


## Creating an External Big Query table via the Metadata files

Now that you have a Delta Lake Table created with the Universal Format activated you can run the following query to create an external table in Big Query. Please replace the `uris` parameter with the appropriate path for your table. 

Please refer [here](https://cloud.google.com/bigquery/docs/iceberg-tables#create-using-metadata-file) for more information. 

  ```sql
    CREATE OR REPLACE EXTERNAL TABLE rac_dataset.ext_products
    OPTIONS (
          format = 'ICEBERG',
          uris = ["gs://bucket_name/table_name/metadata/VERSION-GUID.metadata.json"]
    );

    select * from rac_dataset.ext_products;
  ```

One of the limitations with the metdata file approach for a BigQuery Iceberg tables is that you have to manually update the table to the newest version of the metadata file. In my opinion the best way to do this is from Databricks whenever the table is updated. 

For example, whenever I alter the table in Databricks for ETL purposes, I can leverage the [bq_connect.py](./libs/bq_connect.py) module to run an update on Big Query. 

The class contents looks as follows which contains functions to determine the lastest metdata file and run the update on BigQuery:
```python
from google.cloud import bigquery

class BQConnect():

  def __init__(self, project):
    self.project = project 
    self.client = bigquery.Client( project=self.project)

  def execute_query(self, sql):
    query_job = self.client.query( sql )
    results = query_job.result() 
    return results
  
  def get_latest_iceberg_metadata(self, table_location):
    return [f.path for f in dbutils.fs.ls(table_location+'/metadata') if '.json' in f.path][-1]

  def format_table_update_query(self, dataset, table, metadata_path):
    sql = f"""
          CREATE OR REPLACE EXTERNAL TABLE {dataset}.{table}
          OPTIONS (
              format = 'ICEBERG',
              uris = ["{metadata_path}"]
          );
          """
    return sql
```

This way I can simply run the following in a notebook after a table update. Please note that this is simply a metadta update on the Big Query table so it is FAST and doesn't duplicate the data! 

```python
bq_connect = BQConnect(project=bq_project)

metadata_path = bq_connect.get_latest_iceberg_metadata(table_location)

big_query_tbl_def = bq_connect.format_table_update_query(dataset='rac_dataset', table='ext_products2', metadata_path=metadata_path)

bq_connect.execute_query(big_query_tbl_def)
```

Now you can easily query the table from Big Query as an Iceberg table.
![](./imgs/MetadataAccessResults.png)


## BigLake Metastore Approach (Recommended) - TODO


- https://cloud.google.com/bigquery/docs/iceberg-tables#create-using-biglake-metastore
- Hive Compatible Catalog but is not HMS: https://cloud.google.com/bigquery/docs/manage-open-source-metadata#:~:text=Stay%20organized%20with%20collections%20Save,access%20data%20from%20multiple%20sources.

### Resources 

- [Protocol.md](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-icebergcompatv1)  
- [Column Mapping](https://docs.databricks.com/delta/delta-column-mapping.html)  
- [Uniform](https://docs.databricks.com/delta/uniform.html)
- Query with BigQuery via 