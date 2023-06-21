from pyspark.sql.functions import *

class DataCollector():

  def __init__(self, spark):
    self.spark = spark # pass the spark object for DDL actions

  def get_catalogs():
    """
    Get all catalogs in the metastore
    """
    return [i.catalog for i in self.spark.sql('show catalogs').collect()]
  
  def get_schemas(catalog_name='main'):
    """
    Get all the schemas in a catalog as a python list
    """
    self.spark.sql(f'use {catalog_name}')
    schema_list = [i.databaseName for i in self.spark.sql('show schemas').select('databaseName').collect()]
    return schema_list
  
  def run_schema_analysis(catalog_name, schema_name, no_scan=True):
    """
    Runs Analysis on the entire schema 
    - Recommended to keep no_scan == True so that it runs faster
    - no_scan = True will not get the rows 
    """
    self.spark.sql(f'use {catalog_name}')
    sql_command = f"ANALYZE TABLES IN {schema_name} COMPUTE STATISTICS"
    sql_command += " NOSCAN" if no_scan else ""
    self.spark.sql(sql_command)


  def get_table_bytes(catalog_name, schema_name, table_name):
    """
    Describes the table and collects the table size in bytes
    """
    sql_command = f" DESC EXTENDED main.prophet_forecast_schema.sku_cost_lookup"
    df = (self.spark.sql(sql_command)
          .filter(col('col_name')=='Statistics')
          .withColumn("data_type", regexp_replace(col("data_type"), " bytes", ""))
          .withColumn("data_type_double", col('data_type').cast("double"))
          .select('data_type_double')
        )
    try: 
      return {'catalog_name': catalog_name, 'schema_name': schema_name, 'table_name': table_name, 'bytes':df.collect()[0][0]} # get the value 
    except : 
      return {'catalog_name': catalog_name, 'schema_name': schema_name, 'table_name': table_name, 'bytes': -1} # if there is an issue we will return -1
  