from pyspark.sql.functions import *


class DataCollector():

  def __init__(self, spark):
    self.spark = spark 

  def get_catalogs(self):
    """
    Get all catalogs in the metastore
    """
    self.catalogs = [i.catalog for i in self.spark.sql('show catalogs').collect()]
    return self.catalogs
  
  def get_schemas(self, catalog_name='main'):
    """
    Get all the schemas in a catalog as a python list
    """
    self.spark.sql(f'use {catalog_name}')
    df = (
      self.spark.sql('show schemas')
      .select(col('databaseName').alias('schema_name'))
      .withColumn('catalog_name', lit(catalog_name))
    )
    return df.collect()


  def get_table_bytes(self, catalog_name, schema_name, table_name):
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
  