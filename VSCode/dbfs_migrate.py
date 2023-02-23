import os 
from delta import *
from pyspark.sql import SparkSession 
from pyspark.dbutils import DBUtils # DBR < 12 



spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.service.dbutils.fs.parallel.enabled", True)
dbutils = DBUtils(spark) # you can copy entire directories with dbutils.fs.cp("/dir", "/new/dir", recurse=True)


