# Databricks notebook source
# MAGIC %md
# MAGIC # On-shelf Availability 
# MAGIC The purpose of this notebook is to access and prepare the data needed for calculating on-shelf availability (OSA). 
# MAGIC

# COMMAND ----------

spark.sql("create catalog if not exists rac_demo_catalog")
spark.sql("use catalog rac_demo_catalog")
spark.sql("create schema if not exists genie")
spark.sql("use schema genie")

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.types import *
import pyspark.sql.functions as f

import pandas as pd

# COMMAND ----------

# MAGIC %md ## Step 1: Access Data
# MAGIC
# MAGIC Across this and the subsequent notebooks comprising this demonstration, we will identify potential out-of-stock and on-shelf availability issues requiring further scrutiny through the analysis of store inventory records:
# MAGIC
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/osa_tredence_alerts.jpg' width=75%>
# MAGIC
# MAGIC Out-of-stock (OOS) scenarios occur when a retailer does not have enough inventory to meet consumer demand.  When an insufficient number of product units are made available to customers, not only are immediate sales lost but consumer confidence in the retailer is eroded. Out-of-stocks occur for a variety of reasons. Poor forecasting, limited supply, and operational challenges are all common causes. With each, swift action is required to identify and address the source of the problem less they continue to impact sales.  The challenge with out of stocks is that by the time it is identified, the lead time for requesting replacement units and making them available on the shelf for the consumer may require the retailer to live with the issue for quite some time. It is therefore important that any analysis of stocking levels consider the time to replenishment associated with a given item and location.
# MAGIC
# MAGIC A bit different from OOS issues are on-shelf availability (OSA) problems where inventory may be in the store but it's not placed in a manner that makes it easily accessible to customers. Product may be in inventory but the principal display may give the impression the item is out of stock or in low quantity.  Items may be on the shelf but not pulled forward in a manner that makes them easily viewable by customers.  Product may be technically in inventory but in a backroom that's not accessible to customers. Regardless of the reason, OSA issues tend to lead to lost revenue for retailers.
# MAGIC
# MAGIC To illustrate how analysis of OOS and OSA issues may be performed, Tredence has made available a simulated set of inventory and vendor data available for download [here](https://github.com/tredenceofficial/OSA-Data). To make these data available for use with this and the related notebooks, download the CSV files and then load them to your cloud storage environment.  You may [mount](https://docs.databricks.com/data/databricks-file-system.html#mount-object-storage-to-dbfs) that storage to your Databricks environment as */mnt/osa*.  
# MAGIC
# MAGIC We have automated this downloading step for you and used a */tmp/osa* storage path instead through out this accelerator.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /databricks/driver
# MAGIC wget https://raw.githubusercontent.com/tredenceofficial/OSA-Data/main/osa_raw_data.csv
# MAGIC wget https://raw.githubusercontent.com/tredenceofficial/OSA-Data/main/vendor_leadtime_info.csv

# COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/osa/", True)
dbutils.fs.mv("file:/databricks/driver/osa_raw_data.csv", "dbfs:/tmp/osa/osa_raw_data.csv")
dbutils.fs.mv("file:/databricks/driver/vendor_leadtime_info.csv", "dbfs:/tmp/osa/vendor_leadtime_info.csv")

# COMMAND ----------

# DBTITLE 1,Access the Inventory Data
# schema for inventory data
inventory_schema = StructType([
  StructField('date',DateType()),
  StructField('store_id',IntegerType()),
  StructField('sku',IntegerType()),
  StructField('product_category',StringType()),
  StructField('total_sales_units',IntegerType()),
  StructField('on_hand_inventory_units',IntegerType()),
  StructField('replenishment_units',IntegerType()),
  StructField('inventory_pipeline',IntegerType()),
  StructField('units_in_transit',IntegerType()),
  StructField('units_in_dc',IntegerType()),
  StructField('units_on_order',IntegerType()),
  StructField('units_under_promotion',IntegerType()),
  StructField('shelf_capacity',IntegerType())
  ])

# read inventory data and persist as delta table
(
  spark
   .read
   .csv(
       '/tmp/osa/osa_raw_data.csv',
       header = True,
       schema = inventory_schema,
       dateFormat = 'yyyyMMdd'
       )
   .repartition(sc.defaultParallelism) # repartition to ensure it's written in a manner that supports downstream parallelism
   .write
      .format('delta')
      .mode('overwrite')
      .option('overwriteSchema', 'true')
      .save('/tmp/osa/inventory_raw')
   )

# review data
display(spark.table('DELTA.`/tmp/osa/inventory_raw`'))

# COMMAND ----------

# DBTITLE 1,Access the Vendor Data
# schema for vendor data
vendor_schema = StructType([
  StructField('key',IntegerType()),
  StructField('vendor_id',IntegerType()),
  StructField('sub_vendor_id',IntegerType()),
  StructField('store_id',IntegerType()),
  StructField('item_id',IntegerType()),
  StructField('lead_time_in_dc',IntegerType()),
  StructField('lead_time_in_transit',IntegerType()),
  StructField('lead_time_on_order',IntegerType()),
])

# read vendor data and persist as delta table
(
  spark
   .read
   .csv(
     '/tmp/osa/vendor_leadtime_info.csv',
     header = True,
     schema = vendor_schema
     )
  .withColumnRenamed('item_id','sku') # rename item_id to sku for consistency with inventory data
   .write
     .format('delta')
     .mode('overwrite')
     .option('overwriteSchema', 'true')
     .saveAsTable('vendor')
   )

# review data
display(spark.table('vendor'))

# COMMAND ----------

# MAGIC %md ## Step 2: Address Missing Records
# MAGIC
# MAGIC The inventory data contains records for products in specific stores when an inventory-related transaction occurs. Since not every product *moves* on every date, there will be days for which there is no data for certain store and product SKU combinations. 
# MAGIC
# MAGIC Time series analysis techniques used in our framework require a complete set of records for products within a given location. To address the *missing* entries, we will generate a list of all dates for which we expect records. A cross-join with store-SKU combinations will provide the base set of records for which we expect data.  
# MAGIC
# MAGIC In the real world, not all products are intended to be sold at each location at all times.  In an analysis of non-simulated data, we may require additional information to determine the complete set of dates for a given store-SKU combination for which we should have data:

# COMMAND ----------

# DBTITLE 1,Assemble Complete Set of Dates
# calculate start and end of inventory dataset
start_date, end_date = (
  spark
  .table('DELTA.`/tmp/osa/inventory_raw`')
  .groupBy()
    .agg(
      f.min('date').alias('start_date'),
      f.max('date').alias('end_date')  
        )
  .collect()[0]
  )

# generate contiguous set of dates within start and end range
dates = (
  spark
    .range( (end_date - start_date).days + 1 )  # days in range
    .withColumn('id', f.expr('cast(id as integer)')) # range value from long (bigint) to integer
    .withColumn('date', f.lit(start_date) + f.col('id'))  # add range value to start date to generate contiguous date range
    .select('date')
  )

# display dates
display(dates.orderBy('date'))

# COMMAND ----------

# DBTITLE 1,Assemble Complete Set of Stores-SKUs
# extract unique store-sku combinations in inventory records
store_skus = (
  spark
    .table('DELTA.`/tmp/osa/inventory_raw`')
    .select('store_id','sku','product_category')
    .groupBy('store_id','sku')
      .agg(f.last('product_category').alias('product_category')) # just a hack to get last category assigned to each store-sku combination
  )

display(store_skus)

# COMMAND ----------

# MAGIC %md We can now cross-join the contiguous dates with each unique store-SKU found in the inventory dataset to create the expected records in our complete dataset.  Left outer joining these data to our actual inventory data, we will now have a complete set of records though there will be missing values in many fields which we will need to address in our next step:

# COMMAND ----------

# DBTITLE 1,Generate Complete Set of Inventory Records
# generate one record for each store-sku for each date in range
inventory_with_gaps = (
  dates
    .crossJoin(store_skus)
    .join(
      spark.table('DELTA.`/tmp/osa/inventory_raw`').drop('product_category'), 
      on=['date','store_id','sku'], 
      how='leftouter'
      )
  )

# display inventory records
display(inventory_with_gaps)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC We now have one record for each date-store-SKU combination in our dataset.  However, on those dates for which there were no inventory changes, we are currently missing information about the inventory status of those stores and SKUs.  To address this, we will employ a combination of forward filling, *i.e.* applying the last valid record to subsequent records until a new value is encountered, and defaults.  For the forward fill, we will make use of the [last()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.last.html) function, providing a value of *True* for the *ignorenulls* argument which will force it to retrieve the last non-null value in a sequence:

# COMMAND ----------

# DBTITLE 1,Impute Missing Values
# copy dataframe to enable manipulations in loop
inventory_cleansed = inventory_with_gaps

# apply forward fill to appropriate columns
for c in ['shelf_capacity', 'on_hand_inventory_units']:
  inventory_cleansed = (
    inventory_cleansed
      .withColumn(
          c, 
          f.expr('LAST({0}, True) OVER(PARTITION BY store_id, sku ORDER BY date)'.format(c)) # get last non-null prior value (aka forward-fill)
           )
        )
  
# apply default value of 0 to appropriate columns
inventory_cleansed = (
  inventory_cleansed
    .fillna(
      0, 
      [ 'total_sales_units',
        'units_under_promotion',
        'units_in_transit',
        'units_in_dc',
        'units_on_order',
        'replenishment_units',
        'inventory_pipeline'
        ]
      )
  )

# display data with imputed values
display(inventory_cleansed)

# COMMAND ----------

# MAGIC %md ## Step 3: Identify Key Inventory Events
# MAGIC
# MAGIC With our complete inventory dataset in-hand, we can now identify key inventory-related events within the data.  These include the occurrence of promotions intended to drive product sales and replenishment events during which new units are added to inventory:

# COMMAND ----------

# DBTITLE 1,Calculate Inventory Flags
# derive inventory flags
inventory_final = (
  inventory_cleansed
    .withColumn('promotion_flag', f.expr('CASE WHEN units_under_promotion > 0 THEN 1 ELSE 0 END'))
    .withColumn('replenishment_flag', f.expr('CASE WHEN replenishment_units > 0 THEN 1 ELSE 0 END'))
    )

display(inventory_final)

# COMMAND ----------

# MAGIC %md We can now persist this data for later use:

# COMMAND ----------

# DBTITLE 1,Persist Updated Inventory Data
(
  inventory_final
    .repartition(sc.defaultParallelism)
    .write
      .format('delta')
      .mode('overwrite')
      .option('overwriteSchema', 'true')
      .saveAsTable('inventory')
   )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
