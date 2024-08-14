# Databricks notebook source
# MAGIC %md 
# MAGIC # Out of Stock
# MAGIC The purpose of this notebook is to identify inventory-related problems in the data including *phantom inventory*, *safety stock* alerts and *zero scan* alerts. 

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
import pyspark.sql.functions as f
from pyspark.sql.types import *

import pandas as pd

# COMMAND ----------

# MAGIC %md ## Step 1: Access Data
# MAGIC
# MAGIC Our first step is to access the inventory and vendor data assembled and cleansed in the last notebook:

# COMMAND ----------

spark.sql("create catalog if not exists rac_demo_catalog")
spark.sql("use catalog rac_demo_catalog")
spark.sql("create schema if not exists genie")
spark.sql("use schema genie")

# COMMAND ----------

# DBTITLE 1,Retrieve Data
inventory = spark.table('inventory')
vendor = spark.table('vendor')

# COMMAND ----------

# MAGIC %md ## Step 2: Identify Phantom Inventory
# MAGIC
# MAGIC Next, we will identify problems related to phantom inventory.  Phantom inventory represents product units not accounted for between the inventory and the sales systems.  These units may represent items misplaced, stolen, lost or otherwise inaccurately tracked in one system or the other.  The identification of these units is essential for ensuring the right quantity, *i.e.* not too few and not too many, of a given product are purchased for replenishment and may point to needed operational improvements to ensure accurate inventories are maintained moving forward.
# MAGIC
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/osa_tredence_phantominventory.png' width=75%>
# MAGIC
# MAGIC Phantom inventory is identified by simply calculating the number of units expected to be on-hand at the end of the day relative to those actually on-hand. Minor differences between the units expected and those actually in inventory may not require immediate attention.  A phantom inventory indicator flag is set when the phantom inventory is some multiple of the average daily sales for a given product. Here, we set this multiple to 5-times but some organizations may wish to be more or less sensitive to the detection of problematic levels of phantom inventory:

# COMMAND ----------

# DBTITLE 1,Calculate Phantom Inventory
# phantom inventory calculations
phantom_inventory = (
  inventory
    
    # average daily sales
    .withColumn('daily_sales_units', f.expr('AVG(total_sales_units) OVER(PARTITION BY store_id, sku ORDER BY date)')) 
    
    # on-hand inventory units at the end of previous day
    # for dates with no prior day inventory units, provide alt calculation
    .withColumn('start_on_hand_units', f.expr('''
      COALESCE( 
        LAG(on_hand_inventory_units, 1) OVER(PARTITION BY store_id, sku ORDER BY date), 
        on_hand_inventory_units + total_sales_units - replenishment_units
        )
        ''')) 
    
    # on-hand inventory units at end of day
    .withColumn('end_on_hand_units', f.expr('COALESCE(on_hand_inventory_units, 0)')) 
    
    # calculate phantom inventory as difference in:
    # (previous day's on-hand inventory + current day's replenished units - current day's sales units) and current day's end-of-day inventory 
    .withColumn('phantom_inventory', f.expr('start_on_hand_units + replenishment_units - total_sales_units - end_on_hand_units')) 
    
    # flag only when phantom inventory is at least 5 times average daily sales
    .withColumn('phantom_inventory_ind', f.expr('''
      CASE
        WHEN phantom_inventory <> 0 AND ABS(phantom_inventory) > 5 * daily_sales_units THEN 1 
        ELSE 0 
        END'''))  
  
    .select(
      'date',
      'store_id',
      'sku',
      'daily_sales_units',
      'start_on_hand_units',
      'replenishment_units',
      'total_sales_units',
      'end_on_hand_units',
      'phantom_inventory',
      'phantom_inventory_ind'
      )
    )

# display results
display(phantom_inventory)

# COMMAND ----------

# MAGIC %md ## Step 3: Identify Out of Stocks
# MAGIC
# MAGIC Out of stocks occur when inventory is not sufficient to meet demand.  Most retailers define a safety stock level that serves as the threshold for triggering replenishment orders.  When inventory dips below the safety stock level, a replenishment order is generated.  The remaining inventory on-hand must then be sufficient to meet demand until the replacement units arrive.
# MAGIC
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/osa_tredence_safetystock.png' width=75%>
# MAGIC
# MAGIC There are numerous ways to calculate a safety stock level for a given product.  Here, we will consider the store-specific dynamics associated with a product to arrive at two potentially valid safety stock levels.  The first of these is the number of units on-hand before past replenishment events. The second of these will consider average daily sales relative to lead times for a product. For a given store-SKU combination, we will take the lower of these two values to be our safety stock level.  But before we can calculate these values, we need to estimate the inventory on-hand, something we tackled in our prior phantom inventory calculations:

# COMMAND ----------

# DBTITLE 1,Join Inventory to Phantom Inventory
# combine inventory with phantom inventory and min lead times
inventory_with_pi = (
  inventory.alias('inv')
    .join(phantom_inventory.alias('pi'), on=['store_id','sku','date'])
  
    # limit fields to use moving forward
    .selectExpr(
      'inv.store_id',
      'inv.sku',
      'inv.date',
      'inv.product_category',
      'inv.on_hand_inventory_units',
      'inv.total_sales_units',
      'inv.replenishment_units',
      'inv.replenishment_flag',
      'inv.units_on_order',
      'inv.units_in_transit',
      'inv.units_in_dc',
      'pi.phantom_inventory'
      )
  
  # correct inventory values to enable calculations
  .withColumn('phantom_inventory', f.expr('COALESCE(phantom_inventory, 0)')) 
  .withColumn('on_hand_inventory_units', f.expr('''
              CASE 
                WHEN on_hand_inventory_units < 0 THEN 0 
                ELSE on_hand_inventory_units 
                END''')
             )
   .withColumn('replenishment_units', f.expr('''
              CASE 
                WHEN replenishment_flag = 1 THEN replenishment_units
                ELSE 0 
                END''')
             )
  
  # initialize estimated on-hand inventory field
   .withColumn('estimated_on_hand_inventory', f.lit(0)) 
  )

display(inventory_with_pi)

# COMMAND ----------

# DBTITLE 1,Estimate On-Hand Inventory
# iterate over inventory to calculate current inventory levels
def get_estimated_inventory(inventory_pd: pd.DataFrame) -> pd.DataFrame:
  
  inventory_pd.sort_values('date', inplace=True)
  
  # iterate over records in inventory data
  for i in range(1,len(inventory_pd)):
    
    # get component values
    previous_inv = inventory_pd.estimated_on_hand_inventory.iloc[i-1]
    if previous_inv < 0: previous_inv = 0
    
    replenishment_units = inventory_pd.replenishment_units.iloc[i]
    total_sales_units = inventory_pd.total_sales_units.iloc[i]
    phantom_inventory_units = inventory_pd.phantom_inventory.iloc[i]
    on_hand_inventory_units = inventory_pd.on_hand_inventory_units.iloc[i]
    
    # calculate estimated on-hand inventory
    estimated_on_hand_inventory = previous_inv + replenishment_units - total_sales_units - phantom_inventory_units
    if estimated_on_hand_inventory < 0: estimated_on_hand_inventory = 0
    if estimated_on_hand_inventory > on_hand_inventory_units: estimated_on_hand_inventory = on_hand_inventory_units
    
    inventory_pd.estimated_on_hand_inventory.iloc[i] = estimated_on_hand_inventory

  return inventory_pd


# calculate estimated on-hand inventory
inventory_on_hand = (
  inventory_with_pi
  .groupby('store_id', 'sku')
    .applyInPandas( get_estimated_inventory, schema=inventory_with_pi.schema )
  )

display(inventory_on_hand)

# COMMAND ----------

# MAGIC %md Using this data, we can now calculate the average amount on-hand prior to a replenishment event as well as the average number of units sold for each store-SKU.  For both metrics, we'll limit the calculation to the 90 days prior to the current inventory record.  This will allow for changes in stocking practices and sales velocity over the dataset:

# COMMAND ----------

# DBTITLE 1,Calculate Average On-Hand and Average Daily Sales Metrics
inventory_with_metrics = (
  inventory_on_hand
    
    # AVERAGE ON-HAND UNITS PRIOR TO REPLENISHMENT
    # ------------------------------------------------------------------------------------
    # getting prior day's on-hand inventory only for the days with replenishment
    .withColumn('prior_inventory', f.expr('LAG(estimated_on_hand_inventory, 1) OVER(PARTITION BY store_id, sku ORDER BY date)'))
    .withColumn('prior_inventory', f.expr('COALESCE(prior_inventory,0)'))
    .withColumn('prior_inventory', f.expr('CASE WHEN replenishment_flag=1 THEN prior_inventory ELSE 0 END'))
  
    # calculating rolling average of prior day's on-hand inventory for days with replenishment (over last 90 days)
    .withColumn('rolling_stock_onhand', f.expr('''
      SUM(prior_inventory) OVER(PARTITION BY store_id, sku ORDER BY date ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) /
      (SUM(replenishment_flag) OVER(PARTITION BY store_id, sku ORDER BY date ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) + 1)
      '''
      ))
    .withColumn('rolling_min_expected_stock', f.expr('CASE WHEN replenishment_flag != 1 THEN 0 ELSE rolling_stock_onhand END'))
    .withColumn('rolling_min_expected_stock', f.expr('COALESCE(rolling_min_expected_stock,0)'))
    
    # fixing the inventory values for all dates through forward fill
    .withColumn('min_expected_stock', f.expr('NULLIF(rolling_min_expected_stock,0)'))
    .withColumn('min_expected_stock', f.expr('LAST(min_expected_stock, True) OVER(PARTITION BY store_id, sku ORDER BY date)'))
    .withColumn('min_expected_stock', f.expr('COALESCE(min_expected_stock, 0)'))
    # ------------------------------------------------------------------------------------
  
    # AVERAGE DAILY SALES
    # ------------------------------------------------------------------------------------
    # getting daily sales velocity
    .withColumn('daily_sales_units', f.expr('AVG(total_sales_units) OVER(PARTITION BY store_id, sku ORDER BY date ROWS BETWEEN 90 PRECEDING AND CURRENT ROW)'))
    .withColumn('daily_sales_units', f.expr('LAST(daily_sales_units, True) OVER(PARTITION BY store_id, sku ORDER BY date)'))
    .withColumn('daily_sales_units', f.expr('COALESCE(daily_sales_units, 0)'))
    # ------------------------------------------------------------------------------------
  )

display(inventory_with_metrics)

# COMMAND ----------

# MAGIC %md To derive safety stock levels using average daily sales, we need to know something about the lead times for the replenishment of particular SKUs within a given store location.  We can derive this as follows:

# COMMAND ----------

# DBTITLE 1,Calculate Shortest Lead Time for Each Store-SKU
# calculate shortest lead time for each store-sku combination
lead_time = (
  vendor
    .withColumn('min_lead_time', f.expr('LEAST(lead_time_in_dc, lead_time_in_transit, lead_time_on_order)'))
    .select('store_id', 'sku', 'min_lead_time', 'lead_time_in_dc', 'lead_time_in_transit', 'lead_time_on_order')
    )

display(lead_time)

# COMMAND ----------

# MAGIC %md We can now calculate the safety stock requirements using the lower of the two values calculated from the data:

# COMMAND ----------

# DBTITLE 1,Determine Safety Stock
inventory_safety_stock = (
  inventory_with_metrics
    .join(lead_time, on=['store_id','sku'], how='leftouter')
  
    # safety stock for sales velocity is avg daily sales units * min_lead_time
    .withColumn('ss_sales_velocity', f.expr('daily_sales_units * min_lead_time'))
  
    # use the lower of the min_expected_stock at replenishment or sales_velocity-derived stock requirement as safety stock
    .withColumn('safety_stock', f.expr('CASE WHEN min_expected_stock < ss_sales_velocity THEN min_expected_stock ELSE ss_sales_velocity END'))
    .withColumn('safety_stock', f.expr('CASE WHEN replenishment_flag != 1 THEN 0 ELSE safety_stock END'))
    .withColumn('safety_stock', f.expr('COALESCE(safety_stock,0)'))
    .withColumn('safety_stock', f.expr('CASE WHEN safety_stock=0 THEN min_expected_stock ELSE safety_stock END'))
  
    .select(
      'date',
      'store_id',
      'sku',
      'product_category',
      'total_sales_units', 
      'on_hand_inventory_units',
      'replenishment_units', 
      'replenishment_flag',
      'phantom_inventory',
      'estimated_on_hand_inventory',  
      'prior_inventory',
      'rolling_min_expected_stock', 
      'min_expected_stock',
      'daily_sales_units', 
      'safety_stock', 
      'units_on_order',
      'units_in_transit',
      'units_in_dc',
      'lead_time_in_transit',
      'lead_time_in_dc',
      'lead_time_on_order'
      )
  )

display(inventory_safety_stock)

# COMMAND ----------

# MAGIC %md With a safety stock level defined for each store-SKU, we can now identify dates where:</p>
# MAGIC
# MAGIC 1. The on-hand inventory is less than the safety stock level (*on_hand_less_than_safety_stock*)
# MAGIC 2. The requested replenishment units are not sufficient to meet safety stock requirements (*insufficient_inventory_pipeline_units*)
# MAGIC 3. The inventory pipeline is one day away from not being able to fulfill stocking requirements (*insufficient_lead_time*)
# MAGIC
# MAGIC Each of these conditions represents an inventory management problem which requires addressing. The first two of these conditions may be calculated as follows:

# COMMAND ----------

# DBTITLE 1,Identify Insufficient Inventory On-Hand & In-Pipeline Events
inventory_safety_stock_alert = (
  inventory_safety_stock
  
  # alert 1 - estimated on-hand inventory is less than safety stock
  .withColumn('on_hand_less_than_safety_stock', f.expr('CASE WHEN estimated_on_hand_inventory <= safety_stock THEN 1 ELSE 0 END'))
  
  # alert 2 - inventory in pipeline is not sufficient to reach the safety stock levels
  .withColumn('insufficient_inventory_pipeline_units', f.expr('''
    CASE
      WHEN  (on_hand_less_than_safety_stock = 1) AND 
            (units_on_order + units_in_transit + units_in_dc != 0) AND
            ((units_on_order + units_in_transit + units_in_dc) < (safety_stock - estimated_on_hand_inventory))
         THEN 1
      ELSE 0
      END'''))
  )

display(inventory_safety_stock_alert)

# COMMAND ----------

# MAGIC %md We can identify dates where there was insufficient lead time in the pipeline to meet expected demand, the last of our three events, as follows:

# COMMAND ----------

# DBTITLE 1,Identify Insufficient Lead Time Events
# calculate lead times associated with inventory records
inventory_safety_stock_with_lead_times = (
  
  inventory_safety_stock_alert
  
    # lead time values at store-sku level for various stages 
    .withColumn('lead_time_in_transit', f.expr('COALESCE(lead_time_in_transit,0)'))
    .withColumn('lead_time_on_order', f.expr('COALESCE(lead_time_on_order,0)'))
    .withColumn('lead_time_in_dc', f.expr('COALESCE(lead_time_in_dc,0)'))

    # considering lead time only if estimated on-hand inventory and inventory in pipeline meet the safety stock levels
    .withColumn('lead_time', f.expr('''
       CASE
         WHEN on_hand_less_than_safety_stock = 1 AND
              (units_on_order + units_in_transit + units_in_dc != 0) AND
              ((units_on_order + units_in_transit + units_in_dc) >= (safety_stock - estimated_on_hand_inventory)) 
           THEN GREATEST(
                 COALESCE(lead_time_in_transit,0),
                 COALESCE(lead_time_on_order,0),
                 COALESCE(lead_time_in_dc,0)
                 )+1
         ELSE null 
         END'''))
  )

# identify lead time problems
lead_time_alerts = (
  
  inventory_safety_stock_with_lead_times.alias('a')
  
    # self join to get the previous lead time (most recent one) for the inventory pipeline
    .join(
      (inventory_safety_stock_with_lead_times
          .filter(
            f.expr('lead_time Is Not Null') # considering only non-null records
             ).alias('b')
        ), 
      on=f.expr('a.store_id=b.store_id AND a.sku=b.sku AND a.date > b.date'), 
      how='leftouter'
      )
    .groupBy('a.store_id','a.sku','a.date')
      .agg(
          f.max('a.lead_time').alias('lead_time'),
          f.max('b.date').alias('lead_date'), # day on which the lead time was assigned to the inventory pipeline
          f.max('b.lead_time').alias('prev_lead_time') # lead time assigned to the inventory pipeline
        )
  
    # flag is raised if difference in current date and lead date (from above) is greater than the lead time assigned (prev_lead_time)
    .withColumn('date_diff', f.expr('DATEDIFF(date, lead_date)'))
    .withColumn('insufficient_lead_time', f.expr('''
      CASE
        WHEN lead_time IS NULL AND (prev_lead_time - date_diff) <= 0 THEN 1
        ELSE 0
        END
      '''))
    .select(
      'date',
      'store_id',
      'sku',
      'insufficient_lead_time'
      )
  .join(
    inventory_safety_stock_alert,
    on=['store_id','sku','date']
    )
  )


display(lead_time_alerts)

# COMMAND ----------

# MAGIC %md Combining these conditions, we might flag out of stock situations that require attention as follows:

# COMMAND ----------

# DBTITLE 1,Consolidated Events
consolidated_oos_alerts = (
  lead_time_alerts
    .withColumn('alert_indicator', f.expr('''
      CASE
        WHEN on_hand_less_than_safety_stock = 1 AND insufficient_inventory_pipeline_units = 1 THEN 1 
        WHEN on_hand_less_than_safety_stock = 1 AND insufficient_inventory_pipeline_units != 1 AND insufficient_lead_time = 1 THEN 1
        ELSE 0
        END'''))
    .select(
      'date',
      'store_id',
      'sku',
      'product_category',
      'total_sales_units',
      'daily_sales_units',
      'alert_indicator'
      )
  )

display(consolidated_oos_alerts)

# COMMAND ----------

# MAGIC %md ## Step 4: Identify Zero Sales Issues
# MAGIC
# MAGIC Not every product sells each day in each store location. But when a product goes unsold for a long period of time, it might be wise for someone to verify it is still in inventory.  
# MAGIC
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/osa_tredence_zeroscan.png' width=75%>
# MAGIC
# MAGIC The challenge is that what constitutes a *long* period of time.  Some products move relatively quickly while many products move only occasionally.  For a product that sales hundreds of units daily, we might suspect an inventory problem if we suddenly see no sales on a given day.  For a product that moves only a few units a month, a few days or even a few weeks with no units sold may be nothing to be concerned with.
# MAGIC
# MAGIC With that in mind, we need to examine the number of days with zero units sold for each store-SKU combination relative to the total days a product is available for sale in order to understand the probability a product will experience a zero-sales day:

# COMMAND ----------

# DBTITLE 1,Calculate Ratio of Zero Sales Days to Total Sales Days by Store-SKU
# calculate ratio of total days of zero sales and total days on shelf across observed period
zero_sales_totals = ( 
  inventory
    .withColumn('total_zero_sales_days', f.expr('CASE WHEN total_sales_units == 0 THEN 1 ELSE 0 END'))
    .withColumn('total_days', f.expr('1'))
    .groupBy(['store_id', 'sku'])
      .agg(
        f.sum('total_days').alias('total_days'),
        f.sum('total_zero_sales_days').alias('total_zero_sales_days')
        )
    .withColumn('zero_sales_day_probability', f.expr('total_zero_sales_days / total_days'))
    )

display(zero_sales_totals)

# COMMAND ----------

# MAGIC %md With these ratios in the back of our minds, we can now examine the number of consecutive days a product has experienced zero-units sold:

# COMMAND ----------

# DBTITLE 1,Calculate Consecutive Zero Sales Days
zero_sales_days = (
  
  inventory 
  
    # flag the occurrence of first zero sales day in a series
    .withColumn('sales_change_flag', f.expr('''
        CASE 
          WHEN total_sales_units=0 AND LAG(total_sales_units,1) OVER(PARTITION BY store_id, sku ORDER BY date) != 0 THEN 1 
          ELSE 0 
          END''')) 
  
    # count number of zero sales day series to date (associates records with a given series)
    .withColumn('zero_sales_flag_rank', f.expr('SUM(sales_change_flag) OVER(PARTITION BY store_id, sku ORDER BY date)')) 
  
    # flag all zero sales days
    .withColumn('sales_change_flag_inv', f.expr('CASE WHEN total_sales_units = 0 THEN 1 ELSE 0 END')) 
  
    # count consecutive zero sales days (counter resets with a non-zero sales instance)
    .withColumn('total_days_wo_sales', f.expr('SUM(sales_change_flag_inv) OVER(PARTITION BY store_id, sku, zero_sales_flag_rank ORDER BY date)'))
    .withColumn('total_days_wo_sales', f.expr('CASE WHEN total_sales_units != 0 THEN 0 ELSE total_days_wo_sales END'))
    
    .select(
      'date',
      'store_id',
      'sku',
      'total_sales_units',
      'zero_sales_flag_rank',
      'sales_change_flag_inv',
      'total_days_wo_sales'
      )
  )

display(zero_sales_days.orderBy('store_id','sku','date'))

# COMMAND ----------

# MAGIC %md We know the probability that a product will experience a zero-sales date in a given store location.  Using this value, we can calculate the probability of a product would realistically experience consecutive zero-sales days using a simple function.  When that probability reaches or exceeds a particular threshold (set here at 5% to reflect a fairly low risk tolerance), we might take that as a signal to send someone to inspect the inventory:

# COMMAND ----------

# DBTITLE 1,Calculate Cumulative Probability of Zero Sales Event
zero_sales_inventory = (
  zero_sales_days
    .join(zero_sales_totals.alias('prob'), on=['store_id', 'sku'], how = 'leftouter')
    .withColumn('zero_sales_probability', f.expr('pow(zero_sales_day_probability, total_days_wo_sales)'))
    .withColumn('no_sales_flag', f.expr('CASE WHEN zero_sales_probability < 0.05 THEN 1 ELSE 0 END'))
  )

display(zero_sales_inventory.orderBy('store_id','sku','date'))

# COMMAND ----------

# MAGIC %md ##Step 5: Identify Alert Conditions
# MAGIC
# MAGIC We now have identified several conditions that require attention.  We first identified problematic phantom inventory conditions and then identified inventory below safety stock levels.  Finally, we identified days with zero sales events likely to be a result of an inventory issue.  In this last step, we'll consolidate all this information to build a set with which we can more clearly identify inventory dates requiring attention from analysts:

# COMMAND ----------

# DBTITLE 1,Combining all flags
all_alerts = (
  consolidated_oos_alerts.alias('oos') # OOS alert
    .join(phantom_inventory.alias('pi'), on=['store_id','sku','date'], how='leftouter') # phantom inventory indicator
    .join(zero_sales_inventory, on=['store_id','sku','date'], how='leftouter') # zero sales alert
    .selectExpr(
      'date',
      'store_id',
      'sku',
      'product_category',
      'oos.total_sales_units',
      'oos.alert_indicator as oos_alert',
      'oos.daily_sales_units',
      'no_sales_flag as zero_sales_flag',
      'phantom_inventory',
      'phantom_inventory_ind'
      )
  )

display(all_alerts)

# COMMAND ----------

# DBTITLE 1,Persist Flagged Inventory Data for Future Use
(
  all_alerts
    .repartition(sc.defaultParallelism)
    .write
      .format('delta')
      .mode('overwrite')
      .option('overwriteSchema', 'true')
      .saveAsTable('inventory_flagged')
   )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
