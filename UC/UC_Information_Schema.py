# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog - Exploring the Information Schema
# MAGIC 
# MAGIC The information schema is provided in each catalog other than the `hive_metastore` catalog. Each information schema will reference only the catalog that it belongs to. 
# MAGIC 
# MAGIC There is one exception for the `system` catalog that returns information about objects across all catalogs within the metastore. Metastore is the top level object in Unity Catalog, not to be confused with the Hive Metastore. Note that by default the user will only be able to view data on tables that the user is privileged to interact with.   
# MAGIC 
# MAGIC 
# MAGIC <br></br>
# MAGIC <img src="https://docs.databricks.com/_images/information-schema.png" width=800/>

# COMMAND ----------

dbutils.widgets.text("catalog_name", "system")
dbutils.widgets.text("schema_name", "information_schema")


catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

spark.sql("USE CATALOG {}".format(catalog_name))

# COMMAND ----------

spark.sql("USE SCHEMA {}".format(schema_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Descriptions 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catalog Privileges 
# MAGIC 
# MAGIC A table containing the principals which have access to each catalog. This data can be used to audit access and govern data, and has a a many to one relationship with the `catalogs` table. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM catalog_privileges

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catalogs
# MAGIC 
# MAGIC Contains information about each catalog in the metastore that the user has access to. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM catalogs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Information Schema Catalog Name
# MAGIC 
# MAGIC Single row and single column name where the value is the name of the catalog that the current `information_schema` is stored in. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM information_schema_catalog_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Constraints
# MAGIC 
# MAGIC 
# MAGIC Check Constraints is currently reserved for future implementations (as of 8/31/2022). The purpose is to describe the [check constraints](https://docs.databricks.com/delta/delta-constraints.html#check-constraint) on tables. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM check_constraints

# COMMAND ----------

# MAGIC %md
# MAGIC ### Columns 
# MAGIC 
# MAGIC Describes columns for tables and views in the metastore. This can be used for various purposes.  
# MAGIC 
# MAGIC The primary keys for this table are: `TABLE_CATALOG`, `TABLE_SCHEMA`, `TABLE_NAME`, `COLUMN_NAME`.  
# MAGIC The foreign keys for this table are: `TABLE_CATALOG`, `TABLE_SCHEMA`, `TABLE_NAME`.  
# MAGIC The unique keys for this table are: `TABLE_CATALOG`, `TABLE_SCHEMA`, `TABLE_NAME`, `ORDINAL_POSITION`
# MAGIC 
# MAGIC There is a many to one relationship between columns and tables.  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Referential Constraints
# MAGIC 
# MAGIC Describes the relationship between foreign and primary keys. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM referential_constraints

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Schema Privileges  
# MAGIC 
# MAGIC Lists principals which have privileges on the schemas in the catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM schema_privileges

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Schemata  
# MAGIC Contains metadata and descriptions of the schemas in a catalog/metastore.  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM schemata

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Table Privileges  
# MAGIC 
# MAGIC Lists principals which have privileges on the tables and views in the catalog.  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_privileges

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Tables  
# MAGIC 
# MAGIC Describes tables and views defined within the catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tables 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Views  
# MAGIC 
# MAGIC Describes view specific information about the views in the catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM views

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Constraint Table Usage
# MAGIC 
# MAGIC Describes which tables have constraints and the name of those constraints. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM constraint_table_usage

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Key Column Usage
# MAGIC 
# MAGIC Similar to constraint_table_usage but suplies additional information about the columns specified in the constraint. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM key_column_usage

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Routines
# MAGIC 
# MAGIC TBD

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM routines

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Routine Privileges 
# MAGIC 
# MAGIC TBD

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM routine_privileges

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Routine Parameters
# MAGIC 
# MAGIC TBD

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM routine_parameters

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Routine Columns
# MAGIC TBD

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM routine_columns

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Constraint Column Usage
# MAGIC 
# MAGIC Similar to key_column_usage but without the column information. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM constraint_column_usage

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Table Constraints 
# MAGIC 
# MAGIC Table constraints with additional information about he constraint. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_constraints

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Queries 

# COMMAND ----------

# MAGIC %md
# MAGIC ### All columns that contain substring 

# COMMAND ----------

# MAGIC %sql
# MAGIC ------ 
# MAGIC -- Find all columns in a schema with substring values 
# MAGIC --
# MAGIC ------
# MAGIC SET var.col_string = datetime ; -- this is the variable with column sub string value
# MAGIC 
# MAGIC SELECT * FROM columns WHERE column_name LIKE '%${var.col_string}%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Relationships

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a set of test tables first 
# MAGIC 
# MAGIC 
# MAGIC -- fact table
# MAGIC DROP TABLE IF EXISTS rac_demo_catalog.rac_demo_db.fact_table; 
# MAGIC 
# MAGIC CREATE TABLE rac_demo_catalog.rac_demo_db.fact_table 
# MAGIC (
# MAGIC `id` int not null,
# MAGIC first_name string,
# MAGIC last_name string
# MAGIC ); 
# MAGIC 
# MAGIC ALTER TABLE rac_demo_catalog.rac_demo_db.fact_table ADD CONSTRAINT ft_pk PRIMARY KEY (`id`); 
# MAGIC 
# MAGIC 
# MAGIC -- secondary dim tables
# MAGIC DROP TABLE IF EXISTS rac_demo_catalog.rac_demo_db.dim_table_one ;
# MAGIC 
# MAGIC CREATE TABLE rac_demo_catalog.rac_demo_db.dim_table_one 
# MAGIC (
# MAGIC id int,
# MAGIC pt_id int,
# MAGIC address string
# MAGIC ); 
# MAGIC 
# MAGIC ALTER TABLE rac_demo_catalog.rac_demo_db.dim_table_one ADD CONSTRAINT dt1_fk FOREIGN KEY (pt_id) REFERENCES rac_demo_catalog.rac_demo_db.fact_table(`id`) ; 
# MAGIC 
# MAGIC --- break
# MAGIC 
# MAGIC DROP TABLE IF EXISTS rac_demo_catalog.rac_demo_db.dim_table_two ;
# MAGIC 
# MAGIC CREATE TABLE rac_demo_catalog.rac_demo_db.dim_table_two 
# MAGIC (
# MAGIC id int,
# MAGIC pt_id int,
# MAGIC address string
# MAGIC ); 
# MAGIC 
# MAGIC ALTER TABLE rac_demo_catalog.rac_demo_db.dim_table_two ADD CONSTRAINT dt2_fk FOREIGN KEY (pt_id) REFERENCES rac_demo_catalog.rac_demo_db.fact_table(`id`) ; 
# MAGIC 
# MAGIC --- break
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS rac_demo_catalog.rac_demo_db.dim_table_three ;
# MAGIC 
# MAGIC CREATE TABLE rac_demo_catalog.rac_demo_db.dim_table_three 
# MAGIC (
# MAGIC id int,
# MAGIC pt_id int,
# MAGIC address string
# MAGIC ); 
# MAGIC 
# MAGIC ALTER TABLE rac_demo_catalog.rac_demo_db.dim_table_three ADD CONSTRAINT dt3_fk FOREIGN KEY (pt_id) REFERENCES rac_demo_catalog.rac_demo_db.fact_table(`id`) ; 
# MAGIC 
# MAGIC --- break
# MAGIC 
# MAGIC DROP TABLE IF EXISTS rac_demo_catalog.rac_demo_db.dim_table_four ;
# MAGIC 
# MAGIC CREATE TABLE rac_demo_catalog.rac_demo_db.dim_table_four 
# MAGIC (
# MAGIC id int,
# MAGIC pt_id int,
# MAGIC address string
# MAGIC ); 
# MAGIC 
# MAGIC ALTER TABLE rac_demo_catalog.rac_demo_db.dim_table_four ADD CONSTRAINT dt4_fk FOREIGN KEY (pt_id) REFERENCES rac_demo_catalog.rac_demo_db.fact_table(`id`) ; 
# MAGIC 
# MAGIC 
# MAGIC --- break
# MAGIC 
# MAGIC DROP TABLE IF EXISTS rac_demo_catalog.rac_demo_db.dim_table_five ;
# MAGIC 
# MAGIC CREATE TABLE rac_demo_catalog.rac_demo_db.dim_table_five 
# MAGIC (
# MAGIC id int not null,
# MAGIC pt_id int not null,
# MAGIC address string
# MAGIC ); 
# MAGIC 
# MAGIC ALTER TABLE rac_demo_catalog.rac_demo_db.dim_table_five ADD CONSTRAINT dt6_pk PRIMARY KEY (`id`, pt_id); 
# MAGIC ALTER TABLE rac_demo_catalog.rac_demo_db.dim_table_five ADD CONSTRAINT dt5_fk FOREIGN KEY (pt_id) REFERENCES rac_demo_catalog.rac_demo_db.fact_table(`id`) ; 
# MAGIC 
# MAGIC 
# MAGIC --- break
# MAGIC 
# MAGIC DROP TABLE IF EXISTS rac_demo_catalog.rac_demo_db.dim_table_six ;
# MAGIC 
# MAGIC CREATE TABLE rac_demo_catalog.rac_demo_db.dim_table_six 
# MAGIC (
# MAGIC id int,
# MAGIC pt_id int,
# MAGIC address string
# MAGIC ); 
# MAGIC 
# MAGIC ALTER TABLE rac_demo_catalog.rac_demo_db.dim_table_six ADD CONSTRAINT dt6_fk FOREIGN KEY (pt_id) REFERENCES rac_demo_catalog.rac_demo_db.fact_table(`id`) ; 
# MAGIC ALTER TABLE rac_demo_catalog.rac_demo_db.dim_table_six ADD CONSTRAINT dt6_fk_2 FOREIGN KEY (pt_id, id) REFERENCES rac_demo_catalog.rac_demo_db.dim_table_five(`id`, pt_id) ; 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW rac_demo_catalog.rac_demo_db.table_relationships_view
# MAGIC AS 
# MAGIC SELECT DISTINCT ptc.table_catalog as p_table_catalog -- table 1
# MAGIC   , ptc.table_schema as p_table_schema
# MAGIC   , ptc.table_name as p_table_name
# MAGIC   , stc.table_catalog as s_table_catalog -- table 2
# MAGIC   , stc.table_schema as s_table_schema
# MAGIC   , stc.table_name as s_table_name
# MAGIC   , rc.constraint_name -- foreign key
# MAGIC   , pccu.column_name as constraint_column_name
# MAGIC   , rc.unique_constraint_name -- primary key
# MAGIC   , sccu.column_name as unique_constraint_column_name
# MAGIC 
# MAGIC FROM referential_constraints rc 
# MAGIC INNER JOIN table_constraints ptc on ptc.constraint_name =  rc.constraint_name 
# MAGIC INNER JOIN table_constraints stc on stc.constraint_name =  rc.unique_constraint_name 
# MAGIC INNER JOIN constraint_column_usage pccu on pccu.constraint_name = rc.constraint_name 
# MAGIC INNER JOIN constraint_column_usage sccu on sccu.constraint_name = rc.unique_constraint_name 
# MAGIC ;
# MAGIC 
# MAGIC SELECT * FROM rac_demo_catalog.rac_demo_db.table_relationships_view;

# COMMAND ----------

# DBTITLE 1,Visualize the table relationships as a network graph
import matplotlib.pyplot as plt
import subprocess 

def install(name):
    subprocess.call(['pip', 'install', name])

install('networkx')
# https://networkx.org/documentation/stable//reference/generated/networkx.drawing.nx_pylab.draw_networkx.html#networkx.drawing.nx_pylab.draw_networkx


import networkx as nx

df = spark.sql("SELECT * FROM rac_demo_catalog.rac_demo_db.table_relationships_view")
pdf = df.toPandas()

G = nx.from_pandas_edgelist(pdf, 
                            source = 'p_table_name',
                            target = 's_table_name'
      )

plt.figure(3,figsize=(10,10)) 
pos=nx.spring_layout(G,k=0.5) 
nx.draw_networkx(G,pos)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Counts by Table
# MAGIC 
# MAGIC This can be used to figure out which tables are wide/narrow. Adding a `HAVING num_columns >= x` will allow you to filter for tables with specific column counts. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_catalog, table_name, table_schema, count(1) as num_columns
# MAGIC 
# MAGIC FROM system.information_schema.columns
# MAGIC 
# MAGIC GROUP BY table_name, table_schema, table_catalog
# MAGIC 
# MAGIC ORDER BY 4 desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Count by Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_catalog, table_schema, count(1) as num_tables
# MAGIC 
# MAGIC FROM system.information_schema.tables
# MAGIC 
# MAGIC -- WHERE table_schema = 'my_schema'
# MAGIC 
# MAGIC GROUP BY table_catalog, table_schema
# MAGIC 
# MAGIC ORDER BY 3 DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Constraints - Key Column Count 
# MAGIC 
# MAGIC i.e. which tables have composite keys

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tc.constraint_catalog
# MAGIC , tc.constraint_schema
# MAGIC , tc.constraint_name
# MAGIC , tc.constraint_type
# MAGIC , tc.table_catalog
# MAGIC , tc.table_schema
# MAGIC , tc.table_name
# MAGIC , count(kcu.column_name) as key_column_count
# MAGIC 
# MAGIC FROM table_constraints tc
# MAGIC 
# MAGIC INNER JOIN key_column_usage kcu on tc.constraint_catalog = kcu.constraint_catalog 
# MAGIC             AND tc.constraint_schema = kcu.constraint_schema
# MAGIC             AND tc.constraint_name = kcu.constraint_name
# MAGIC 
# MAGIC 
# MAGIC GROUP BY tc.constraint_catalog
# MAGIC , tc.constraint_schema
# MAGIC , tc.constraint_name
# MAGIC , tc.constraint_type
# MAGIC , tc.table_catalog
# MAGIC , tc.table_schema
# MAGIC , tc.table_name
# MAGIC 
# MAGIC ORDER BY 8 desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catalog Objects Created by Date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_catalog, table_schema, table_name, table_type, 1 as cnt
# MAGIC FROM tables
# MAGIC WHERE table_schema != 'information_schema' 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tables 
# MAGIC where table_catalog='sumit_uc_demo' and table_schema = 'retail'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Types of Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_catalog, table_schema, table_name, table_type, 1 as cnt
# MAGIC FROM tables
# MAGIC WHERE table_schema != 'information_schema' 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Who is Creating Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT table_catalog, table_schema, table_name, created_by, 1 as cnt
# MAGIC FROM tables
# MAGIC WHERE table_schema != 'information_schema' 

# COMMAND ----------


