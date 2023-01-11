# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access and Management - AWS 
# MAGIC 
# MAGIC In this notebook we will walk through one of the most popular (potentially a universal best practice) for accessing data in [S3](https://aws.amazon.com/s3/). This notebook hopes to show the controls that should be put in place for companies that require a sophisticated data governance model. Not all organizations require this level of data management as some companies may have general data requirements that apply to the majority of users, while other orgs may have various levels of access for each individual users. This notebook applies to the more complex data access requirements. As an example alternative, some customers are completly fine using Databricks mounts even though all users will have the same data access.  
# MAGIC 
# MAGIC We will complete the following items (not necessarily in this order). 
# MAGIC - S3 Access  Overview
# MAGIC - Create a Database with and without Location
# MAGIC   - This is a best practice and allows for a better experience when managing tables
# MAGIC - Create a Delta table
# MAGIC   - Managed tables (with location) will automatically register in the database
# MAGIC - Manage access to the delta table
# MAGIC   - READ, WRITE, Row Level Security, and Column Level Security 
# MAGIC - Show how to enforce secret scope ACLs  
# MAGIC - Show how to enforce cluster ACLs  
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Please note that while this notebook is Python we will write as must of the code in SQL (`spark.sql()` commands) in order to demonstrate these patterns to the broadest set of users. 
# MAGIC 
# MAGIC [Secret Access Control](https://docs.databricks.com/security/access-control/secret-acl.html#secret-access-control) and [Cluster Access Control](https://docs.databricks.com/security/access-control/cluster-acl.html) are to very important features required to ensure fine grained control over your data platform.  
# MAGIC 
# MAGIC General overview of cloud storage access:
# MAGIC - AWS  
# MAGIC   - [Instance profiles](https://docs.databricks.com/administration-guide/cloud-configurations/aws/instance-profiles.html#step-5-add-the-instance-profile-to-databricks) attached to a cluster then allow users to attach to specific clusters  
# MAGIC - Azure  
# MAGIC   - Service principals should be used to access ADLS Gen2 directly and users should be granted specific permissions to the secret scope containing the service principles. Typically this can be done at a cluster setting to reduce complexity.  
# MAGIC - GCP 
# MAGIC   - You should use service accounts associated to clusters and control access at the cluster level. 
# MAGIC - Databricks SQL
# MAGIC   - Databricks SQL connects to the Delta Lake using a single authentication mechanism (Instance Profile / Service Principle). This means that principle should have access to all the data with the lowest requried priviledge level. Once this is set then users should be controlled using [Table ACLs](https://docs.databricks.com/security/access-control/table-acls/index.html).  
# MAGIC   - Lowest requried priviledge level means that if all DBSQL users only need READ access then the account should be set to that. But if some users need manage or write access then the principle will need that level of access.  
# MAGIC   
# MAGIC   
# MAGIC Resources:
# MAGIC - [Access S3 Directly](https://docs.databricks.com/administration-guide/cloud-configurations/aws/instance-profiles.html) using an instance profile
# MAGIC - Use [Field Eng](https://e2-demo-field-eng.cloud.databricks.com/) workspace.  

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "")
database_name = dbutils.widgets.get("DatabaseName")
dbutils.widgets.text("Directory", "ryan.chynoweth@databricks.com")
directory = dbutils.widgets.get("Directory")
storage_name = "oetrta"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cluster Configuration 
# MAGIC 
# MAGIC I am using cluster configurations for S3 authentication using an instance profile. See the image below. Because of the instance profile the below command works. 
# MAGIC 
# MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/InstanceProfile.png" width=400/>
# MAGIC   
# MAGIC 
# MAGIC When we do this then all people who can connect to the cluster have access to these credentials. This is where [cluster ACLs](https://docs.databricks.com/security/access-control/cluster-acl.html) come into play. Here we have the ability to set up different clusters with various levels of data access. 
# MAGIC 
# MAGIC In addition to cluster access controls, admins have the ability to restrict users who can access the instance profile itself which can be leveraged as another control over permissions.  

# COMMAND ----------

# list files
dbutils.fs.ls("s3a://{}/".format(storage_name))

# COMMAND ----------

# DBTITLE 1,Create Directory for new Database
data_path = "s3a://{}/{}/{}".format(storage_name, directory, database_name)


reset_dir = False
if reset_dir:
  dbutils.fs.rm(data_path, True)
  
dbutils.fs.mkdirs(data_path)

# COMMAND ----------

# should be empty after we first create it. There may be tables in it if I didn't delete them
dbutils.fs.ls(data_path)

# COMMAND ----------

# DBTITLE 1,Create an UNMANAGED external table by providing it an abfss location 
spark.sql("""CREATE TABLE test_table
  USING DELTA
  LOCATION '{}/test_table'
  AS SELECT * FROM {}.application_output
  """.format(data_path, database_name)
         )

# COMMAND ----------

dbutils.fs.ls(data_path+"/test_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Access Control 
# MAGIC 
# MAGIC With [Table Access Control](https://docs.databricks.com/security/access-control/table-acls/table-acl.html) and cluster ACLs you can restrict users to only using the tables within the hive metastore. With table access you should set cluster ACLs so that users must use a cluster with table access enabled appropriately. 
# MAGIC 
# MAGIC Direct access described here can be used with table access. 
# MAGIC 
# MAGIC Please review permission levels [here](https://docs.databricks.com/security/access-control/table-acls/object-privileges.html#privileges.

# COMMAND ----------

# DBTITLE 1,Grant read access to the table
# spark.sql("""GRANT SELECT ON TABLE test_table TO `TestDataScienceGroup` """) ## MUST HAVE TABLE ACCESS ENABLED ON THE CLUSTER OTHERWISE THIS ERRORS OUT. Keeping here for demo purposes

# COMMAND ----------

# DBTITLE 1,Must drop table and delete dir because it is an UNMANAGED table
spark.sql("drop table if exists test_table")
dbutils.fs.rm(data_path+"/test_table", True)

# COMMAND ----------

# DBTITLE 1,Create a database with location - allows us to have MANAGED external tables
spark.sql("""CREATE DATABASE IF NOT EXISTS {}
             LOCATION '{}' """.format(database_name, data_path)
)


# COMMAND ----------

spark.sql(""" USE {}""".format(database_name))

# COMMAND ----------

# DBTITLE 1,Same code as above but without the location
spark.sql("""CREATE TABLE test_table
  USING DELTA
  AS SELECT * FROM {}.application_output
  """.format(database_name)
         )

# COMMAND ----------

# DBTITLE 1,Stores it in the database location
dbutils.fs.ls(data_path)

# COMMAND ----------

# DBTITLE 1,This helps automatically add it to hive without an explicit "CREATE TABLE" command
# MAGIC %sql
# MAGIC select * from test_table

# COMMAND ----------

display(
spark.sql("""
SELECT * FROM {}.test_table
""".format(database_name))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row and column level security 
# MAGIC 
# MAGIC This level of security can be accomplished by using [dynamic view functions](https://docs.databricks.com/security/access-control/table-acls/object-privileges.html#dynamic-view-functions). This allows us to create views with specific columns or filters to hide data dynamically using `current_user()` and `is_member()` functions.  
# MAGIC 
# MAGIC Below is an example where I fully restrict users from seeing a number of columns by not including them in the view but then I can dynamically show values based on their group assignment.  

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW test_view AS 
# MAGIC 
# MAGIC SELECT device_type, device_id, predicted_device_operational_status, -- only select columns users can see
# MAGIC CASE WHEN is_member('TestDataScienceGroup') THEN reading_1 ELSE 'Redacted' END AS reading_1 -- use a case statement to dynamically show a column
# MAGIC 
# MAGIC FROM test_table
# MAGIC WHERE is_member('TestDataScienceGroup')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test_view

# COMMAND ----------

# DBTITLE 1,Flip the case statement so column doesn't show
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW test_view AS 
# MAGIC 
# MAGIC SELECT device_type, device_id, predicted_device_operational_status, -- only select columns users can see
# MAGIC CASE WHEN is_member('TestDataScienceGroup') THEN 'Redacted' ELSE reading_1 END AS reading_1 -- use a case statement to dynamically show a column
# MAGIC 
# MAGIC FROM test_table
# MAGIC WHERE is_member('TestDataScienceGroup')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test_view

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW test_view AS 
# MAGIC 
# MAGIC SELECT device_type, device_id, predicted_device_operational_status, -- only select columns users can see
# MAGIC CASE WHEN is_member('TestDataScienceGroup') THEN reading_1 ELSE 'Redacted' END AS reading_1 -- use a case statement to dynamically show a column
# MAGIC 
# MAGIC FROM test_table
# MAGIC WHERE NOT is_member('TestDataScienceGroup')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test_view -- should see no data since I am a member of the TestDataScienceGroup 

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS test_table")

# COMMAND ----------

# DBTITLE 1,Our "test_table" should be gone from the directory because it is a MANAGED table
dbutils.fs.ls(data_path)

# COMMAND ----------


