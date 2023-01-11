# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access and Management - Azure
# MAGIC 
# MAGIC In this notebook we will walk through one of the most popular (potentially a universal best practice) for accessing data in Azure Data Lake Storage Gen2. This notebook hopes to show the controls that should be put in place for companies that require a sophisticated data governance model. Not all organizations require this level of data management as some companies may have general data requirements that apply to the majority of users, while other orgs may have various levels of access for each individual users. This notebook applies to the more complex data access requirements. As an example alternative, some customers are completly fine using Databricks mounts even though all users will have the same data access.  
# MAGIC 
# MAGIC We will complete the following items (not necessarily in this order). 
# MAGIC - ADLS Access  Overview
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
# MAGIC - [Access ADLS Gen2 Directly](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access#access-adls-gen2-directly) using a service principal

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "")
database_name = dbutils.widgets.get("DatabaseName")
dbutils.widgets.text("Directory", "")
directory = dbutils.widgets.get("Directory")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Secret Scope ACLs
# MAGIC 
# MAGIC In the next cell we use a Databricks scope to securely store and use sensitive information or environment variables. To create a Secret Scope and add secrets to the scope users need to use the [Databricks REST API](https://docs.databricks.com/dev-tools/api/latest/secrets.html#secrets-api) or the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html). In this example I will use the Databricks CLI. I will assume that the configuration of the CLI has already occurred using a PAT.     
# MAGIC 
# MAGIC 1. Create a secret scope.  
# MAGIC   ```
# MAGIC   databricks secrets create-scope --scope <SCOPE NAME>
# MAGIC   ``` 
# MAGIC 
# MAGIC 1. Add a secret to the scope.  
# MAGIC   ```
# MAGIC   databricks secrets put --scope <SCOPE NAME> --key <SECRET KEY NAME> --string-value <SECRET VALUE>
# MAGIC   ```
# MAGIC   
# MAGIC 1. Grant a user/group the ability to READ and LIST secrets from the scope. Possible permissions are READ, WRITE, MANAGE  
# MAGIC   ```
# MAGIC   databricks secrets put-acl --scope <SCOPE NAME> --principal <user i.e. ryan.chynoweth@databricks.com OR group> --permission READ
# MAGIC   ```
# MAGIC   
# MAGIC   It is important to note that Databricks admins will have MANAGE permissions for all secret scopes in the workspace. So non-admin users will not have access to the scope you create unless explicitly granted. In the scenario above I would have created a scope and added a secret, then gave someone permission to READ the scope. Since we are using service principals to authenticate against ADLS Gen2, we want to ensure that only specific people have access to the credentials. It would be a best practice to use groups to manage these ACLs.  
# MAGIC   
# MAGIC #### Summary
# MAGIC 
# MAGIC 1. Use secret scopes and manage permissions to the scopes at a group level.  
# MAGIC 1. Use cluster policies and cluster ACLs to restrict which users can connect to which clusters.  
# MAGIC 1. Ideally 1 and 2 would be managed using groups where all users would have same access to secrets and clusters. 
# MAGIC 1. Allow cluster level or session level configuration of authentication to ADLS

# COMMAND ----------

storage_account_name = dbutils.secrets.get("rac_scope", "oneenvstoragename")
container_name = dbutils.secrets.get("rac_scope", "oneenvcontainer")
client_id = dbutils.secrets.get("rac_scope", "oneenvclientid")
client_secret = dbutils.secrets.get("rac_scope", "oneenvclientsecret")
tenant_id = dbutils.secrets.get("rac_scope", "oneenvtenantid")

# COMMAND ----------

# DBTITLE 1,Notebook configs (see next cell) - we want to use cluster configs
# # Set configs

# spark.conf.set("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(storage_account_name), "OAuth")

# spark.conf.set("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(storage_account_name), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

# spark.conf.set("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(storage_account_name), client_id)

# spark.conf.set("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(storage_account_name), client_secret )

# spark.conf.set("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(storage_account_name),  "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cluster Configuration 
# MAGIC 
# MAGIC I am using cluster configurations for my ADLS authentication.  
# MAGIC 
# MAGIC In the Spark configuration of the cluster configuration you can input the following: 
# MAGIC ```
# MAGIC fs.azure.account.auth.type OAuth
# MAGIC fs.azure.account.oauth.provider.type org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
# MAGIC fs.azure.account.oauth2.client.id {{secrets/<SCOPE NAME>/<SECRET KEY NAME>}}
# MAGIC fs.azure.account.oauth2.client.secret {{secrets/<SCOPE NAME>/<SECRET KEY NAME>}}
# MAGIC fs.azure.account.oauth2.client.endpoint https://login.microsoftonline.com/<YOUR TENANT ID>/oauth2/token
# MAGIC ```
# MAGIC 
# MAGIC Where `{{secrets/<SCOPE NAME>/<SECRET KEY NAME>}}` is our ability to reference secrets stored in scopes.  
# MAGIC 
# MAGIC When we do this then all people who can connect to the cluster have access to these credentials. This is where [cluster ACLs](https://docs.databricks.com/security/access-control/cluster-acl.html) come into play. Here we have the ability to set up different clusters with various. In union with cluster policies you can enforce that these Spark configurations are set in a particular way so that users still have the ability to create their own clusters. 
# MAGIC 
# MAGIC As with Secret ACLs, admins should utilize groups as much as possible when managing access to the various resources.  
# MAGIC 
# MAGIC In my opinion setting these at the cluster level is a better as it is a better experience for the developer to not have to set configurations each time. Additionally, there is some limitations to the session level configs that I am not fully aware of, but one being `CREATE DATABASE <database> LOCATION '/path/database'` where this does not work if you provide a location for the database.  
# MAGIC 
# MAGIC Session Level Authentication is done with the following Python/Scala inside a notebook (see above):  
# MAGIC ```
# MAGIC spark.conf.set("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(storage_account_name), "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(storage_account_name), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(storage_account_name), client_id)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(storage_account_name), client_secret )
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(storage_account_name),  "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id))
# MAGIC ```

# COMMAND ----------

# list files
dbutils.fs.ls("abfss://{}@{}.dfs.core.windows.net/".format(container_name, storage_account_name))

# COMMAND ----------

df = spark.read.format('delta').load("abfss://my_container@my_storage.dfs.core.windows.net/*/databases/*")

# /dbfs/mnt/storage_account_name/container_name
df = spark.read.format('delta').load("/dbfs/mnt/*/*/*/databases/*")

# COMMAND ----------

# DBTITLE 1,Create Directory for new Database
data_path = "abfss://{}@{}.dfs.core.windows.net/{}/databases/{}".format(container_name, storage_account_name, directory, database_name)


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

# MAGIC %md
# MAGIC ### Cluster Policies 
# MAGIC 
# MAGIC A good practice would be to leverage cluster policies to force users to have specific configurations for the clusters they are using for jobs and interactive usage. Below is a sample of a cluster policy that can be used for a use case tag and access configurations.  
# MAGIC 
# MAGIC ```
# MAGIC {
# MAGIC   "custom_tags.UseCase": {
# MAGIC     "type": "fixed",
# MAGIC     "value": "<<USE CASE TAG>>"
# MAGIC   },
# MAGIC   "spark_conf.fs.azure.account.oauth2.client.id": {
# MAGIC     "type": "fixed",
# MAGIC     "value": "{{secrets/<<SCOPE NAME>>/<<SECRET NAME>>}}"
# MAGIC   },
# MAGIC   "spark_conf.fs.azure.account.oauth.provider.type": {
# MAGIC     "type": "fixed",
# MAGIC     "value": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
# MAGIC   },
# MAGIC   "spark_conf.fs.azure.account.oauth2.client.endpoint": {
# MAGIC     "type": "fixed",
# MAGIC     "value": "https://login.microsoftonline.com/<<TENANT ID>>/oauth2/token"
# MAGIC   },
# MAGIC   "spark_conf.fs.azure.account.auth.type": {
# MAGIC     "type": "fixed",
# MAGIC     "value": "OAuth"
# MAGIC   },
# MAGIC   "spark_conf.fs.azure.account.oauth2.client.secret": {
# MAGIC     "type": "fixed",
# MAGIC     "value": "{{secrets/<<SCOPE NAME>>/<<SECRET NAME>>}}"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC This would allow users to create clusters with specific configurations! 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks SQL Access 
# MAGIC 
# MAGIC The recommended pattern for Databricks SQL is to use Table ACLs as we showed above. This is great to control access on a user or group level. 
# MAGIC 
# MAGIC In short, SQL access is configured at the workspace level with the same configurations that you would set for the clusters. It is important to note that all endpoints will have the same exact authentication against the datasets, but users access is filtered via [Table ACLs](https://docs.databricks.com/security/access-control/table-acls/index.html) and handled internally.  
# MAGIC 
# MAGIC To set the configuration for Databricks SQL you will need to navigate to the SQL Admin Console and input the above spark configurations.   
# MAGIC  
# MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/SQL Admin Console.png" />  
