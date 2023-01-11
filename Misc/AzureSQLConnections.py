# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Azure SQL / SQL Server 
# MAGIC 
# MAGIC In this notebook we attempt to cover the different options for connecting to [Azure SQL Database]() and [SQL Server](). Please note that the specialized connectors are [created and maintained by Microsoft](https://github.com/microsoft/sql-spark-connector).  
# MAGIC 
# MAGIC In general, you have the following options:
# MAGIC - [Apache Spark Connector for SQL Server and Azure SQL](https://docs.microsoft.com/en-us/sql/connect/spark/connector?view=sql-server-ver15#performance-comparison) - MUST INSTALL USING MAVEN ON YOUR CLUSTER
# MAGIC   - Spark 2.4.x: `com.microsoft.azure:spark-mssql-connector:1.0.2`
# MAGIC   - Spark 3.0.x: `com.microsoft.azure:spark-mssql-connector_2.12:1.1.0`
# MAGIC   - Spark 3.1.x: `com.microsoft.azure:spark-mssql-connector_2.12:1.2.0`
# MAGIC - [Apache Spark Connector for Azure Synapse](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/synapse-analytics)
# MAGIC   - We will not go over this connector but highlighting that there is a dedicated option for Synapse.    
# MAGIC - [Generic JDBC Connector](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting and getting secret values 

# COMMAND ----------

jdbcUsername = dbutils.secrets.get(scope = "my_scope", key = "azuresqluser")
jdbcPassword = dbutils.secrets.get(scope = "my_scope", key = "azuresqlpassword")
jdbcHostname = dbutils.secrets.get(scope = "my_scope", key = "azuresqlserver")
jdbcPort = dbutils.secrets.get(scope = "my_scope", key = "azuresqlport")
jdbcDatabase = dbutils.secrets.get(scope = "my_scope", key = "azuresqldatabase")

# COMMAND ----------

url = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};user={jdbcUsername}@{jdbcDatabase};password={jdbcPassword};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading a table from Azure SQL using the Spark Connector 
# MAGIC 
# MAGIC Note - the "dbtable" option is the table that I wish to read. If I want to write a pushdown query into SQL Server then I need to wrap it in parenthesis as shown below. 

# COMMAND ----------

df = (spark.read
        .format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", url)
        .option("dbtable", "(SELECT * FROM dbo.my_table ) as data") 
        .load()
       
       )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing data to a table from Spark to Azure SQL 

# COMMAND ----------

(df.write
 .format("com.microsoft.sqlserver.jdbc.spark")
 .mode("overwrite")
 .option("truncate", "true")
 .option("url", url)
 .option("dbtable", "dbo.my_table")
 .save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recommendations/Tips on Writing Data
# MAGIC 
# MAGIC Please refer to the [Microsoft Documentation](https://docs.microsoft.com/en-us/sql/connect/spark/connector) for exact specifications and rules. What I discuss below is opinion from a third party without insight into the actual development of the product. 
# MAGIC 
# MAGIC 1. When you are creating a new table or overwriting a table then you will be assigned a default schema based on your dataframe column types. This schema is never "ideal". A best practice would be to define your schema in the database prior to writing to the table. 
# MAGIC   - PLEASE NOTE: that if you use `overwrite` but with `truncate == false` then the table schema is lost. If you wish to retain the schema then please set `.option("truncate", "true")` as seen above. If your mode is `insert` then the existing table schema is retained. 
# MAGIC   
# MAGIC 1. All the options available in the [generic JDBC driver](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) are also available with the Mircosoft connector - plus a few additional ones that we will mention. 
# MAGIC 
# MAGIC 
# MAGIC 1. There are `user` and `password` options available if you wish to provide those outside of the URL. In our examples we insert these values in the URL string.  
# MAGIC 
# MAGIC 
# MAGIC Options that you should look into first for the generic JDBC: 
# MAGIC - batchsize
# MAGIC - isolationLevel
# MAGIC - truncate 
# MAGIC - createTableOptions: allows you to specify schema on write 
# MAGIC - customSchema: allows you to cast columns on read
# MAGIC 
# MAGIC [Additional options](https://docs.microsoft.com/en-us/sql/connect/spark/connector?view=sql-server-ver15#supported-options) that you should look into first for the Microsoft connector: 
# MAGIC - reliabilityLevel
# MAGIC   - Spark is a distributed system that has the potential of losing nodes on a cluster. This will improve the reliability of a write if set to `NO_DUPLICATES` and guarantee that if you lose an executor there won't be duplicate data in your table.  
# MAGIC - dataPoolDataSource  
# MAGIC - isolationLevel  
# MAGIC - tableLock  
# MAGIC   - Improves write performance but will lock your table from other consumers
# MAGIC - schemaCheckEnabled  
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Lastly, the syntax and compatibility between the generic JDBC and Microsoft built connector is extremely similar. The biggest difference will be performance. The Microsoft connector is built specifically for Azure SQL and SQL Server making it more performant on those two databases. The JDBC connector is purposed for DB2, MariaDB, Microsoft SQL, Oracle, and PostgreSQL.  

# COMMAND ----------

# reading using JDBC 

df = (spark.read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", "dbo.my_table")
    .load())

(df.write
 .format("jdbc")
 .mode("overwrite")
 .option("truncate", "true")
 .option("url", url)
 .option("dbtable", "dbo.my_table")
 .save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connecting via Spark SQL
# MAGIC 
# MAGIC ```
# MAGIC CREATE TEMPORARY VIEW jdbcTable
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:postgresql:dbserver",
# MAGIC   dbtable "schema.tablename",
# MAGIC   user 'username',
# MAGIC   password 'password'
# MAGIC )
# MAGIC 
# MAGIC INSERT INTO TABLE jdbcTable
# MAGIC SELECT * FROM resultTable
# MAGIC ```
