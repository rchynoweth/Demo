# Minimal Python imports 
from delta import *
from pyspark.sql import SparkSession

# this is to handle configs for demo purposes 
# A little messy but gets the job done
config_file = "delta_demos/LocalSpark/config.conf"
config = {}
with open(config_file) as f:
    lines = f.readlines()
    for l in lines:
        k = l.split('=')[0]
        v = l.split('=')[1].strip("\n")
        config[k] = v


# Set configs from dictionary 
storage_account_name = config.get('storage_account_name')
container_name = config.get('container_name')
client_id = config.get('client_id')
client_secret = config.get('client_secret')
tenant_id = config.get('tenant_id')

# Initialize Spark
spark = (SparkSession
	.builder
	.config('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:3.3.1,io.delta:delta-core_2.12:2.2.0')
	.config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
	.config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
	.getOrCreate()
)



# setting this to true would allow the principal to create a new container if it does not exist
# spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "True")


# Set configs for direct access to ADLS via Hadoop 
spark.conf.set("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(storage_account_name), "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(storage_account_name), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(storage_account_name), client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(storage_account_name), client_secret )
spark.conf.set("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(storage_account_name),  "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id))


# NOTICE! - Please provide extended path to a CSV(s)
data_path = "abfss://{}@{}.dfs.core.windows.net/".format(container_name, storage_account_name)
df = spark.read.csv(data_path, header=True)
df.show()


# NOTICE! - Please provide extended path to a delta table
delta_path = "abfss://{}@{}.dfs.core.windows.net/".format(container_name, storage_account_name)
delta_df = spark.read.format('delta').load(delta_path)
delta_df.show()