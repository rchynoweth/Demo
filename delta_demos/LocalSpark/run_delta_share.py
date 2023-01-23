from delta import *
import delta_sharing
from pyspark.sql import SparkSession

# I used an absolute path
profile_file = "/Users/ryan.chynoweth/Documents/gitmine/Demo/delta_demos/LocalSpark/delta-sharing-server-0.5.3/conf/oss-share.share"
client = delta_sharing.SharingClient(profile=profile_file)
share_name = "rac_adls_share"
schema_name = "rac_schema"
table_name = "demo_table"

# list everything 
client.list_shares()
client.list_all_tables()

# list subsets
s = delta_sharing.Share(share_name)
table_schema = delta_sharing.Schema(name=schema_name, share=share_name)
client.list_schemas(s)
client.list_tables(schema=table_schema)


# Initialize Spark
spark = (SparkSession
	.builder
	.config('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:3.3.1,io.delta:delta-core_2.12:2.2.0,io.delta:delta-sharing-spark_2.12:0.6.2')
	.config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
	.config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
	.getOrCreate()
)

# Read from share
table_url = f"{profile_file}#{share_name}.{schema_name}.{table_name}"
share_df = delta_sharing.load_as_spark(url=table_url)
share_df.show()

(spark.read
    .format("deltaSharing")
    .load(table_url)
    .show()
)
