# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit, col

# Define the schema for the source DataFrame
source_schema = StructType([
    StructField("category_id", IntegerType(), True),
    StructField("category_description", StringType(), True),
    StructField("parent_id", IntegerType(), True)
])

# Create data for the source DataFrame
source_data = [
    (1, 'Electronics', 0),
    (2, 'Computers', 1),
    (3, 'Laptops', 2),
    (4, 'Gaming Laptops', 3),
    (5, 'Smartphones', 1),
    (6, 'Clothing', 0),
    (7, 'Men\'s Wear', 6),
    (8, 'Women\'s Wear', 6)
]

# Create the source DataFrame
source_df = spark.createDataFrame(source_data, schema=source_schema)

display(source_df)

# COMMAND ----------

(source_df
 .withColumn('level', lit(0))
 .withColumn("path", col('category_description'))
 .createOrReplaceTempView("source_df")
)

display(spark.sql('select * from source_df'))

# COMMAND ----------

level = 0
# we want to start with the products that have no parent
df = spark.sql('select * from source_df where parent_id = 0')
df.createOrReplaceTempView("tmp_df")


while True:
  print(f"Level: {level}")
  next_level_df = spark.sql(f"""
                            select b.category_id, b.category_description, b.parent_id, {level+1} as level, concat(a.path, ' > ', b.category_description) as path
                            from tmp_df a
                            inner join source_df b on a.category_id = b.parent_id
                                    and a.level = {level}
                            """)
  if not next_level_df.isEmpty():
    df = df.union(next_level_df)
    df.createOrReplaceTempView("tmp_df")
    level+=1
  else : 
    df.createOrReplaceTempView("final_df")
    break

# COMMAND ----------

display(spark.sql("select * from final_df order by level"))

# COMMAND ----------


