#!/bin/bash

# Remove the old JAR file from the target directory
rm -rf /databricks/jars/----ws_3_5--third_party--bigquery-connector--spark-bigquery-connector-hive-2.3__hadoop-3.2_2.12--118181791--fatJar-assembly-0.22.2-SNAPSHOT.jar

# Download the JAR file
# wget -P /databricks/jars/ https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.39.1.jar
# wget -P /databricks/jars/ https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.41.0.jar
wget -P /databricks/jars/ -O /databricks/jars/spark_3_5_bigquery_0_39_1.jar https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.39.1.jar

