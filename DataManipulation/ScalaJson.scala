// Databricks notebook source
import org.apache.spark.sql.types.{StringType, StructField, StructType, ArrayType}
import org.apache.spark.sql.Row

// COMMAND ----------

val data = Seq(("{\"a\": \"adsfsd\", \"b\": \"dsafdsa\", \"c\": []}"), ("{\"a\": \"adsfsd\", \"b\": \"dsafdsa\", \"c\": []}"), ("{\"a\": \"adsfsd\", \"b\": \"dsafdsa\", \"c\": []}"))


// COMMAND ----------

val sc = StructType(Array(
  StructField("value", StringType)
))

val rdd = spark.sparkContext.parallelize(data)

val df2 = rdd.toDF
display(rdd.toDF)

// COMMAND ----------

df2.write.format("delta").mode("overwrite").save("/tmp/data/json")

// COMMAND ----------

val df = spark.readStream.format("delta").load("/tmp/data/json")

// COMMAND ----------

import org.json4s._
import org.json4s.jackson.JsonMethods._

val jsonStrToMap = (json_str:String) => {
  implicit val formats = org.json4s.DefaultFormats

  parse(json_str).extract[Map[String, Any]]
}

val jsonStrToMapUDF = udf(jsonStrToMap)

// COMMAND ----------

import org.apache.spark.sql.functions.to_json


display(df.select(to_json($"value")))


// COMMAND ----------

display(df.rdd.map(_.mkString(",")))

// COMMAND ----------

display(df.withColumn("new_json", jsonStrToMapUDF($"value")))

// COMMAND ----------

display(df.withColumn("new_json", jsonStrToMapUDF($"value")).select("new_json", "new_json.a"))

// COMMAND ----------

import org.apache.spark.sql.types.{StringType, StructField, StructType, ArrayType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.from_json

val data = Seq(("{\"a\": \"adsfsd\", \"b\": \"dsafdsa\", \"c\": []}"), ("{\"a\": \"adsfsd\", \"b\": \"dsafdsa\", \"c\": []}"), ("{\"a\": \"adsfsd\", \"b\": \"dsafdsa\", \"c\": [], \"d\": \"optional\"}"))


// COMMAND ----------



// COMMAND ----------

val sc = StructType(Array(
  StructField("a", StringType),
  StructField("b", StringType),
  StructField("c", ArrayType(StringType)),
  StructField("d", StringType)
))

val rdd = spark.sparkContext.parallelize(data)

val df = rdd.toDF
display(rdd.toDF)

// COMMAND ----------

display(df.withColumn("new_value", from_json($"value", sc)))

// COMMAND ----------

display(spark.read.json(rdd))


// COMMAND ----------

import org.apache.spark.sql.types.{StringType, StructField, StructType, ArrayType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.from_json

val data = Seq(("{\"a\": \"adsfsd\", \"b\": \"dsafdsa\", \"c\": []}"), ("{\"a\": \"adsfsd\", \"b\": \"dsafdsa\", \"c\": []}"), ("{\"a\": \"adsfsd\", \"b\": \"dsafdsa\", \"c\": [], \"d\": \"optional\", \"abc\": \"fail\"}"), ("{\"abbbbbbbb\": \"adsfsd\"}"))

val sc = StructType(Array(
  StructField("a", StringType),
  StructField("b", StringType),
  StructField("c", ArrayType(StringType)),
  StructField("d", StringType)
))

val rdd = spark.sparkContext.parallelize(data)

val df = rdd.toDF
display(rdd.toDF)

display(df.withColumn("new_value", from_json($"value", sc)))
