-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Parsing JSON with SQL
-- MAGIC 
-- MAGIC [Documentation](https://docs.databricks.com/spark/latest/spark-sql/semi-structured.html)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # this database should be the same as the same as the set up notebooks 
-- MAGIC dbutils.widgets.text("database_name", "rac_demo_db")
-- MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS {} ".format(dbutils.widgets.get("database_name")))
-- MAGIC spark.sql("use {}".format(dbutils.widgets.get("database_name"))) ## select the database to use should be the same as the notebooks 1-3 for data setup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("Database: {}".format(dbutils.widgets.get("database_name")))

-- COMMAND ----------

CREATE TABLE store_data AS SELECT
'{
   "store":{
      "fruit": [
        {"weight":8,"type":"apple"},
        {"weight":9,"type":"pear"}
      ],
      "basket":[
        [1,2,{"b":"y","a":"x"}],
        [3,4],
        [5,6]
      ],
      "book":[
        {
          "author":"Nigel Rees",
          "title":"Sayings of the Century",
          "category":"reference",
          "price":8.95
        },
        {
          "author":"Herman Melville",
          "title":"Moby Dick",
          "category":"fiction",
          "price":8.99,
          "isbn":"0-553-21311-3"
        },
        {
          "author":"J. R. R. Tolkien",
          "title":"The Lord of the Rings",
          "category":"fiction",
          "reader":[
            {"age":25,"name":"bob"},
            {"age":26,"name":"jack"}
          ],
          "price":22.99,
          "isbn":"0-395-19395-8"
        }
      ],
      "bicycle":{
        "price":19.95,
        "color":"red"
      }
    },
    "owner":"amy",
    "zip code":"94025",
    "fb:testid":"1234"
 }' as raw

-- COMMAND ----------

select * from store_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Top level column

-- COMMAND ----------


SELECT raw:owner, RAW:owner FROM store_data

-- COMMAND ----------

-- References are case sensitive when you use brackets
SELECT raw:OWNER case_insensitive, raw:['OWNER'] case_sensitive FROM store_data

-- COMMAND ----------

-- Use backticks to escape special characters. References are case insensitive when you use backticks.
-- Use brackets to make them case sensitive.
SELECT raw:`zip code`, raw:`Zip Code`, raw:['fb:testid'] FROM store_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Extract nested fields

-- COMMAND ----------

-- Use dot notation
SELECT raw:store.bicycle FROM store_data
-- the column returned is a string

-- COMMAND ----------

-- Use brackets
SELECT raw:store['bicycle'], raw:store['BICYCLE'] FROM store_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Extract values from arrays

-- COMMAND ----------

-- Index elements
SELECT raw:store.fruit[0], raw:store.fruit[1] FROM store_data

-- COMMAND ----------

-- Extract subfields from arrays
SELECT raw:store.book[*].isbn FROM store_data

-- COMMAND ----------

-- Access arrays within arrays or structs within arrays
SELECT
    raw:store.basket[*],
    raw:store.basket[*][0] first_of_baskets,
    raw:store.basket[0][*] first_basket,
    raw:store.basket[*][*] all_elements_flattened,
    raw:store.basket[0][2].b subfield
FROM store_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cast Values

-- COMMAND ----------

-- price is returned as a double, not a string
SELECT raw:store.bicycle.price::double FROM store_data

-- COMMAND ----------

-- use from_json to cast into more complex types
SELECT from_json(raw:store.bicycle, 'price double, color string') bicycle FROM store_data
-- the column returned is a struct containing the columns price and color

-- COMMAND ----------

SELECT from_json(raw:store.basket[*], 'array<array<string>>') baskets FROM store_data
-- the column returned is an array of string arrays

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Null Behavior

-- COMMAND ----------

select '{"key":null}':key is null sql_null, '{"key":null}':key == 'null' text_null

-- COMMAND ----------


