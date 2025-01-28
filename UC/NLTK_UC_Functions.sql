-- Databricks notebook source
use catalog rac_demo_catalog;
use schema rac_demo_db;
create volume if not exists nltk_data;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION rac_demo_catalog.rac_demo_db.ds_ba_string_singularization(text_string STRING)

RETURNS STRING

LANGUAGE PYTHON

COMMENT 'Description: This function converts plural words in a given string to their singular form.

Returns the string with words singularized, and raises an exception for errors.

Scope: This function can be used in any context where there is a need to convert plural words to singular form, such as data preprocessing, text normalization, or data cleaning tasks. '
AS $$
"""

Description: This function converts plural words in a given string to their singular form.

Scope: This function can be used in any context where there is a need to convert plural words to singular form, such as data preprocessing, text normalization, or data cleaning tasks.


Parameters:
- text_string (string): target string for conversion of plural words to singular form.


Returns:
- string: Returns the string with words singularized, and raises an exception for errors.

Example usage:
SELECT feature_store.text_cleanup.ds_ba_string_singularization('apples bananas cherries') AS result;

"""
import tempfile
import nltk
import os
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
from nltk.data import path

with tempfile.TemporaryDirectory() as tmpdirname:
  path.append(tmpdirname)
  nltk.download('wordnet', download_dir=tmpdirname)
  try:
    lemmatizer = WordNetLemmatizer()
    if text_string is not None:
      singularized_words = [lemmatizer.lemmatize(word) for word in text_string.split()]
      return " ".join(singularized_words)
    else:
      return text_string

  except Exception as e:
      raise Exception(f"Error in singularizing words: {str(e)}")

$$

-- COMMAND ----------

select rac_demo_catalog.rac_demo_db.ds_ba_string_singularization('apples bananas cherries')

-- COMMAND ----------


