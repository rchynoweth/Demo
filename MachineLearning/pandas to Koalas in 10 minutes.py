# Databricks notebook source
# MAGIC %md # pandas to Koalas in 10 minutes

# COMMAND ----------

# MAGIC %md ## Migration from pandas to Koalas

# COMMAND ----------

# MAGIC %md ### Object creation

# COMMAND ----------

import numpy as np
import pandas as pd
import databricks.koalas as ks

# COMMAND ----------

# Create a pandas Series
pser = pd.Series([1, 3, 5, np.nan, 6, 8]) 
# Create a Koalas Series
kser = ks.Series([1, 3, 5, np.nan, 6, 8])
# Create a Koalas Series by passing a pandas Series
kser = ks.Series(pser)
kser = ks.from_pandas(pser)

# COMMAND ----------

pser

# COMMAND ----------

kser

# COMMAND ----------

kser.sort_index()

# COMMAND ----------

# Create a pandas DataFrame
pdf = pd.DataFrame({'A': np.random.rand(5),
                    'B': np.random.rand(5)})
# Create a Koalas DataFrame
kdf = ks.DataFrame({'A': np.random.rand(5),
                    'B': np.random.rand(5)})
# Create a Koalas DataFrame by passing a pandas DataFrame
kdf = ks.DataFrame(pdf)
kdf = ks.from_pandas(pdf)

# COMMAND ----------

pdf

# COMMAND ----------

kdf.sort_index()

# COMMAND ----------

# MAGIC %md ### Viewing data

# COMMAND ----------

kdf.head(2)

# COMMAND ----------

kdf.describe()

# COMMAND ----------

kdf.sort_values(by='B')

# COMMAND ----------

kdf.transpose()

# COMMAND ----------

from databricks.koalas.config import set_option, get_option
ks.get_option('compute.max_rows')

# COMMAND ----------

ks.set_option('compute.max_rows', 2000)
ks.get_option('compute.max_rows')

# COMMAND ----------

# MAGIC %md ### Selection

# COMMAND ----------

kdf['A']  # or kdf.A

# COMMAND ----------

kdf[['A', 'B']]

# COMMAND ----------

kdf.loc[1:2]

# COMMAND ----------

kdf.iloc[:3, 1:2]

# COMMAND ----------

kser = ks.Series([100, 200, 300, 400, 500], index=[0, 1, 2, 3, 4])
# The below commented line will fail since Koalas disallows adding columns coming from
# different DataFrames or Series to a Koalas DataFrame as adding columns requires
# join operations which are generally expensive.
# This operation can be enabled by setting compute.ops_on_diff_frames to True.
# If you want to know about more detail, See the following blog post.
# https://databricks.com/blog/2020/03/31/10-minutes-from-pandas-to-koalas-on-apache-spark.html
# kdf['C'] = kser

# COMMAND ----------

# Those are needed for managing options
from databricks.koalas.config import set_option, reset_option
set_option("compute.ops_on_diff_frames", True)
kdf['C'] = kser
# Reset to default to avoid potential expensive operation in the future
reset_option("compute.ops_on_diff_frames")
kdf

# COMMAND ----------

# MAGIC %md ### Applying Python function with Koalas object

# COMMAND ----------

kdf.apply(np.cumsum)

# COMMAND ----------

kdf.apply(np.cumsum, axis=1)

# COMMAND ----------

kdf.apply(lambda x: x ** 2)

# COMMAND ----------

def square(x) -> ks.Series[np.float64]:
    return x ** 2

# COMMAND ----------

kdf.apply(square)

# COMMAND ----------

# Working properly since size of data <= compute.shortcut_limit (1000)
ks.DataFrame({'A': range(1000)}).apply(lambda col: col.max())

# COMMAND ----------

# Not working properly since size of data > compute.shortcut_limit (1000)
ks.DataFrame({'A': range(1001)}).apply(lambda col: col.max())

# COMMAND ----------

ks.set_option('compute.shortcut_limit', 1001)
ks.DataFrame({'A': range(1001)}).apply(lambda col: col.max())

# COMMAND ----------

# MAGIC %md ### Grouping Data

# COMMAND ----------

kdf.groupby('A').sum()

# COMMAND ----------

kdf.groupby(['A', 'B']).sum()

# COMMAND ----------

# MAGIC %md ### Plotting

# COMMAND ----------

# This is needed for visualizing plot on notebook
%matplotlib inline

# COMMAND ----------

speed = [0.1, 17.5, 40, 48, 52, 69, 88]
lifespan = [2, 8, 70, 1.5, 25, 12, 28]
index = ['snail', 'pig', 'elephant',
         'rabbit', 'giraffe', 'coyote', 'horse']
kdf = ks.DataFrame({'speed': speed,
                   'lifespan': lifespan}, index=index)
kdf.plot.bar()

# COMMAND ----------

kdf.plot.barh()

# COMMAND ----------

kdf = ks.DataFrame({'mass': [0.330, 4.87, 5.97],
                    'radius': [2439.7, 6051.8, 6378.1]},
                   index=['Mercury', 'Venus', 'Earth'])
kdf.plot.pie(y='mass')

# COMMAND ----------

kdf = ks.DataFrame({
    'sales': [3, 2, 3, 9, 10, 6, 3],
    'signups': [5, 5, 6, 12, 14, 13, 9],
    'visits': [20, 42, 28, 62, 81, 50, 90],
}, index=pd.date_range(start='2019/08/15', end='2020/03/09',
                       freq='M'))
kdf.plot.area()

# COMMAND ----------

kdf = ks.DataFrame({'pig': [20, 18, 489, 675, 1776],
                    'horse': [4, 25, 281, 600, 1900]},
                   index=[1990, 1997, 2003, 2009, 2014])
kdf.plot.line()

# COMMAND ----------

kdf = pd.DataFrame(
    np.random.randint(1, 7, 6000),
    columns=['one'])
kdf['two'] = kdf['one'] + np.random.randint(1, 7, 6000)
kdf = ks.from_pandas(kdf)
kdf.plot.hist(bins=12, alpha=0.5)

# COMMAND ----------

kdf = ks.DataFrame([[5.1, 3.5, 0], [4.9, 3.0, 0], [7.0, 3.2, 1],
                    [6.4, 3.2, 1], [5.9, 3.0, 2]],
                   columns=['length', 'width', 'species'])
kdf.plot.scatter(x='length',
                 y='width',
                 c='species',
                 colormap='viridis')

# COMMAND ----------

# MAGIC %md ## Missing Functionalities and Workarounds in Koalas

# COMMAND ----------

# MAGIC %md ### Directly use pandas APIs through type conversion

# COMMAND ----------

kidx = kdf.index

# COMMAND ----------

# Index.to_list() raises PandasNotImplementedError.
# Koalas does not support this because it requires collecting all data into the client
# (driver node) side. A simple workaround is to convert to pandas using to_pandas().
# If you want to know about more detail, See the following blog post.
# https://databricks.com/blog/2020/03/31/10-minutes-from-pandas-to-koalas-on-apache-spark.html
# kidx.to_list()

# COMMAND ----------

kidx.to_pandas().to_list()

# COMMAND ----------

# MAGIC %md ### Native Support for pandas Objects

# COMMAND ----------

kdf = ks.DataFrame({'A': 1.,
                    'B': pd.Timestamp('20130102'),
                    'C': pd.Series(1, index=list(range(4)), dtype='float32'),
                    'D': np.array([3] * 4, dtype='int32'),
                    'F': 'foo'})

# COMMAND ----------

kdf

# COMMAND ----------

# MAGIC %md ### Distributed execution for pandas functions

# COMMAND ----------

i = pd.date_range('2018-04-09', periods=2000, freq='1D1min')
ts = ks.DataFrame({'A': ['timestamp']}, index=i)

# DataFrame.between_time() is not yet implemented in Koalas.
# A simple workaround is to convert to a pandas DataFrame using to_pandas(),
# and then applying the function.
# If you want to know about more detail, See the following blog post.
# https://databricks.com/blog/2020/03/31/10-minutes-from-pandas-to-koalas-on-apache-spark.html
# ts.between_time('0:15', '0:16')

# COMMAND ----------

ts.to_pandas().between_time('0:15', '0:16')

# COMMAND ----------

ts.map_in_pandas(func=lambda pdf: pdf.between_time('0:15', '0:16'))

# COMMAND ----------

# MAGIC %md ### Using SQL in Koalas

# COMMAND ----------

kdf = ks.DataFrame({'year': [1990, 1997, 2003, 2009, 2014],
                    'pig': [20, 18, 489, 675, 1776],
                    'horse': [4, 25, 281, 600, 1900]})

# COMMAND ----------

ks.sql("SELECT * FROM {kdf} WHERE pig > 100")

# COMMAND ----------

pdf = pd.DataFrame({'year': [1990, 1997, 2003, 2009, 2014],
                    'sheep': [22, 50, 121, 445, 791],
                    'chicken': [250, 326, 589, 1241, 2118]})

# COMMAND ----------

ks.sql('''
    SELECT ks.pig, pd.chicken
    FROM {kdf} ks INNER JOIN {pdf} pd
    ON ks.year = pd.year
    ORDER BY ks.pig, pd.chicken''')

# COMMAND ----------

# MAGIC %md ## Working with PySpark

# COMMAND ----------

# MAGIC %md ### Conversion from and to PySpark DataFrame

# COMMAND ----------

kdf = ks.DataFrame({'A': [1, 2, 3, 4, 5], 'B': [10, 20, 30, 40, 50]})
sdf = kdf.to_spark()
type(sdf)

# COMMAND ----------

sdf.show()

# COMMAND ----------

from databricks.koalas import option_context
with option_context(
        "compute.default_index_type", "distributed-sequence"):
    kdf = sdf.to_koalas()
type(kdf)

# COMMAND ----------

kdf

# COMMAND ----------

sdf.to_koalas(index_col='A')

# COMMAND ----------

# MAGIC %md ### Checking Spark execution plans

# COMMAND ----------

from databricks.koalas import option_context

with option_context(
        "compute.ops_on_diff_frames", True,
        "compute.default_index_type", 'distributed'):
    df = ks.range(10) + ks.range(10)
    df.explain()

# COMMAND ----------

with option_context(
        "compute.ops_on_diff_frames", False,
        "compute.default_index_type", 'distributed'):
    df = ks.range(10)
    df = df + df
    df.explain()

# COMMAND ----------

# MAGIC %md ### Caching DataFrames

# COMMAND ----------

with option_context("compute.default_index_type", 'distributed'):
    df = ks.range(10)
    new_df = (df + df).cache()  # `(df + df)` is cached here as `df`
    new_df.explain()

# COMMAND ----------

new_df.unpersist()

# COMMAND ----------

with (df + df).cache() as df:
    df.explain()
