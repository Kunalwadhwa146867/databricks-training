# Databricks notebook source
# MAGIC %md
# MAGIC # Quickstart: DataFrame
# MAGIC 
# MAGIC This is a short introduction and quickstart for the PySpark DataFrame API. PySpark DataFrames are lazily evaluated. They are implemented on top of [RDD](https://spark.apache.org/docs/latest/rdd-programming-guide.html#overview)s. When Spark [transforms](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations) data, it does not immediately compute the transformation but plans how to compute later. When [actions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions) such as `collect()` are explicitly called, the computation starts.
# MAGIC This notebook shows the basic usages of the DataFrame, geared mainly for new users. You can run the latest version of these examples by yourself in 'Live Notebook: DataFrame' at [the quickstart page](https://spark.apache.org/docs/latest/api/python/getting_started/index.html).
# MAGIC 
# MAGIC There is also other useful information in Apache Spark documentation site, see the latest version of [Spark SQL and DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html), [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html), [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html), [Spark Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html) and [Machine Learning Library (MLlib) Guide](https://spark.apache.org/docs/latest/ml-guide.html).
# MAGIC 
# MAGIC PySpark applications start with initializing `SparkSession` which is the entry point of PySpark as below. In case of running it in PySpark shell via <code>pyspark</code> executable, the shell automatically creates the session in the variable <code>spark</code> for users.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame Creation
# MAGIC 
# MAGIC A PySpark DataFrame can be created via `pyspark.sql.SparkSession.createDataFrame` typically by passing a list of lists, tuples, dictionaries and `pyspark.sql.Row`s, a [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) and an RDD consisting of such a list.
# MAGIC `pyspark.sql.SparkSession.createDataFrame` takes the `schema` argument to specify the schema of the DataFrame. When it is omitted, PySpark infers the corresponding schema by taking a sample from the data.
# MAGIC 
# MAGIC Firstly, you can create a PySpark DataFrame from a list of rows

# COMMAND ----------

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
df

# COMMAND ----------

# MAGIC %md
# MAGIC Create a PySpark DataFrame with an explicit schema.

# COMMAND ----------

df = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')
df

# COMMAND ----------

# MAGIC %md
# MAGIC Create a PySpark DataFrame from a pandas DataFrame

# COMMAND ----------

pandas_df = pd.DataFrame({
    'a': [1, 2, 3],
    'b': [2., 3., 4.],
    'c': ['string1', 'string2', 'string3'],
    'd': [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
    'e': [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)]
})
df = spark.createDataFrame(pandas_df)
df

# COMMAND ----------

# MAGIC %md
# MAGIC Create a PySpark DataFrame from an RDD consisting of a list of tuples.

# COMMAND ----------

rdd = spark.sparkContext.parallelize([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
])
df = spark.createDataFrame(rdd, schema=['a', 'b', 'c', 'd', 'e'])
df

# COMMAND ----------

# MAGIC %md
# MAGIC The DataFrames created above all have the same results and schema.

# COMMAND ----------

# All DataFrames above result same.
df.show()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Viewing Data
# MAGIC 
# MAGIC The top rows of a DataFrame can be displayed using `DataFrame.show()`.

# COMMAND ----------

df.show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Alternatively, you can enable `spark.sql.repl.eagerEval.enabled` configuration for the eager evaluation of PySpark DataFrame in notebooks such as Jupyter. The number of rows to show can be controlled via `spark.sql.repl.eagerEval.maxNumRows` configuration.

# COMMAND ----------

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
df

# COMMAND ----------

# MAGIC %md
# MAGIC The rows can also be shown vertically. This is useful when rows are too long to show horizontally.

# COMMAND ----------

df.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC You can see the DataFrame's schema and column names as follows:

# COMMAND ----------

df.columns

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Show the summary of the DataFrame

# COMMAND ----------

df.select("a", "b", "c").describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC `DataFrame.collect()` collects the distributed data to the driver side as the local data in Python. Note that this can throw an out-of-memory error when the dataset is too large to fit in the driver side because it collects all the data from executors to the driver side.

# COMMAND ----------

df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC In order to avoid throwing an out-of-memory exception, use `DataFrame.take()` or `DataFrame.tail()`.

# COMMAND ----------

df.take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark DataFrame also provides the conversion back to a [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) to leverage pandas API. Note that `toPandas` also collects all data into the driver side that can easily cause an out-of-memory-error when the data is too large to fit into the driver side.

# COMMAND ----------

df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selecting and Accessing Data
# MAGIC 
# MAGIC PySpark DataFrame is lazily evaluated and simply selecting a column does not trigger the computation but it returns a `Column` instance.

# COMMAND ----------

df.a

# COMMAND ----------

# MAGIC %md
# MAGIC In fact, most of column-wise operations return `Column`s.

# COMMAND ----------

from pyspark.sql import Column
from pyspark.sql.functions import upper

type(df.c) == type(upper(df.c)) == type(df.c.isNull())

# COMMAND ----------

# MAGIC %md
# MAGIC These `Column`s can be used to select the columns from a DataFrame. For example, `DataFrame.select()` takes the `Column` instances that returns another DataFrame.

# COMMAND ----------

df.select(df.c).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Assign new `Column` instance.

# COMMAND ----------

df.withColumn('upper_c', upper(df.c)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC To select a subset of rows, use `DataFrame.filter()`.

# COMMAND ----------

df.filter(df.a == 1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying a Function
# MAGIC 
# MAGIC PySpark supports various UDFs and APIs to allow users to execute Python native functions. See also the latest [Pandas UDFs](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#pandas-udfs-aka-vectorized-udfs) and [Pandas Function APIs](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#pandas-function-apis). For instance, the example below allows users to directly use the APIs in [a pandas Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html) within Python native function.

# COMMAND ----------

import pandas
from pyspark.sql.functions import pandas_udf

@pandas_udf('long')
def pandas_plus_one(series: pd.Series) -> pd.Series:
    # Simply plus one by using pandas Series.
    return series + 1

df.select(pandas_plus_one(df.a)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Another example is `DataFrame.mapInPandas` which allows users directly use the APIs in a [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) without any restrictions such as the result length.

# COMMAND ----------

def pandas_filter_func(iterator):
    for pandas_df in iterator:
        yield pandas_df[pandas_df.a == 1]

df.mapInPandas(pandas_filter_func, schema=df.schema).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grouping Data
# MAGIC 
# MAGIC PySpark DataFrame also provides a way of handling grouped data by using the common approach, split-apply-combine strategy.
# MAGIC It groups the data by a certain condition applies a function to each group and then combines them back to the DataFrame.

# COMMAND ----------

df = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Grouping and then applying the `avg()` function to the resulting groups.

# COMMAND ----------

df.groupby('color').avg().show()

# COMMAND ----------

# MAGIC %md
# MAGIC You can also apply a Python native function against each group by using pandas API.

# COMMAND ----------

def plus_mean(pandas_df):
    return pandas_df.assign(v1=pandas_df.v1 - pandas_df.v1.mean())

df.groupby('color').applyInPandas(plus_mean, schema=df.schema).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Co-grouping and applying a function.

# COMMAND ----------

df1 = spark.createDataFrame(
    [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
    ('time', 'id', 'v1'))

df2 = spark.createDataFrame(
    [(20000101, 1, 'x'), (20000101, 2, 'y')],
    ('time', 'id', 'v2'))

def asof_join(l, r):
    return pd.merge_asof(l, r, on='time', by='id')

df1.groupby('id').cogroup(df2.groupby('id')).applyInPandas(
    asof_join, schema='time int, id int, v1 double, v2 string').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting Data in/out
# MAGIC 
# MAGIC CSV is straightforward and easy to use. Parquet and ORC are efficient and compact file formats to read and write faster.
# MAGIC 
# MAGIC There are many other data sources available in PySpark such as JDBC, text, binaryFile, Avro, etc. See also the latest [Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html) in Apache Spark documentation.

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV

# COMMAND ----------

df.write.csv('foo.csv', header=True)
spark.read.csv('foo.csv', header=True).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet

# COMMAND ----------

df.write.parquet('bar.parquet')
spark.read.parquet('bar.parquet').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ORC

# COMMAND ----------

df.write.orc('zoo.orc')
spark.read.orc('zoo.orc').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working with SQL
# MAGIC 
# MAGIC DataFrame and Spark SQL share the same execution engine so they can be interchangeably used seamlessly. For example, you can register the DataFrame as a table and run a SQL easily as below:

# COMMAND ----------

df.createOrReplaceTempView("tableA")
spark.sql("SELECT count(*) from tableA").show()

# COMMAND ----------

# MAGIC %md
# MAGIC In addition, UDFs can be registered and invoked in SQL out of the box:

# COMMAND ----------

@pandas_udf("integer")
def add_one(s: pd.Series) -> pd.Series:
    return s + 1

spark.udf.register("add_one", add_one)
spark.sql("SELECT add_one(v1) FROM tableA").show()

# COMMAND ----------

# MAGIC %md
# MAGIC These SQL expressions can directly be mixed and used as PySpark columns.

# COMMAND ----------

from pyspark.sql.functions import expr

df.selectExpr('add_one(v1)').show()
df.select(expr('count(*)') > 0).show()