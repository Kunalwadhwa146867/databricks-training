# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Different methods to create a dataframe in pyspark using createDataFrame method

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Using List of Row objects where Row is of type pyspark.sql.Row

# COMMAND ----------

from datetime import date,datetime
from pyspark.sql import Row
df = spark.createDataFrame([
    Row(id=1,name='A',reg_date=date(2023,2,1),reg_timestamp=datetime(2023,2,1,14,0,10)),
    Row(id=2,name='B',reg_date=date(2023,2,2),reg_timestamp=datetime(2023,2,1,15,0,10)),
    Row(id=3,name='C',reg_date=date(2023,2,3),reg_timestamp=datetime(2023,2,1,16,0,10))
])

df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##2. By explicity specifying schema

# COMMAND ----------

from datetime import date,datetime
from pyspark.sql import Row
df = spark.createDataFrame([
    (1,'A',date(2023,2,1),datetime(2023,2,1,14,0,10)),
    (2,'B',date(2023,2,2),datetime(2023,2,2,15,0,10)),
    (3,'C',date(2023,2,3),datetime(2023,2,3,16,0,10))
],schema='id int,name string,reg_date date,reg_timestamp timestamp')

df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Through pandas dataframe

# COMMAND ----------

import pandas as pd
pandas_df = pd.DataFrame(
    {
     'id': [1,2,3],
     'name': ['A','B','C'],
     'reg_date': [date(2023,2,1),date(2023,2,2),date(2023,2,3)],
     'reg_timestamp': [datetime(2023,2,1,14,0,10),datetime(2023,2,2,15,0,10),datetime(2023,2,3,16,0,10)]
    })
df = spark.createDataFrame(pandas_df)
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The dataframe can also be created through rdd

# COMMAND ----------

rdd = spark.sparkContext.parallelize([
        (1,'A',date(2023,2,1),datetime(2023,2,1,14,0,10)),
        (2,'B',date(2023,2,2),datetime(2023,2,2,15,0,10)),
        (3,'C',date(2023,2,3),datetime(2023,2,3,16,0,10))
    ])

df = spark.createDataFrame(rdd,schema='id int,name string,reg_date date,reg_timestamp timestamp')
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## The records can also be displayed vertically

# COMMAND ----------

df.show(1,vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## To get columns from a dataframe

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## To describe columns from a dataframe

# COMMAND ----------

df.select('id','name').describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## To convert spark dataframe to pandas dataframe

# COMMAND ----------

df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying aggregate function

# COMMAND ----------

df = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])
df.show()

# COMMAND ----------

df.groupby('color').avg().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding a new column to a dataframe

# COMMAND ----------

from pyspark.sql.functions import lower
rdd = spark.sparkContext.parallelize([
        (1,'A',date(2023,2,1),datetime(2023,2,1,14,0,10)),
        (2,'B',date(2023,2,2),datetime(2023,2,2,15,0,10)),
        (3,'C',date(2023,2,3),datetime(2023,2,3,16,0,10))
    ])

df = spark.createDataFrame(rdd,schema='id int,name string,reg_date date,reg_timestamp timestamp')
df = df.withColumn('name_lower',lower(df.name))
df.show()

# COMMAND ----------


