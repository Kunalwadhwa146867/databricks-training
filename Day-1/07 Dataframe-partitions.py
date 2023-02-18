# Databricks notebook source
# MAGIC %md
# MAGIC Create a dataframe using some input data

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(
    [
        (5, "Bob"), 
        (5, "Bob"),
        (2, "Alice"),
        (2, "Alice"),
    ],
    ["age", "name"]
)

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Find out number of default partitions

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC Check default parallelism which is the number of cores

# COMMAND ----------

# MAGIC %scala
# MAGIC sc.defaultParallelism

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC Using repartition to increase number of partitions

# COMMAND ----------

df = df.repartition(10)
print(df.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC Using coalesce to reduce number of partitions, coalesce cannot increase number of partitions as it works on the existing number of partitions. This uses existing partitions to minimize the amount of data that’s shuffle

# COMMAND ----------

df = df.coalesce(20)
print(df.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC Coalesce to reduce number of partitions

# COMMAND ----------

df = df.coalesce(4)
print(df.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC Create some dummy data

# COMMAND ----------

import pandas as pd
import numpy as np
from pyspark.sql.functions import spark_partition_id

def create_dummy_data():

    data = np.vstack([np.random.randint(0, 5, size=10), 
                      np.random.random(10)])

    df = pd.DataFrame(data.T, columns=["id", "values"])

    return spark.createDataFrame(df)

def show_partition_id(df):
    """Helper function to show partition."""
    return df.select(*df.columns, spark_partition_id().alias("pid")).show()

df1 = create_dummy_data()
#df2 = create_dummy_data()

# COMMAND ----------

df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Show partition id before repartioning

# COMMAND ----------

df1.show()

# COMMAND ----------

show_partition_id(df1)
#show_partition_id(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC Show partition id after repartitioning

# COMMAND ----------

show_partition_id(df1.repartition(2, "id"))
#show_partition_id(df1.repartition(2, "id"))

# COMMAND ----------

