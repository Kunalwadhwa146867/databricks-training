# Databricks notebook source
# MAGIC %md
# MAGIC ## Create a dataframe and show number of partitions

# COMMAND ----------

df = spark.createDataFrame([
  (24,'A'),
  (25,'B'),
  (26,'C')
],schema='age int,name string')

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check default parallelism

# COMMAND ----------

sc.defaultParallelism

# COMMAND ----------

# MAGIC %md
# MAGIC ## Increase number of partitions using repartition

# COMMAND ----------

df = df.repartition(10)
df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Use coalesce - it can't be use to increase the number of partitions

# COMMAND ----------

df = df.coalesce(20)
df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Use coalesce - it can be use to decrease the number of partitions

# COMMAND ----------

df = df.coalesce(5)
df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Show partition id using spark_partition_id

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id
df = df.withColumn('pid',spark_partition_id())
df.show()
