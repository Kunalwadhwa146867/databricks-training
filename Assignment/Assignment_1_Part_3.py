# Databricks notebook source
# MAGIC %md
# MAGIC ### Read a csv file and write it in another location as csv and parquet. Then make the comparison in size and read time

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/asa/airlines/

# COMMAND ----------

df = spark \
.read \
.format('csv') \
.option('header',True) \
.option('inferSchema',True) \
.load('/databricks-datasets/asa/airlines/')

# COMMAND ----------

# MAGIC %md
# MAGIC Write as csv

# COMMAND ----------

df \
.write \
.format('csv') \
.mode('overwrite') \
.save('dbfs:/FileStore/sample_data/output_assignment_3_csv/')

# COMMAND ----------

# MAGIC %md
# MAGIC Write as parquet

# COMMAND ----------

df \
.write \
.format('parquet') \
.mode('overwrite') \
.save('dbfs:/FileStore/sample_data/output_assignment_3_parquet/')

# COMMAND ----------

# MAGIC %md
# MAGIC Runtime comparison for write operation
# MAGIC 
# MAGIC |csv     |parquet  |
# MAGIC |--------|---------|
# MAGIC |7.33 min|8.33 min |

# COMMAND ----------

df = spark \
.read \
.format('csv') \
.option('header',True) \
.option('inferSchema',True) \
.load('dbfs:/FileStore/sample_data/output_assignment_3_csv/')

df.show(5,False)

# COMMAND ----------

df = spark \
.read \
.format('parquet') \
.load('dbfs:/FileStore/sample_data/output_assignment_3_parquet/')

df.show(5,False)

# COMMAND ----------

# MAGIC %md
# MAGIC Runtime comparison for read operation
# MAGIC 
# MAGIC |csv     |parquet      |
# MAGIC |--------|-------------|
# MAGIC |3.87 min|7.23 seconds |

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/sample_data/output_assignment_3_csv'))
display(dbutils.fs.ls('/FileStore/sample_data/output_assignment_3_parquet'))
