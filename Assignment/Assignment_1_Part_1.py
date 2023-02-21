# Databricks notebook source
# MAGIC %md
# MAGIC ### Read csv file from one location, apply a filter clause and write it to another location

# COMMAND ----------

df = spark \
    .read \
    .format('csv') \
    .option('header',True) \
    .option('inferSchema',True) \
    .load('dbfs:/FileStore/sample_data/New_Zealand_period_life_tables_2017_2019_CSV.csv')

df.show(10,False)
df.printSchema()

# COMMAND ----------

df_male = df.filter("sex == 'Male'")
df_male \
.write \
.format('csv') \
.mode('overwrite') \
.save('dbfs:/FileStore/sample_data/output_assignment_1/')

# COMMAND ----------

# MAGIC %fs ls /FileStore/sample_data/output_assignment_1/
