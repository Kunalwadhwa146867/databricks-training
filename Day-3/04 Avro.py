# Databricks notebook source
from pyspark.sql.functions import col,array_contains

# COMMAND ----------

df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/day4/uszips.csv")
df.show()

# COMMAND ----------

df.write.partitionBy("city","zip").format("avro").save("/FileStore/tables/day4/uszips2.avro")

# COMMAND ----------

df1 = spark.read.format("avro").load("/FileStore/tables/day4/uszips.avro").where(col("city") == 'Camuy')
df1.show()

# COMMAND ----------

spark.sql("CREATE TEMPORARY VIEW ZIPC2 USING avro OPTIONS (path \"/FileStore/tables/day4/uszips.avro/city=Camuy\")")
spark.sql("SELECT * FROM ZIPC2").show()

# COMMAND ----------

df = spark.sql("SELECT * FROM avro.`/FileStore/tables/day4/uszips.avro` where zip=603")
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

