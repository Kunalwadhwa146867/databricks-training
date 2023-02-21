# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains

# COMMAND ----------

# Reading csv file
df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/day4/uszips.csv")
df.printSchema()

# COMMAND ----------

df.write.mode('overwrite').json("/FileStore/tables/day4/uszips.json")


# COMMAND ----------

rdd = sc.textFile("/FileStore/tables/day4/uszips.json")
rdd.collect()
    

# COMMAND ----------

df = spark.read.json("/FileStore/tables/day4/uszips_1.json")
df.show(5) 

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark DataFrameWriter also has a method mode() to specify saving mode.
# MAGIC 
# MAGIC overwrite – mode is used to overwrite the existing file.
# MAGIC 
# MAGIC append – To add the data to the existing file.
# MAGIC 
# MAGIC ignore – Ignores write operation when the file already exists.
# MAGIC 
# MAGIC error – This is a default option when the file already exists, it returns an error.

# COMMAND ----------

#  create a temporary view or table directly on the parquet file instead of creating from DataFrame.

spark.sql("CREATE TEMPORARY VIEW ZIPC4 USING json OPTIONS (path \"/FileStore/tables/day4/uszips.json\")")
spark.sql("SELECT * FROM ZIPC4").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # With Nested Json

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
# MAGIC 
# MAGIC sensor_schema = StructType(fields=[
# MAGIC     StructField('sensorName', StringType(), False),
# MAGIC     StructField('sensorDate', StringType(), True),
# MAGIC     StructField(
# MAGIC         'sensorReadings', ArrayType(
# MAGIC             StructType([
# MAGIC                 StructField('sensorChannel', IntegerType(), False),
# MAGIC                 StructField('sensorReading', DoubleType(), True),
# MAGIC                 StructField('datetime', StringType(), True)
# MAGIC             ])
# MAGIC         )
# MAGIC     )
# MAGIC ])

# COMMAND ----------

dfq = spark.read.json("/FileStore/tables/sample/sensor1-7.json",schema=sensor_schema)
dfq.show(5, False) 

# COMMAND ----------

from pyspark.sql.functions import explode

data_df = dfq.select(
    "sensorName",
    explode("sensorReadings").alias("sensorReadingsExplode")
)

data_df.show(5, False) 

# COMMAND ----------

from pyspark.sql.functions import explode

data_df = dfq.select(
    "sensorName",
    explode("sensorReadings").alias("sensorReadingsExplode")
).select("sensorName", "sensorReadingsExplode.*")

data_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW json_temp_view
# MAGIC AS SELECT * FROM json.`/FileStore/tables/sample/sensor1-7.json`;
# MAGIC 
# MAGIC SELECT * FROM json_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT sensorName, sensorReadings.sensorChannel
# MAGIC FROM json_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sensorName, explode(sensorReadings) as Readings
# MAGIC FROM json_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sensorName, size(sensorReadings) as size
# MAGIC FROM json_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sensorName,Readings.* from (SELECT sensorName, explode(sensorReadings) as Readings
# MAGIC FROM json_temp_view)

# COMMAND ----------

