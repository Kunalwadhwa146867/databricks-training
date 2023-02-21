# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains

# COMMAND ----------

# Reading csv file
df = spark.read.csv("/FileStore/tables/day4/uszips.csv")
df.printSchema()


# COMMAND ----------

df.show()

# COMMAND ----------

# Reading csv file
df = spark.read.option("header", True).csv("/FileStore/tables/day4/uszips.csv")
df.show()

# COMMAND ----------

# Reading csv file
df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/day4/uszips.csv")
df.printSchema()

# COMMAND ----------

schema = StructType() \
         .add("zip",IntegerType(),True) \
         .add("lat",DoubleType(),True) \
         .add("lng",DoubleType(),True) \
         .add("city",StringType(),True) \
         .add("state_id",StringType(),True) \
         .add("county_name",StringType(),True) \
        .add("county_name1",StringType(),True) 

df_with_schema = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("/FileStore/tables/day4/uszips.csv")

df_with_schema.printSchema()        
df_with_schema.show()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/day4/uszips.csv

# COMMAND ----------

# Reading csv file using load
df = spark.read.format("csv").option("header", True).option("delimiter", ",").load("/FileStore/tables/day4/uszips.csv")
df.show()

# COMMAND ----------

# Reading csv file using load
df = spark.read.format("csv").option("header", True).option("delimiter", "|").load("/FileStore/tables/day4/uszips_pipe.csv")
df.show()

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

df.write.mode('overwrite').option("header" , True).csv("/FileStore/tables/day4/uszips_pipe_n101.csv")



# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/day4

# COMMAND ----------

# Reading csv file using load
df.write.format("csv").option("delimiter", "|").save("/FileStore/tables/day4/uszips_pipe_n180.csv")


# COMMAND ----------

# Reading csv file using load
df = spark.read.format("csv").option("header", True).option("delimiter", ",").load("/FileStore/tables/day4/uszips_pipe_n180.csv")
df.show()

# COMMAND ----------

df.write.format??

# COMMAND ----------

df.write.csv??

# COMMAND ----------


# ReaWritingding csv file using load
df.write.format("csv").option("delimiter", "|").option("linesep", "~").save("/FileStore/tables/day4/uszips_pipe_n130.csv")

# COMMAND ----------

