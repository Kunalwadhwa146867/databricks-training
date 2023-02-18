# Databricks notebook source
## Create a data frame from a csv file
from pyspark.sql import SparkSession

spark = SparkSession\
.builder\
.appName("Basic Structured Operations")\
.master("local[2]")\
.getOrCreate()

online_retail_df = spark.read.format("csv")\
.option("header", "true")\
.load("/FileStore/tables/online_retail.csv")

online_retail_df.show(5, False)

# COMMAND ----------

## Create a data frame from custom inpiuts data
from pyspark.sql.functions import col, column
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, Row

data_details = [
    Row("John", "Doe", 56),
    Row("Foo", "Bar", 23)
]

Schema = StructType(
    [
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("age", IntegerType(), True)
    ]
)

created_df = spark.createDataFrame(data_details, schema=Schema)
created_df.show()

# COMMAND ----------

