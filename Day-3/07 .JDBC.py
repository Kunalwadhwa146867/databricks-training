# Databricks notebook source
#mysql:mysql-connector-java:8.0.29

# COMMAND ----------

url="jdbc:mysql://mk-mysql-spark.mysql.database.azure.com:3306/sparkdb"

# COMMAND ----------


df = spark.read.format('jdbc').option("url", url) \
    .option("dbtable", 'zipcode') \
    .option("user", "spark") \
    .option("password", "Sprk123@") \
    .option("driver", "com.mysql.jdbc.Driver").load()

df.show()

# COMMAND ----------

qry = "select * from zipcode where zip = 622"
df = spark.read.format('jdbc').option("url", url) \
    .option("query", qry) \
    .option("user", "spark") \
    .option("password", "Sprk123@") \
    .option("driver", "com.mysql.jdbc.Driver").load()

df.show()

# COMMAND ----------

url="jdbc:mysql://mk-mysql-spark.mysql.database.azure.com:3306/sparkdb?user=spark&password=Sprk123@"
props = {"driver": "com.mysql.jdbc.Driver" }
df = spark.read.jdbc(url=url,table='zipcode',properties=props)
df.show()

# COMMAND ----------

url="jdbc:mysql://mk-mysql-spark.mysql.database.azure.com:3306/sparkdb"
props = {"driver": "com.mysql.jdbc.Driver", "user": "spark" , "password":"Sprk123@" }
df = spark.read.jdbc(url=url,table='zipcode',properties=props)
df.show()

# COMMAND ----------

url="jdbc:mysql://mk-mysql-spark.mysql.database.azure.com:3306/sparkdb"
props = {"driver": "com.mysql.jdbc.Driver", "user": "spark" , "password":"Sprk123@" }

query = "(select * from zipcode where zip =  901) Alias"
df = spark.read.jdbc(url=url,table=query,properties=props)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Writing to Database
# MAGIC 
# MAGIC There are three modes 1)ignore 2)append 3)overwrite

# COMMAND ----------

# Reading csv file
df_write = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/day4/uszips_A1.csv")

df_write = df_write.withColumnRenamed("county_name","country_name")
df_write.show()

# COMMAND ----------

df_write.write.mode('append').format('jdbc').option("url", url) \
    .option("dbtable", 'zipcode') \
    .option("user", "spark") \
    .option("password", "Sprk123@") \
    .option("driver", "com.mysql.jdbc.Driver").save()

# COMMAND ----------

# Using properties , jdbc interface
url="jdbc:mysql://mk-mysql-spark.mysql.database.azure.com:3306/sparkdb"
props = {"driver": "com.mysql.jdbc.Driver", "user": "spark" , "password":"Sprk123@" }
df_write.write.mode("append").jdbc(url=url,table='zipcode_10',properties=props)


# COMMAND ----------

# Using properties , jdbc interface
url="jdbc:mysql://mk-mysql-spark.mysql.database.azure.com:3306/sparkdb"
props = {"driver": "com.mysql.jdbc.Driver", "user": "spark" , "password":"Sprk123@" }
df_write.write.option("createTableColumnTypes", "state_id varchar(32), country_name VARCHAR(1024)").jdbc(url=url,table='zipcode200',properties=props)

# COMMAND ----------

import os

print(os.cpu_count())

# COMMAND ----------

df =df.repartition(6)

# COMMAND ----------

print(df.rdd.getNumPartitions())

# COMMAND ----------



df = spark.read.format('jdbc').option("url", url) \
    .option("dbtable", 'zipcode') \
    .option("partitionColumn", 'zip') \
    .option("lowerBound", 1) \
    .option("upperBound", 10000) \
    .option("numPartitions", 4) \
    .option("user", "spark") \
    .option("password", "Sprk123@") \
    .option("driver", "com.mysql.jdbc.Driver").load()

df.show()

# COMMAND ----------

df.write.format('jdbc').option("url", url) \
    .option("dbtable", 'zipcode161') \
    .option("partitionColumn", 'zip') \
    .option("lowerBound", 1) \
    .option("upperBound", 10000) \
    .option("numPartitions", 6) \
    .option("user", "spark") \
    .option("password", "Sprk123@") \
    .option("driver", "com.mysql.jdbc.Driver").save()


# COMMAND ----------

