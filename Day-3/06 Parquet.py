# Databricks notebook source
df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/day4/uszips.csv")
df.show()

# COMMAND ----------

df.write.parquet("/FileStore/tables/day4/uszips_111.parquet")

# COMMAND ----------

parDF=spark.read.parquet("/FileStore/tables/day4/uszips_4.parquet")
parDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC df.write.mode('append').parquet("/FileStore/tables/day4/uszips_1.parquet")
# MAGIC 
# MAGIC df.write.mode('overwrite').parquet("/FileStore/tables/day4/uszips_1.parquet")

# COMMAND ----------

parDF.createOrReplaceTempView("ParquetTable")
parkSQL = spark.sql("select * from ParquetTable where zip =631 ")
parkSQL.show()

# COMMAND ----------

#  create a temporary view or table directly on the parquet file instead of creating from DataFrame.

spark.sql("CREATE TEMPORARY VIEW ZIPC USING parquet OPTIONS (path \"/FileStore/tables/day4/uszips_1.parquet\")")
spark.sql("SELECT * FROM ZIPC").show()

# COMMAND ----------

df = spark.sql("SELECT * FROM parquet.`/FileStore/tables/day4/uszips_1.parquet` where zip=603")
df.show()

# COMMAND ----------

# Partition by
df.write.partitionBy("city", "zip").parquet("/FileStore/tables/day4/uszips_141.parquet")

# COMMAND ----------

parDF=spark.read.parquet("/FileStore/tables/day4/uszips_141.parquet/city=Aguada/zip=602")
parDF.show()

# COMMAND ----------

#  create a temporary view or table directly on the parquet file instead of creating from DataFrame.

spark.sql("CREATE TEMPORARY VIEW ZIPC2 USING parquet OPTIONS (path \"/FileStore/tables/day4/uszips_9.parquet/city=Aguada\")")
spark.sql("SELECT * FROM ZIPC2").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Schema Merging
# MAGIC 
# MAGIC setting data source option mergeSchema to true when reading Parquet files, or
# MAGIC 
# MAGIC setting the global SQL option spark.sql.parquet.mergeSchema to true.

# COMMAND ----------

from pyspark.sql import Row
squaresDF = spark.createDataFrame(sc.parallelize(range(1, 6))
                                  .map(lambda i: Row(single=i, square=i ** 2)))
squaresDF.show()
squaresDF.printSchema()


# COMMAND ----------

from pyspark.sql import Row
cubeDF = spark.createDataFrame(sc.parallelize(range(1, 6))
                                  .map(lambda i: Row(single=i, cube=i ** 3)))
cubeDF.show()
cubeDF.printSchema()

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /FileStore/tables/day4/merget19

# COMMAND ----------

squaresDF.write.parquet("/FileStore/tables/day4/merget19/XYZ=uyt")

# COMMAND ----------

cubeDF.write.parquet("/FileStore/tables/day4/merget19/XYZ=asdsadsad")

# COMMAND ----------

mergedDF = spark.read.option("mergeSchema", "true").parquet("/FileStore/tables/day4/merget19")
mergedDF.printSchema()
mergedDF.show()

# COMMAND ----------

print(mergedDF.rdd.getNumPartitions())

# COMMAND ----------

df =df.repartition(4)

# COMMAND ----------

df.write.parquet("/FileStore/tables/day4/uszips_10000.parquet")

# COMMAND ----------

