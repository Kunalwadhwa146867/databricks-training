# Databricks notebook source
# MAGIC %md
# MAGIC SparkContext has been available since Spark 1.x versions and it’s an entry point to Spark when you wanted to program and use Spark RDD. Most of the operations/methods or functions we use in Spark are comes from SparkContext  e.g. parallelize , textFile etc.	Be default Spark shell provides “sc” object which is an instance of SparkContext class. We can directly use this object where required

# COMMAND ----------

print(sc)

# COMMAND ----------

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("PySpark App")

sc = SparkContext.getOrCreate(conf=conf)
#sc = SparkContext("local", "First App")
print(sc)




# COMMAND ----------



# COMMAND ----------

lines= sc.parallelize(["Spark is a great example for distrubited computing.", "Spark is widely used for Structured and SemiStructured data processing"])

lines.collect()


# COMMAND ----------



# COMMAND ----------

lines= sc.parallelize(["Spark is a great example for distrubited computing.", "Spark is widely used for Structured and SemiStructured data processing"])
result = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda x,y: x+y).collect()
for (word, count) in result:
    print("%s: %i" % (word, count))

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls dbfs:/FileStore/tables/hiring5.txt

# COMMAND ----------

rdd = sc.textFile('dbfs:/FileStore/tables/hiring5.txt')
rdd1 = rdd.map(lambda x: x.split(' '))
rdd2 = rdd1.filter(lambda x : 'not' not in x)
rdd3 = rdd2.map(lambda x: len(x))
rdd4 = rdd3.max()
rdd3.collect() # Action
#rdd4 = rdd3.max()

# COMMAND ----------

#rdd = sc.textFile('dbfs:/FileStore/tables/hiring5.txt')
print(rdd.getNumPartitions())


# COMMAND ----------

print(spark)

# COMMAND ----------

sc.getConf().getAll()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC With Spark 2.0 a new class org.apache.spark.sql.SparkSession has been introduced to use which is a combined class for all different contexts we used to have prior to 2.0 (SQLContext and HiveContext e.t.c) release hence SparkSession can be used in replace with SQLContext and HiveContext.. SparkSession is an entry point to Spark and creating a SparkSession instance would be the first statement you would write to program with RDD, DataFrame and Dataset and SparkSession will be created using SparkSession.builder() builder patterns.
# MAGIC 
# MAGIC Spark Session also includes all the APIs available in different contexts –
# MAGIC Spark Context,
# MAGIC SQL Context,
# MAGIC Streaming Context,
# MAGIC Hive Context.

# COMMAND ----------

from pyspark.sql import SparkSession

spark1 = SparkSession.builder.master('loddcal[1]').appName("SimpleApp1").getOrCreate()
print(spark1.sparkContext)
sc=spark.sparkContext
lines= sc.parallelize(["Spark is a great example for distrubited computing.", "Spark is widely used for Structured and SemiStructured data processing"])
#lines.collect()





# COMMAND ----------

spark.conf.get("spark.app.name")

# COMMAND ----------

import os
print(os.getenv("PYSPARK_PYTHON"))

# COMMAND ----------

