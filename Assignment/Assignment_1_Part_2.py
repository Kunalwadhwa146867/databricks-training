# Databricks notebook source
# MAGIC %md
# MAGIC ## Map transformation

# COMMAND ----------

rdd = sc.parallelize(['A','B','C','D','E'],2)
print(rdd.glom().collect())
new_rdd = rdd.map(lambda ele: ele+'B')
print(new_rdd.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter transformation

# COMMAND ----------

filtered_rdd = rdd.filter(lambda ele: ele not in ('C','E','B'))
print(filtered_rdd.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatmap transformation

# COMMAND ----------

flatmap_rdd = rdd.flatMap(lambda ele: (ele+'A',ele+'B',ele+'C'))
flatmap_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## MapPartitions transformation - apply function to a whole partition and function accepts one argument only
# MAGIC ## MapPartitionsWithIndex transformation - apply function to a whole partition along with index and function accepts two arguments

# COMMAND ----------

def fun(p):
    for ele in p:
        yield [ele+"Q"]

def fun1(i,p):
    for ele in p:
        yield [str(i)+ele+"Q"]
print(rdd.getNumPartitions())
print(rdd.collect())
mapPartitions = rdd.mapPartitions(fun)
mapPartitionsWithIndex = rdd.mapPartitionsWithIndex(fun1)
print(mapPartitions.getNumPartitions())
print(mapPartitions.collect())
print(mapPartitionsWithIndex.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## groupBy transformation

# COMMAND ----------

name_rdd = sc.parallelize(['Kunal','Ashish','Krishna','Rahul','Amit'],2)
name_rdd.collect()
grouped_name_rdd = name_rdd.groupBy(lambda ele: ele[0])
for (k,v) in grouped_name_rdd.collect():
    print(k,' - ',list(v))

# COMMAND ----------

# MAGIC %md
# MAGIC ##sortBy transformation

# COMMAND ----------

sorted_name_rdd = name_rdd.sortBy(lambda ele: ele[0])
sorted_name_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## union transformation

# COMMAND ----------

rdd1 = sc.parallelize(['a','b','c'],2)
rdd2 = sc.parallelize(['d','e','a'],2)

union_rdd = rdd1.union(rdd2)
print(union_rdd.getNumPartitions())
union_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## zip transformation - Returns a new RDD containing pairs whose key is the item in the original RDD, and whose value is that item’s corresponding element (same partition, same index) in a second RDD

# COMMAND ----------

rdd1 = sc.parallelize(['a','b','c'],2)
rdd2 = sc.parallelize(['d','e','a'],2)

zipped_rdd = rdd1.zip(rdd2)
print(zipped_rdd.getNumPartitions())
zipped_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##cartesian transformation

# COMMAND ----------

rdd1 = sc.parallelize(['a','b','c'],2)
rdd2 = sc.parallelize(['d','e','a'],2)

cartesian_rdd = rdd1.cartesian(rdd2)
print(cartesian_rdd.getNumPartitions())
cartesian_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## distinct transformation

# COMMAND ----------

union_rdd.distinct().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## intersection transformation

# COMMAND ----------

rdd1 = sc.parallelize(['a','b','c'],2)
rdd2 = sc.parallelize(['d','e','a'],2)
rdd1.intersection(rdd2).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## zipWithIndex transformation

# COMMAND ----------

rdd1 = sc.parallelize(['a','b','c'],2)
rdd1.zipWithIndex().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## zipWithUniqueId transformation

# COMMAND ----------

sc.parallelize(['a','b','c'],2).zipWithUniqueId().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## flatMapValues

# COMMAND ----------

pair_rdd = sc.parallelize([('a',('b','c','d')),('e',('f','g','h'))])
def f(x): return x
pair_rdd.flatMapValues(f).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## groupByKey

# COMMAND ----------

pair_rdd = sc.parallelize([('a',1),('b',2),('c',3),('a',1),('b',2),('c',3)])
grouped_rdd = pair_rdd.groupByKey()
for ele in grouped_rdd.collect():
    print(ele[0],list(ele[1]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## reduceByKey - merges locally on each mapper before sending results to a reducer, similarly to a “combiner” in MapReduce

# COMMAND ----------

from operator import add
reduced_rdd = pair_rdd.reduceByKey(add)
reduced_rdd.collect()
