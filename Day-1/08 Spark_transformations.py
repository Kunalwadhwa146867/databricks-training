# Databricks notebook source
"""Spark Transformations

Essential Core & Intermediate Spark Operations

1.) General

• map
• filter
• flatMap
• mapPartitions
• mapPartitionsWithIndex
• groupBy
• sortBy
"""

# COMMAND ----------

# MAGIC %md
# MAGIC map() -- Return a new RDD by applying a function to each element of this RDD

# COMMAND ----------

x = sc.parallelize(["b", "a", "c"],2)
y = x.map(lambda z: (z, 1))
print("Map Transformation :",end='\n')
print("x: ",x.glom().collect())
print("y: ",y.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC collect() --- Return all items in the RDD to the driver in a single list

# COMMAND ----------

# MAGIC %md
# MAGIC filter() -- Return a new RDD containing only the elements that satisfy a predicate

# COMMAND ----------

x = sc.parallelize([1,2,3])
y = x.filter(lambda x: x%2 == 1) #keep odd values
print("Filter Transformation :")
print("x: ",x.collect())
print("y: ",y.collect())


# COMMAND ----------

# MAGIC %md
# MAGIC flatmap() -- Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results

# COMMAND ----------

x = sc.parallelize([1,2,3])
y = x.flatMap(lambda x: (x, x*100, 42))
z = x.map(lambda x: (x, x*100, 42))
print("FlatMap Transformation :")
print("x: ",x.collect())
print("y: ",y.collect())
print("z: ",z.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC mapPartitions() -- Return a new RDD by applying a function to each partition of this RDD

# COMMAND ----------

x = sc.parallelize([1,2,3], 2)
def f(iterator): yield sum(iterator); yield 42
y = x.mapPartitions(f)

print("No. of partitions :",x.getNumPartitions())
print("x: ",x.glom().collect())
print()
print("y without glom: ",y.collect())
# glom() Return an RDD created by coalescing all elements within each partition into a list.
print("y with glom: ",y.glom().collect())


# COMMAND ----------

# MAGIC %md
# MAGIC mapPartitionsWithIndex -- Return a new RDD by applying a function to each partition of this RDD, while tracking the index of the original partition

# COMMAND ----------

x = sc.parallelize([1,2,3], 2) # with 2 partitions
def f(partitionIndex, iterator): yield (partitionIndex, sum(iterator))
y = x.mapPartitionsWithIndex(f)
# glom() flattens elements on the same partition

print("Results with 2 partitons")
print("x: ",x.glom().collect())
print("y: ",y.glom().collect())
print()



a = sc.parallelize([1,2,3], 5) # with 2 partitions
def f(partitionIndex, iterator): yield (partitionIndex, sum(iterator))
b = a.mapPartitionsWithIndex(f)
print("Results with 5 partitons")
print("a: ",a.glom().collect())
print("b: ",b.glom().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC groupBy() -- Group the data in the original RDD. Create pairs where the key is the output of a user function, and the value is all items for which the function yields this key.

# COMMAND ----------

x = sc.parallelize(['John', 'Fred', 'Anna', 'James'])
y = x.groupBy(lambda w: w[0])

print( [(k, list(v)) for (k, v) in y.collect()])

# COMMAND ----------

# MAGIC %md
# MAGIC sortBy -- sorts the data in the original RDD. It receives key-value pairs (K, V) as an input, sorts the elements in ascending or descending order and generates a dataset in an order

# COMMAND ----------

# sortBy

x = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
y = sc.parallelize(x)

sort_by_index_1 = y.sortBy(lambda x: x[0])
print("sort_by_index_1 : ",sort_by_index_1.collect())

sort_by_index_2 = y.sortBy(lambda x: x[1])
print("sort_by_index_2 : ",sort_by_index_2.collect())

# COMMAND ----------

"""
2.) Set Theory / Relational
• union
• intersection
• subtract
• distinct
• cartesian
• zip
"""

# COMMAND ----------

# MAGIC %md
# MAGIC union() --  Return a new RDD containing all items from two original RDDs. Duplicates are not called.

# COMMAND ----------



x = sc.parallelize([1,2,3], 2)
print("x : ",x.glom().collect())

y = sc.parallelize([3,4], 1)
print("y : ",y.glom().collect())

z = x.union(y)
print("z : ",z.glom().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC zip() -- Return a new RDD containing pairs whose key is the item in the original RDD, and whose value is that item’s corresponding element (same partition, same index) in a second RDD

# COMMAND ----------

x = sc.parallelize([1, 2, 3])
y = x.map(lambda n:n*n)
print("y : ",y.collect())
z = x.zip(y)
print("Zip output : ",z.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC cartesian() -- generates a cartessian product of the elements

# COMMAND ----------

x = sc.parallelize([1, 2, 3])
y = x.map(lambda n:n*n)
print("y : ",y.collect())
z = x.cartesian(y)
print("cartesian output : ",z.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC distinct() -- finds the distinct elements from a set

# COMMAND ----------

x = sc.parallelize([1, 2, 3,3,2,4,5,5])
print("x : ",x.collect())
z = x.distinct()
print("Distinct output : ",z.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC intersection() -- performs intersect of two sets

# COMMAND ----------

x = sc.parallelize([1,2,3],1)
print("x : ",x.glom().collect())

y = sc.parallelize([3,4],1)
print("y : ",y.glom().collect())

z = x.intersection(y)
print("z : ",z.glom().collect())

# COMMAND ----------

"""
3.) Data Structure / I/O
• keyBy
• zipWithIndex
• zipWithUniqueID
• zipPartitions
• coalesce
• repartition
• repartitionAndSortWithinPartitions
• pipe

"""

#colease -- avoid shuffling of data. --- it can only decrease the no.of partitons

x = sc.parallelize([1,2,3,4,5,6,7], 4)
y = x.coalesce(2)
print("x : ",x.glom().collect())
print("y : ",y.glom().collect())


# COMMAND ----------

#repartition -- will do data shuffling, can increase and decrease the no. of partitons

x = sc.parallelize([1,2,3,4,5,6,7], 4)
y = x.repartition(2)
print("x : ",x.glom().collect())
print("y : ",y.glom().collect())

# COMMAND ----------

#pipe - u can run external code using this

sc.parallelize(['1', '2', '', '3']).pipe('cat').collect()

# COMMAND ----------

#zipWithIndex -- Zips this RDD with its element indices. The ordering is first based on the partition index and then the ordering of items within each partition. So the first item in the first partition gets index 0, and the last item in the last partition receives the largest index.

x = sc.parallelize(["a", "b", "c", "d"], 3)
print("x : ",x.glom().collect())
y = x.zipWithIndex()
print("y : ",y.glom().collect())

# COMMAND ----------

#zipWithUniqueId --Zips this RDD with generated unique Long ids.

sc.parallelize(["a", "b", "c", "d", "e"], 3).zipWithUniqueId().collect()

# COMMAND ----------

#repartitionAndSortWithinPartitions -- Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys.

x = sc.parallelize([(0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)],4)
print("x : ",x.glom().collect())

#here lambda function is deciding in which partion the data would go. lambda fn depend on lexicographic ordering defined on Python
y = x.repartitionAndSortWithinPartitions(5, lambda x: x % 5, True)
print("y : ",y.glom().collect())



# COMMAND ----------

"""
Essential Core & Intermediate PairRDD Operations

1) General
• flatMapValues
• groupByKey
• reduceByKey
• reduceByKeyLocally
• aggregateByKey
• sortByKey
• combineByKey

"""

#flatMapValues --  Pass each value in the key-value pair RDD through a flatMap function without changing the keys; this also retains the original RDD's partitioning.

x = sc.parallelize([("a", ["x", "y", "z"]), ("b", ["p", "r"])])
def f(x): return x

x.flatMapValues(f).glom().collect()

# COMMAND ----------

# groupByKey -- Group the values for each key in the original RDD. Create a new pair where the original key corresponds to this collected group of values.

x = sc.parallelize([('B',5),('B',4),('A',3),('A',2),('A',1)])
y = x.groupByKey()
print(x.collect())
print(list((j[0], list(j[1])) for j in y.collect()))

# COMMAND ----------

# reduceByKey -- This will perform the merging locally on each mapper before sending results to a reducer, similarly to a “combiner” in MapReduce.

from operator import add
rdd = sc.parallelize([('B',5),('B',4),('A',3),('A',2),('A',1)])
print("rdd : ",rdd.glom().collect())

y = rdd.reduceByKey(add)
print("y : ", y.glom().collect())
# reduceByKeyLocally  -- Merge the values for each key using an associative and commutative reduce function, but return the results immediately to the master as a dictionary.
z= rdd.reduceByKeyLocally(add)
print("y : ", z.items())


# COMMAND ----------

#Append: Adds its argument as a single element to the end of a list. The length of the list increases by one.

my_list = ['geeks', 'for', 'geeks']
another_list = [6]
my_list.append(another_list)
print ("append results : ",my_list)
another_list = [0]
my_list.append(another_list)
print ("append results : ",my_list)
another_list = [4]
my_list.append(another_list)
print ("append results : ",my_list)
print()

#extend(): Iterates over its argument and adding each element to the list and extending the list. The length of the list increases by number of elements in it’s argument.

my_list = ['geeks', 'for', 'geeks']
another_list = [6, 0, 4, 1]
my_list.extend(another_list)
print ("extend results : ",my_list)
print()


#combineByKey

x = sc.parallelize([("a", 1), ("b", 1), ("a", 3), ("a", 5), ("a",7) ],3)

# functions are created in order of execution in combineByKey

#creates single-element list for each element 
def to_list(a):
    return [a]

#append the above single element in one list basis on key
def append(a, b):
    a.append(b)
    return a

#Now, we are extending above 2 list
def extend(a, b):
    a.extend(b)
    return a

print("x : ", x.glom().collect())
x.combineByKey(to_list, append, extend).glom().collect()

# COMMAND ----------

#sortBykey

tmp2 = [('MARY', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)]

sc.parallelize(tmp2).sortByKey(True, 3, keyfunc=lambda k:k.lower()).collect() #some issue with keyFunc, it doesn't behave as it is supposed to.

# COMMAND ----------

#aggregateByKey

rdd = sc.parallelize([['a', 1], ['a', 2], ['b', 1], ['a', 3], ['b', 3], ['c',5]], 5)

print("rdd : ", rdd.glom().collect())

diff = 0


def sub_op(x,y):
    if x>y:
        return x - y
    return y - x

def comb_op(x,y):
    if x > y:
        return x - y
    return y - x

rdd.aggregateByKey(diff,sub_op,comb_op).glom().collect()

# COMMAND ----------

from operator import add
rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 3), ("a", 5), ("a",7) ],3)
print("rdd : ",rdd.glom().collect())


#append the above single element in one list basis on key
def append(a, b):
    #a.append(b)
    return a * b


y = rdd.reduceByKey(append)
print("y : ", y.glom().collect())


# COMMAND ----------

"""
2. Data Structure
"""
#partitionBy

x = sc.parallelize([('J','James'),('F','Fred'),
('A','Anna'),('J','John')], 4)
y = x.partitionBy(3, lambda w: 0 if w[0] < 'H' else 1)
print (x.glom().collect())
print (y.glom().collect())

# COMMAND ----------

"""
3.) Set Theory / Relational

• cogroup (=groupWith)
• join
• subtractByKey
• fullOuterJoin
• leftOuterJoin
• rightOuterJoin
"""


"""
****Join****

Return an RDD containing all pairs of elements with matching keys in self and other.

Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in self and (k, v2) is in other.

Performs a hash join across the cluster.
"""

x = sc.parallelize([("a", 1), ("b", 2),("c",1)])
y = sc.parallelize([("a", 3), ("a", 4), ("b", 5),("d",1)])
z = x.join(y)
print("Join : ",z.collect()) 

j = x.leftOuterJoin(y)
print("left outer join : ",j.collect()) 

k = x.rightOuterJoin(y)
print("right outer join : ",k.collect()) 

l = x.fullOuterJoin(y)
print("full outer join : ",l.collect()) 

# COMMAND ----------

#cogroup -- For each key k in self or other, return a resulting RDD that contains a tuple with the list of values for that key in self as well as other.

x = sc.parallelize([("a", 1), ("b", 4)])
y = sc.parallelize([("a", 2)])


[(x, tuple(map(list, y))) for x, y in list(x.cogroup(y).collect())]

# COMMAND ----------

"""
ACTIONS

1.) General
• reduce
• collect
• aggregate
• fold
• first
• take
• top
• collectAsMap

"""

#reduce -- Aggregate all the elements of the RDD by applying a user function pairwise to elements and partial results, and returns a result to the driver
x = sc.parallelize([1,2,3,4])
y = x.reduce(lambda a,b: a+b)
print(x.collect())
print(y)
print()

# first -- The function by default returns the first values it sees. It will return the first non-null value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
print("first : ",x.first())

#take -- Returns the first x rows as a list of Row.
print("take : ",x.take(2))

#top -- This method should only be used if the resulting array is expected to be small, as all the data is loaded into the driver’s memory. It returns the list sorted in descending order.
print("top : ",x.top(3))


#fold -- Aggregate the elements of each partition, and then the results for all the partitions, using a given associative function and a neutral “zero value.”

from operator import add
print("fold : ",x.fold(0, add))



# COMMAND ----------

#collectAsMap -- Return the key-value pairs in this RDD to the master as a dictionary. This method should only be used if the resulting data is expected to be small, as all the data is loaded into the driver’s memory.

m = sc.parallelize([[1, 2], [3, 4]]).collectAsMap()
print(m)

# COMMAND ----------

#aggregate -- Aggregate the elements of each partition, and then the results for all the partitions, using a given combine functions and a neutral “zero value.”

seqOp = (lambda local_result, list_element: (local_result[0] + list_element, local_result[1] + 1) )
combOp = (lambda some_local_result, another_local_result: (some_local_result[0] + another_local_result[0], some_local_result[1] + another_local_result[1]) )


#seqOp = lambda data, item: (data[0] + [item], data[1] + item)
#combOp = lambda d1, d2: (d1[0] + d2[0], d1[1] + d2[1])

x = sc.parallelize([1,2,3,4],2)
print(x.glom().collect())
#y = x.aggregate(([], 0), seqOp, combOp)
y = x.aggregate((0, 0), seqOp, combOp)
print(y)

# COMMAND ----------

"""
2.) Math / Statistical
• count
• takeSample
• max
• min
• sum
• mean
• variance
• stdev
• sampleVariance

"""


#count -- Return the number of elements in this RDD.

x = sc.parallelize([1,2,3,4,6])
print("count : ", x.count())

print("max : ", x.max())

print("min : ", x.min())

print("sum : ", x.sum())

print("mean : ", x.mean())

print("variance : ", x.variance())

print("stdev : ", x.stdev())

print("sampleVariance : ", x.sampleVariance())

# COMMAND ----------

"""

3.) Set Theory / Relational
• takeOrdered
"""

x= sc.parallelize([10, 1, 2, 9, 3, 4, 5, 6, 7],2)
print("using default Key as None : ",x.takeOrdered(6))



print("Using Manulay key :",x.takeOrdered(6, key=lambda x: -x))

# COMMAND ----------

"""
Data Structure /I/O
"""
# saveAsTextFile

dbutils.fs.rm("/temp/demo", True)
x = sc.parallelize([2,4,1])
x.saveAsTextFile("/temp/demo")
y = sc.textFile("/temp/demo")
print(y.collect())

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets'))

#f = open('databricks-datasets/README.md', 'r')
#print(f.read())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls databricks-datasets/airlines/

# COMMAND ----------

# MAGIC %python
# MAGIC diamonds = spark.read.csv("/databricks-datasets/airlines/part-00000", header="true", inferSchema="true")
# MAGIC #diamonds.write.format("delta").mode("overwrite").save("/delta/diamonds")
# MAGIC diamonds.show(4)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("Spark UDF")\
        .getOrCreate();

schema = StructType([
StructField('accepter_id', LongType(), False),
StructField('requester_id',LongType(),False)
])


# COMMAND ----------

userData = spark.createDataFrame([
    (1,2),
    (1,3),
    (1,4),
    (5,1),
    (4,5),
    (4,3),
    (2,9),
    (1,9)
    
], schema)
print("Table")
print(userData.show())

userData.createOrReplaceTempView("user_data")



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select user_id , count(user_id) as friends
# MAGIC from (
# MAGIC select accepter_id as user_id 
# MAGIC from user_data 
# MAGIC 
# MAGIC union all 
# MAGIC 
# MAGIC select requester_id as user_id from user_data 
# MAGIC )
# MAGIC group by user_id order by friends  desc
# MAGIC limit 1

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("Spark")\
        .getOrCreate();

schema = StructType([
StructField('id', LongType(), False),
StructField('num',LongType(),False)
])


# COMMAND ----------

userData = spark.createDataFrame([
    (1,1),
    (2,1),
    (3,1),
    (4,2),
    (5,1),
    (6,2),
    (7,2)
    
], schema)
print("Table")
print(userData.show())

userData.createOrReplaceTempView("user_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC with intermdt as
# MAGIC (select 
# MAGIC id, num,
# MAGIC LAG(num) over ( order by id) as previous_row,
# MAGIC num as current_row,
# MAGIC LEAD(num) over ( order by id asc) as next_row
# MAGIC 
# MAGIC 
# MAGIC from user_data
# MAGIC )
# MAGIC 
# MAGIC select 
# MAGIC  distinct (num)
# MAGIC  from intermdt
# MAGIC  where num = previous_row and num = current_row and num = next_row

# COMMAND ----------

