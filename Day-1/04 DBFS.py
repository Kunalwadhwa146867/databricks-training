# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Databricks File System is distributed file system mounted on top Databricks cluster which allows for distributed storage. It offers following advantages - 
# MAGIC   1. Mount storage objects to access data without hassle
# MAGIC   2. Interact with object storage using file and directory semantics instead of using URLs
# MAGIC   3. Data can be persisted after usage. No risk of losing data after cluster terminates.
# MAGIC 
# MAGIC DBFS Root:
# MAGIC   The default storage location in DBFS is DBFS root. We have following locations in DBFS root as below-
# MAGIC   - /FileStore
# MAGIC   - /databricks-datasets
# MAGIC   - /databricks-results

# COMMAND ----------

# DBTITLE 1,Using %fs method to list dbfs
# MAGIC %fs ls /databricks-datasets/COVID

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore

# COMMAND ----------

# DBTITLE 1,Using dbutils method to list dbf
dbutils.fs.ls('/')

# COMMAND ----------

# DBTITLE 1,Example ----
display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/databricks-datasets/airlines'))

# COMMAND ----------

# MAGIC %fs ls /FileStore/

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/"))

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/COVID/IHME"))

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/COVID/IHME/2020_03_31.1"))

# COMMAND ----------

# MAGIC %fs ls dbfs:/

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %pwd

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:/tmp5/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore

# COMMAND ----------

# MAGIC %md
# MAGIC %fs and dbutils.fs read by default from root (dbfs:/). To read from the local filesystem, you must use file:/.

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls file:/tmp

# COMMAND ----------

# MAGIC %md
# MAGIC Upload the data to DBFS 

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/")

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/FileStore/tableasa

# COMMAND ----------

def get_dir_content(ls_path):
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isFile():
            yield dir_path.path
        elif dir_path.isDir() and ls_path != dir_path.path:
            yield from get_dir_content(dir_path.path)
    
list(get_dir_content('/databricks-datasets/COVID/CORD-19/2020-03-13'))

# COMMAND ----------

dbutils.fs.help("cp")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/tables/hiring1.txt","dbfs:/FileStore/tables/hiring5.txt")

# COMMAND ----------

# MAGIC %fs
# MAGIC cp dbfs:/FileStore/tables/hiring3.txt dbfs:/FileStore/tables/hiring5.txt

# COMMAND ----------

# MAGIC %fs
# MAGIC mv dbfs:/FileStore/tables/hiring.txt dbfs:/FileStore/tables/hiring5.txt

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/FileStore/tables/

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

