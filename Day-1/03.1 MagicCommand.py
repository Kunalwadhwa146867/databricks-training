# Databricks notebook source
# MAGIC %md
# MAGIC %sh: Allows you to run shell code in your notebook. To fail the cell if the shell command has a non-zero exit status, add the -e option. This command runs only on the Apache Spark driver, and not the workers. To run a shell command on all nodes, use an init script.
# MAGIC 
# MAGIC %fs: Allows you to use dbutils filesystem commands. For example, to run the dbutils.fs.ls command to list files, you can specify %fs ls instead. For more information, see Use %fs magic commands.
# MAGIC 
# MAGIC %md: Allows you to include various types of documentation, including text, images, and mathematical formulas and equations.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can use the language magic command %<language> at the beginning of a cell. The supported magic commands are: %python, %r, %scala, and %sql.

# COMMAND ----------

# MAGIC %md  TEST

# COMMAND ----------

# MAGIC %sql
# MAGIC select 1

# COMMAND ----------

# MAGIC %magic

# COMMAND ----------

# MAGIC %lsmagic

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC THIS is sample

# COMMAND ----------

# MAGIC %scala
# MAGIC val conf = new SparkConf()
# MAGIC              .setMaster("local[2]")
# MAGIC              .setAppName("CountingSheep")
# MAGIC val sc = new SparkContext(conf)

# COMMAND ----------

