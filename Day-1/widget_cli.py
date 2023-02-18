# Databricks notebook source
dbutils.notebook.run("widget", 600, {"Arg1": 670})

# COMMAND ----------

# MAGIC %run ./widget $Arg1=975

# COMMAND ----------

