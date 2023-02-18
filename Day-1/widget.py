# Databricks notebook source
dbutils.widgets.text(
  name='Arg1',
  defaultValue='ABC',
  label='Arg1'
)

print(dbutils.widgets.get("Arg1"))

# COMMAND ----------

dbutils.widgets.remove('Arg1')

# COMMAND ----------

dbutils.widgets.multiselect(
  name='days_multiselect',
  defaultValue='Tuesday',
  choices=['Monday', 'Tuesday', 'Wednesday', 'Thursday',
    'Friday', 'Saturday', 'Sunday'],
  label='Days of the Week'
)

print(dbutils.widgets.get("days_multiselect"))

# COMMAND ----------

dbutils.widgets.dropdown(
  name='toys_dropdown',
  defaultValue='basketball',
  choices=['alphabet blocks', 'basketball', 'cape', 'doll'],
  label='Toys'
)

print(dbutils.widgets.get("toys_dropdown"))

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

