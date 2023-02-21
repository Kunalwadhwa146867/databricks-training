# Databricks notebook source
# MAGIC %md
# MAGIC ### Adding Text Widget

# COMMAND ----------

dbutils.widgets.text(name='Arg1',label='Arg1',defaultValue='ABC')
print(dbutils.widgets.get('Arg1'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Multiselect Widget

# COMMAND ----------

dbutils.widgets.multiselect(name='Arg2',label='Days of week',defaultValue='Tuesday',choices=['Monday','Tuesday','Wednesday','Thursday','Friday'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Dropdown Widget

# COMMAND ----------

dbutils.widgets.dropdown(name='Arg3',label='Games',defaultValue='basketball',choices=['basketball','cricket','football'])

# COMMAND ----------

dbutils.widgets.remove('Arg1')

# COMMAND ----------

dbutils.widgets.removeAll()
