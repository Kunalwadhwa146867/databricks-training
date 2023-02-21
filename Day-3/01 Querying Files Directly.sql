-- Databricks notebook source
-- MAGIC %md <i18n value="ba5cb184-9677-4b79-b000-f42c5fff9044"/>
-- MAGIC 
-- MAGIC 
-- MAGIC # Extracting Data Directly from Files
-- MAGIC 
-- MAGIC In this notebook, you'll learn to extract data directly from files using Spark SQL on Databricks.

-- COMMAND ----------

-- MAGIC %md <i18n value="00f263ec-293b-4adf-bf4b-b81f04de6e31"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Note that our source directory contains many JSON files.

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls dbfs:/FileStore/tables/sample/iris.json

-- COMMAND ----------

-- MAGIC %md <i18n value="8e04a0d2-4d79-4547-b2b2-765eefaf6285"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Query a Single File
-- MAGIC 
-- MAGIC To query the data contained in a single file, execute the query with the following pattern:
-- MAGIC 
-- MAGIC <strong><code>SELECT * FROM file_format.&#x60;/path/to/file&#x60;</code></strong>
-- MAGIC 
-- MAGIC Make special note of the use of back-ticks (not single quotes) around the path.

-- COMMAND ----------

SELECT * FROM json.`dbfs:/FileStore/tables/sample/jsn/iris.json`

-- COMMAND ----------

-- MAGIC %md <i18n value="01cd5a22-a236-4ef6-bf65-7c40379d7ef9"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Query a Directory of Files
-- MAGIC 
-- MAGIC Assuming all of the files in a directory have the same format and schema, all files can be queried simultaneously by specifying the directory path rather than an individual file.

-- COMMAND ----------

SELECT * FROM json.`dbfs:/FileStore/tables/sample/jsn/*.json`

-- COMMAND ----------

-- MAGIC %md <i18n value="46590bb8-cf4b-4c3d-a9c6-e431bad4a5e9"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Create References to Files
-- MAGIC This ability to directly query files and directories means that additional Spark logic can be chained to queries against files.
-- MAGIC 
-- MAGIC When we create a view from a query against a path, we can reference this view in later queries. Here, we'll create a temporary view, but you can also create a permanent reference with regular view.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW json_temp_view
AS SELECT * FROM json.`dbfs:/FileStore/tables/sample/jsn`;

SELECT * FROM json_temp_view

-- COMMAND ----------

-- MAGIC %md <i18n value="0a627f4b-ec2c-4002-bf9b-07a788956f03"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Extract Text Files as Raw Strings
-- MAGIC 
-- MAGIC When working with text-based files (which include JSON, CSV, TSV, and TXT formats), you can use the **`text`** format to load each line of the file as a row with one string column named **`value`**. This can be useful when data sources are prone to corruption and custom text parsing functions will be used to extract value from text fields.

-- COMMAND ----------

SELECT * FROM text.`dbfs:/FileStore/tables/sample/jsn`

-- COMMAND ----------

-- MAGIC %md <i18n value="ffae0f7a-b956-431d-b1cb-6d2be33b4f6c"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Extract the Raw Bytes and Metadata of a File
-- MAGIC 
-- MAGIC Some workflows may require working with entire files, such as when dealing with images or unstructured data. Using **`binaryFile`** to query a directory will provide file metadata alongside the binary representation of the file contents.
-- MAGIC 
-- MAGIC Specifically, the fields created will indicate the **`path`**, **`modificationTime`**, **`length`**, and **`content`**.

-- COMMAND ----------

SELECT * FROM binaryFile.`dbfs:/FileStore/tables/sample/jsn`

-- COMMAND ----------

