-- Databricks notebook source
-- MAGIC %md <i18n value="469222b4-7728-4980-a105-a850e5234a3c"/>
-- MAGIC 
-- MAGIC 
-- MAGIC # Providing Options for External Sources
-- MAGIC While directly querying files works well for self-describing formats, many data sources require additional configurations or schema declaration to properly ingest records.

-- COMMAND ----------

-- MAGIC %md <i18n value="7ba475f1-5a89-46ee-b400-b8846699d5b5"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## When Direct Queries Don't Work 
-- MAGIC 
-- MAGIC While views can be used to persist direct queries against files between sessions, this approach has limited utility.
-- MAGIC 
-- MAGIC CSV files are one of the most common file formats, but a direct query against these files rarely returns the desired results.

-- COMMAND ----------

SELECT * FROM csv.`dbfs:/FileStore/tables/day4/uszips_pipe.csv`

-- COMMAND ----------

-- MAGIC %md <i18n value="7ae3fe20-46e7-42be-95d6-4f933b758ef7"/>
-- MAGIC 
-- MAGIC 
-- MAGIC We can see from the above that:
-- MAGIC 1. The header row is being extracted as a table row
-- MAGIC 1. All columns are being loaded as a single column
-- MAGIC 1. The file is pipe-delimited (**`|`**)

-- COMMAND ----------

SELECT * FROM csv.`dbfs:/FileStore/tables/online_retail.csv`

-- COMMAND ----------

-- MAGIC %md <i18n value="f371955b-5e96-41d3-b413-ed5bb51820fc"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Registering Tables on External Data with Read Options
-- MAGIC 
-- MAGIC While Spark will extract some self-describing data sources efficiently using default settings, many formats will require declaration of schema or other options.
-- MAGIC 
-- MAGIC While there are many <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-using.html" target="_blank">additional configurations</a> you can set while creating tables against external sources, the syntax below demonstrates the essentials required to extract data from most formats.
-- MAGIC 
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE table_identifier (col_name1 col_type1, ...)<br/>
-- MAGIC USING data_source<br/>
-- MAGIC OPTIONS (key1 = val1, key2 = val2, ...)<br/>
-- MAGIC LOCATION = path<br/>
-- MAGIC </code></strong>
-- MAGIC 
-- MAGIC Note that options are passed with keys as unquoted text and values in quotes. Spark supports many <a href="https://docs.databricks.com/data/data-sources/index.html" target="_blank">data sources</a> with custom options, and additional systems may have unofficial support through external <a href="https://docs.databricks.com/libraries/index.html" target="_blank">libraries</a>. 

-- COMMAND ----------

-- MAGIC %md <i18n value="ea38261e-12c1-4bba-9670-9c4144c19494"/>
-- MAGIC 
-- MAGIC 
-- MAGIC The cell below demonstrates using Spark SQL DDL to create a table against an external CSV source, specifying:
-- MAGIC 1. The column names and types
-- MAGIC 1. The file format
-- MAGIC 1. The delimiter used to separate fields
-- MAGIC 1. The presence of a header
-- MAGIC 1. The path to where this data is stored

-- COMMAND ----------

CREATE TABLE uszips_csv
  (zip INTEGER, lat DOUBLE, lng DOUBLE, city STRING, state_id STRING, country_name STRING )
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "dbfs:/FileStore/tables/day4/uszips_pipe.csv"

-- COMMAND ----------

select * from uszips_csv

-- COMMAND ----------

-- MAGIC %md <i18n value="a9b747d0-1811-4bbe-b6d6-c89671289286"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Note that no data has moved during table declaration. 
-- MAGIC 
-- MAGIC Similar to when we directly queried our files and created a view, we are still just pointing to files stored in an external location.
-- MAGIC 
-- MAGIC Run the following cell to confirm that data is now being loaded correctly.

-- COMMAND ----------

SELECT * FROM uszips_csv

-- COMMAND ----------

SELECT COUNT(*) FROM uszips_csv

-- COMMAND ----------

-- MAGIC %md <i18n value="d0ad4268-b339-40cc-88be-1b07e9759d5e"/>
-- MAGIC 
-- MAGIC 
-- MAGIC All the metadata and options passed during table declaration will be persisted to the metastore, ensuring that data in the location will always be read with these options.
-- MAGIC 
-- MAGIC **NOTE**: When working with CSVs as a data source, it's important to ensure that column order does not change if additional data files will be added to the source directory. Because the data format does not have strong schema enforcement, Spark will load columns and apply column names and data types in the order specified during table declaration.
-- MAGIC 
-- MAGIC Running **`DESCRIBE EXTENDED`** on a table will show all of the metadata associated with the table definition.

-- COMMAND ----------

DESCRIBE EXTENDED uszips_csv

-- COMMAND ----------

-- MAGIC %md <i18n value="39ee6311-f095-4f86-8fe5-c5be569b6a10"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Limits of Tables with External Data Sources
-- MAGIC Note that whenever we're defining tables or queries against external data sources, we **cannot** expect the performance guarantees associated with Delta Lake and Lakehouse.
-- MAGIC 
-- MAGIC For example: while Delta Lake tables will guarantee that you always query the most recent version of your source data, tables registered against other data sources may represent older cached versions.

-- COMMAND ----------

-- MAGIC %md <i18n value="e9bb05a1-81db-4360-a131-bd14453af87e"/>
-- MAGIC 
-- MAGIC 
-- MAGIC At the time we previously queried this data source, Spark automatically cached the underlying data in local storage. This ensures that on subsequent queries, Spark will provide the optimal performance by just querying this local cache.
-- MAGIC 
-- MAGIC Our external data source is not configured to tell Spark that it should refresh this data. 
-- MAGIC 
-- MAGIC We **can** manually refresh the cache of our data by running the **`REFRESH TABLE`** command.

-- COMMAND ----------

REFRESH TABLE uszips_csv

-- COMMAND ----------

-- MAGIC %md <i18n value="b7005568-3b03-4c4a-9ed0-122f14de6f7b"/>
-- MAGIC 
-- MAGIC 
-- MAGIC Note that refreshing our table will invalidate our cache, meaning that we'll need to rescan our original data source and pull all data back into memory. 
-- MAGIC 
-- MAGIC For very large datasets, this may take a significant amount of time.