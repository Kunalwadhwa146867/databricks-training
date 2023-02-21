-- Databricks notebook source
-- MAGIC %md <i18n value="bc50b1e9-781a-405d-bed4-c80dbd97e0d1"/>
-- MAGIC 
-- MAGIC 
-- MAGIC # higher order functions for working with arrays

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Interacting with JSON Data

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TEMP VIEW json_temp_view
-- MAGIC AS SELECT * FROM json.`/FileStore/tables/sample/sensor1-7.json`;
-- MAGIC 
-- MAGIC SELECT * FROM json_temp_view

-- COMMAND ----------

-- MAGIC %md <i18n value="df218c13-c1e9-4644-8859-d1d66106f224"/>
-- MAGIC 
-- MAGIC  
-- MAGIC ## Collect Arrays
-- MAGIC 
-- MAGIC The **`collect_set`** function can collect unique values for a field, including fields within arrays.
-- MAGIC 
-- MAGIC The **`flatten`** function allows multiple arrays to be combined into a single array.
-- MAGIC 
-- MAGIC The **`array_distinct`** function removes duplicate elements from an array.
-- MAGIC 
-- MAGIC Here, we combine these queries to create a simple table that shows the unique collection of actions and the items in a user's cart.

-- COMMAND ----------

SELECT 
  collect_set(sensorName) AS sensorName_y,
  array_distinct(flatten(collect_set(sensorReadings.sensorReading))) AS sensorReading_H
FROM json_temp_view
GROUP BY sensorName

-- COMMAND ----------

-- MAGIC %md <i18n value="d1ed83fa-4d2f-4138-b343-4d070c0d0e40"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Higher Order Functions
-- MAGIC Higher order functions in Spark SQL allow you to work directly with complex data types. When working with hierarchical data, records are frequently stored as array or map type objects. Higher-order functions allow you to transform data while preserving the original structure.
-- MAGIC 
-- MAGIC Higher order functions include:
-- MAGIC - **`FILTER`** filters an array using the given lambda function.
-- MAGIC - **`EXISTS`** tests whether a statement is true for one or more elements in an array. 
-- MAGIC - **`TRANSFORM`** uses the given lambda function to transform all elements in an array.
-- MAGIC - **`REDUCE`** takes two lambda functions to reduce the elements of an array to a single value by merging the elements into a buffer, and the apply a finishing function on the final buffer.

-- COMMAND ----------

-- MAGIC %md <i18n value="5fcad266-d5a0-4255-9c6b-be3634ea9e79"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Filter
-- MAGIC Remove items that are not king-sized from all records in our **`items`** column. We can use the **`FILTER`** function to create a new column that excludes that value from each array.
-- MAGIC 
-- MAGIC **`FILTER (items, i -> i.item_id LIKE "%K") AS king_items`**
-- MAGIC 
-- MAGIC In the statement above:
-- MAGIC - **`FILTER`** : the name of the higher-order function <br>
-- MAGIC - **`items`** : the name of our input array <br>
-- MAGIC - **`i`** : the name of the iterator variable. You choose this name and then use it in the lambda function. It iterates over the array, cycling each value into the function one at a time.<br>
-- MAGIC - **`->`** :  Indicates the start of a function <br>
-- MAGIC - **`i.item_id LIKE "%K"`** : This is the function. Each value is checked to see if it ends with the capital letter K. If it is, it gets filtered into the new column, **`king_items`**

-- COMMAND ----------

-- filter for sales of only king sized items
SELECT
  order_id,
  items,
  FILTER (items, i -> i.item_id LIKE "%K") AS king_items
FROM sales

-- COMMAND ----------

-- MAGIC %md <i18n value="7ef7b728-dfad-4cdf-8f41-8c72a76d4310"/>
-- MAGIC 
-- MAGIC 
-- MAGIC You may write a filter that produces a lot of empty arrays in the created column. When that happens, it can be useful to use a **`WHERE`** clause to show only non-empty array values in the returned column. 
-- MAGIC 
-- MAGIC In this example, we accomplish that by using a subquery (a query within a query). They are useful for performing an operation in multiple steps. In this case, we're using it to create the named column that we will use with a **`WHERE`** clause.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW king_size_sales AS

SELECT order_id, king_items
FROM (
  SELECT
    order_id,
    FILTER (items, i -> i.item_id LIKE "%K") AS king_items
  FROM sales)
WHERE size(king_items) > 0;
  
SELECT * FROM king_size_sales

-- COMMAND ----------

-- MAGIC %md <i18n value="e30bf997-6eb6-4ee7-94b8-6827ecdcce7d"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Transform
-- MAGIC Built-in functions are designed to operate on a single, simple data type within a cell; they cannot process array values. **`TRANSFORM`** can be particularly useful when you want to apply an existing function to each element in an array. 
-- MAGIC 
-- MAGIC Compute the total revenue from king-sized items per order.
-- MAGIC 
-- MAGIC **`TRANSFORM(king_items, k -> CAST(k.item_revenue_in_usd * 100 AS INT)) AS item_revenues`**
-- MAGIC 
-- MAGIC In the statement above, for each value in the input array, we extract the item's revenue value, multiply it by 100, and cast the result to integer. Note that we're using the same kind as references as in the previous command, but we name the iterator with a new variable, **`k`**.

-- COMMAND ----------

-- get total revenue from king items per order
CREATE OR REPLACE TEMP VIEW king_item_revenues AS

SELECT
  order_id,
  king_items,
  TRANSFORM (
    king_items,
    k -> CAST(k.item_revenue_in_usd * 100 AS INT)
  ) AS item_revenues
FROM king_size_sales;

SELECT * FROM king_item_revenues


-- COMMAND ----------

-- MAGIC %md <i18n value="6c15bff7-7667-4118-9b72-27068d6fa6be"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Summary
-- MAGIC Spark SQL offers a comprehensive set of native functionality for interacting with and manipulating highly nested data.
-- MAGIC 
-- MAGIC While some syntax for this functionality may be unfamiliar to SQL users, leveraging built-in functions like higher order functions can prevent SQL engineers from needing to rely on custom logic when dealing with highly complex data structures.