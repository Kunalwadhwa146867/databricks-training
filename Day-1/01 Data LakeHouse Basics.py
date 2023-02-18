# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Data Warehouse Vs Data Lake Vs Data LakeHouse

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #Data Warehouse -
# MAGIC 
# MAGIC  * Typically used for data analysis and reporting, data warehouses rely on ETL mechanisms to extract, transform, and load data into a destination.
# MAGIC 
# MAGIC  * The data in this case is checked against the pre-defined schema (internal database format) when being uploaded, which is known as the schema-on-write approach.
# MAGIC 
# MAGIC  * Purpose-built, data warehouses allow for making complex queries on structured data via SQL (Structured Query Language) and getting results fast for business intelligence
# MAGIC              
# MAGIC              
# MAGIC             
# MAGIC              

# COMMAND ----------

# MAGIC %md 
# MAGIC  <img src="https://content.altexsoft.com/media/2021/11/traditional-data-warehouse-platform-architecture.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ###Key data warehouse limitations: 
# MAGIC 
# MAGIC *Inefficiency and high costs of traditional data warehouses in terms of continuously growing data volumes.
# MAGIC 
# MAGIC *Inability to handle unstructured data such as audio, video, text documents, and social media posts.
# MAGIC 
# MAGIC *The DW makeup isn’t the best fit for complex data processing such as machine learning as warehouses normally store task-specific data, while machine learning and data science tasks thrive on the availability of all collected data.

# COMMAND ----------

# MAGIC %md
# MAGIC # DataLake
# MAGIC 
# MAGIC Store huge amounts of raw data in its native formats (structured, unstructured, and semi-structured) and in open file formats such as Apache Parquet for further big data processing, analysis, and machine learning purposes
# MAGIC 
# MAGIC Unlike data warehouses, data lakes don’t require data transformation prior to loading as there isn’t any schema for data to fit Instead, the schema is verified when a person queries data, which is known as the schema-on-read approach
# MAGIC 
# MAGIC            All of this makes data lakes more robust and cost-effective compared to traditional data warehouses

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC  <img src="https://content.altexsoft.com/media/2021/11/data-lake-architecture-example.png.webp">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Key data lake limitations:
# MAGIC 
# MAGIC Business intelligence and reporting are challenging as data lakes require additional tools and techniques to support SQL queries.
# MAGIC 
# MAGIC Poor data quality, reliability, and integrity are problems.
# MAGIC 
# MAGIC Issues with data security and governance exist.
# MAGIC 
# MAGIC Data in lakes is disorganized which often leads to the data stagnation problem.

# COMMAND ----------

# MAGIC %md
# MAGIC # The two-tier architecture with a data lake and data warehouses

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://content.altexsoft.com/media/2021/11/the-two-tier-architecture-with-a-data-lake-and-dat.png.webp">

# COMMAND ----------

# MAGIC %md
# MAGIC ###This approach had often resulted in increased complexity and costs as data should be kept consistent between the two systems

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Lakehouse
# MAGIC 
# MAGIC A lakehouse mitigates major limitations of data warehouses and data lakes by offering:
# MAGIC 
# MAGIC ###Improved data reliability:
# MAGIC              Fewer cycles of ETL data transfers between different systems are needed, reducing the chance of quality issues.
# MAGIC 
# MAGIC ###Decreased costs: 
# MAGIC              Data won’t be kept in several storage systems simultaneously and ongoing ETL costs will be reduced too.
# MAGIC 
# MAGIC ###More actionable data: 
# MAGIC              The structure of a lakehouse helps organize big data in a data lake, solving the stagnation problem.
# MAGIC 
# MAGIC ###Better data management: 
# MAGIC               Not only can lakehouses keep large volumes of diverse data, but they also allow multiple use cases for it, including advanced analytics, reporting, and machine learning.

# COMMAND ----------

# MAGIC %md
# MAGIC ##In general, a data lakehouse system will consist of five layers.
# MAGIC 
# MAGIC    Ingestion layer \
# MAGIC    Storage layer \
# MAGIC    Metadata layer \
# MAGIC    API layer \
# MAGIC    Consumption layer 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src = "https://content.altexsoft.com/media/2021/11/the-multi-layered-data-lakehouse-architecture.png.webp">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://www.databricks.com/wp-content/uploads/2020/01/data-lakehouse-new.png">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #  The Databricks Lakehouse Platform
# MAGIC 
# MAGIC The Databricks Lakehouse Platform combines the best elements of data lakes and data warehouses to deliver the reliability, strong governance and performance of data warehouses with the openness, flexibility and machine learning support of data lakes. \
# MAGIC It’s built on open source and open standards to maximize flexibility. \
# MAGIC And, its common approach to data management, security and governance helps you operate more efficiently and innovate faster.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src= "https://www.databricks.com/wp-content/uploads/2022/02/Marketecture.svg">

# COMMAND ----------

