# Databricks notebook source
# MAGIC %md <i18n value="014a6669-786f-416d-ac00-9c2e4e9eecfe"/>
# MAGIC   
# MAGIC # Create and Manage Interactive Clusters
# MAGIC 
# MAGIC A Databricks cluster is a set of computation resources and configurations on which you run data engineering, data science, and data analytics workloads, such as production ETL pipelines, streaming analytics, ad-hoc analytics, and machine learning. You run these workloads as a set of commands in a notebook or as an automated job. 
# MAGIC 
# MAGIC ###All-purpose clusters 
# MAGIC - use all-purpose clusters to analyze data collaboratively using interactive notebooks.
# MAGIC - can create an all-purpose cluster using the UI, CLI, or REST API. 
# MAGIC - can manually terminate and restart an all-purpose cluster. 
# MAGIC - Multiple users can share such clusters to do collaborative interactive analysis
# MAGIC 
# MAGIC ###Job clusters.
# MAGIC - use job clusters to run fast and robust automated jobs.
# MAGIC - job cluster is created when we run a job and terminates the cluster when the job is complete
# MAGIC - We cannot restart a job cluster manually

# COMMAND ----------

# MAGIC %md <i18n value="cb2cc2e0-3923-4969-bc01-11698cd1761c"/>
# MAGIC   
# MAGIC ## Create Cluster
# MAGIC 
# MAGIC Depending on the workspace in which you're currently working, you may or may not have cluster creation privileges. 
# MAGIC 
# MAGIC Instructions in this section assume that you **do** have cluster creation privileges, and that you need to deploy a new cluster to execute the lessons in this course.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Use the left sidebar to navigate to the **Compute** page by clicking on the ![compute](https://files.training.databricks.com/images/clusters-icon.png) icon
# MAGIC 1. Click the blue **Create Cluster** button
# MAGIC 1. For the **Cluster name**, use your name so that you can find it easily and the instructor can easily identify it if you have problems
# MAGIC 1. Set the **Cluster mode** to **Single Node** (this mode is required to run this course)
# MAGIC 1. Use the recommended **Databricks runtime version** for this course
# MAGIC 1. Leave boxes checked for the default settings under the **Autopilot Options**
# MAGIC 1. Click the blue **Create Cluster** button
# MAGIC 
# MAGIC **NOTE:** Clusters can take several minutes to deploy. Once you have finished deploying a cluster, feel free to continue to explore the cluster creation UI.

# COMMAND ----------

# MAGIC %md <i18n value="d1a8cf77-f6e8-40df-8355-3a598441457a"/>
# MAGIC 
# MAGIC ## Manage Clusters
# MAGIC 
# MAGIC Once the cluster is created, go back to the **Compute** page to view the cluster.
# MAGIC 
# MAGIC Select a cluster to review the current configuration. 
# MAGIC 
# MAGIC Click the **Edit** button. Note that most settings can be modified (if you have sufficient permissions). Changing most settings will require running clusters to be restarted.

# COMMAND ----------

# MAGIC %md <i18n value="b7ce6ce1-4d68-4a91-b325-3831c1653c67"/>
# MAGIC 
# MAGIC ## Restart, Terminate, and Delete
# MAGIC 
# MAGIC Note that while **Restart**, **Terminate**, and **Delete** have different effects, they all start with a cluster termination event. (Clusters will also terminate automatically due to inactivity assuming this setting is used.)
# MAGIC 
# MAGIC When a cluster terminates, all cloud resources currently in use are deleted. This means:
# MAGIC * Associated VMs and operational memory will be purged
# MAGIC * Attached volume storage will be deleted
# MAGIC * Network connections between nodes will be removed
# MAGIC 
# MAGIC In short, all resources previously associated with the compute environment will be completely removed. This means that **any results that need to be persisted should be saved to a permanent location**. Note that you will not lose your code, nor will you lose data files that you've saved out appropriately.
# MAGIC 
# MAGIC The **Restart** button will allow us to manually restart our cluster. This can be useful if we need to completely clear out the cache on the cluster or wish to completely reset our compute environment.
# MAGIC 
# MAGIC The **Terminate** button allows us to stop our cluster. We maintain our cluster configuration setting, and can use the **Restart** button to deploy a new set of cloud resources using the same configuration.
# MAGIC 
# MAGIC The **Delete** button will stop our cluster and remove the cluster configuration.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Cluster node type
# MAGIC 
# MAGIC A cluster consists of one **driver** node and zero or more **worker** nodes
# MAGIC 
# MAGIC ### Driver Node 
# MAGIC The driver node maintains state information of all notebooks attached to the cluster. \
# MAGIC The driver node also maintains the SparkContext, interprets all the commands you run from a notebook or a library on the cluster, \
# MAGIC Runs the Apache Spark master that coordinates with the Spark executors.
# MAGIC 
# MAGIC ### Worker Node
# MAGIC Worker nodes run the Spark executors and other services required for proper functioning clusters. \
# MAGIC When you distribute your workload with Spark, all the distributed processing happens on worker nodes. \
# MAGIC Databricks runs one executor per worker node. Therefore, the terms executor and worker are used interchangeably in the context of the Databricks architecture.\

# COMMAND ----------

# MAGIC %md
# MAGIC ## Single Node cluster properties
# MAGIC A Single Node cluster is a cluster consisting of an Apache Spark driver and no Spark workers. A Single Node cluster supports Spark jobs and all Spark data sources. A Single Node cluster has the following properties:
# MAGIC 
# MAGIC - Runs Spark locally.
# MAGIC - The driver acts as both master and worker, with no worker nodes.
# MAGIC - Spawns one executor thread per logical core in the cluster, minus 1 core for the driver.
# MAGIC - All stderr, stdout, and log4j log output is saved in the driver log.
# MAGIC - A Single Node cluster can’t be converted to a Standard cluster. To use a Standard cluster, create the cluster and attach your notebook to it.

# COMMAND ----------



# COMMAND ----------

