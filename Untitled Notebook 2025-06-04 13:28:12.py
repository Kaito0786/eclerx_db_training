# Databricks notebook source
dbutils.help()

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/raw/

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/eclerxinputfiles")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/raw/sales_data.csv","dbfs:/FileStore/eclerxinputfiles")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/raw/sales_data.csv")

# COMMAND ----------


