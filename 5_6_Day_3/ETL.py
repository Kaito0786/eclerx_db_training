# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/eclerxinputfiles/

# COMMAND ----------

df = spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/eclerxinputfiles/circuits.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df1 = df.withColumnRenamed("circuitId","circuit_Id").withColumnRenamed("circuitRef","circuit_Ref")

# COMMAND ----------

df1.display()

# COMMAND ----------

df1=df1.withColumn("current_date", current_date())

# COMMAND ----------

from pyspark.sql.functions import current_date

# COMMAND ----------

df1= df1.drop("url")

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema formula1

# COMMAND ----------

# MAGIC %sql
# MAGIC use database formula1

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("formula1.circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.circuit

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended formula1.circuit

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE eclerx_people (
# MAGIC     id INT ,
# MAGIC     name VARCHAR(100),
# MAGIC     state VARCHAR(50)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO eclerx_people (id, name, state) VALUES
# MAGIC (1, 'Alice Johnson', 'California'),
# MAGIC (2, 'Bob Smith', 'Texas'),
# MAGIC (3, 'Charlie Brown', 'New York'),
# MAGIC (4, 'Diana Prince', 'Florida'),
# MAGIC (5, 'Ethan Hunt', 'Nevada');

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended formula1.eclerx_people

# COMMAND ----------


