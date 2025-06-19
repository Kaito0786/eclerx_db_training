# Databricks notebook source
spark

# COMMAND ----------


user_data=([(1,'Naval'),(2,'Karan')])
user_schema="id int, name string"

df=spark.createDataFrame(data=user_data, schema=user_schema)

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df.select("id","name").display()

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

df1=df.select(col("id").alias("emp_id"))

# COMMAND ----------

df1.display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id")

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name"}).display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").withColumnRenamed("name","emp_name").display()

# COMMAND ----------

df.withColumn("ingestion_date",current_date())

# COMMAND ----------

df.display()

# COMMAND ----------

df2=df.withColumn("ingestion_date",current_date())

# COMMAND ----------

df2.display()

# COMMAND ----------


