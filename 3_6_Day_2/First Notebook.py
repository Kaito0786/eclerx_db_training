# Databricks notebook source
print("Welcome to Databricks !!")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT "SQL"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE CompanyDB;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CompanyDB;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Employees (
# MAGIC     ID INT,
# MAGIC     Name VARCHAR(100),
# MAGIC     Position VARCHAR(100),
# MAGIC     Salary DECIMAL(10, 2)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Employees (ID, Name, Position, Salary) VALUES
# MAGIC (1, 'Alice Johnson', 'Software Engineer', 85000.00),
# MAGIC (2, 'Bob Smith', 'Product Manager', 95000.00),
# MAGIC (3, 'Carol Lee', 'Data Analyst', 78000.00);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Employees;
