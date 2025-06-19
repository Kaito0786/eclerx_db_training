-- Databricks notebook source
CREATE TABLE employees_data (
    employee_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department VARCHAR(50),
    salary DECIMAL(10, 2)
);

-- COMMAND ----------

INSERT INTO employees_data (employee_id, first_name, last_name, department, salary) VALUES
(1, 'Alice', 'Johnson', 'Engineering', 75000.00),
(2, 'Bob', 'Smith', 'Marketing', 65000.00),
(3, 'Charlie', 'Brown', 'Sales', 70000.00),
(4, 'Diana', 'Prince', 'HR', 68000.00),
(5, 'Ethan', 'Clark', 'Finance', 72000.00);

-- COMMAND ----------

CREATE TABLE sales_external_v1 (
  product_name STRING,
  region STRING,
  amount DOUBLE
)

USING DELTA
LOCATION '/FileStore/eclerxinputfiles/sales_external_v1';

-- COMMAND ----------

INSERT INTO sales_external_v1 VALUES
  ('Laptop', 'North', 1200.50),
  ('Phone', 'South', 850.75),
  ('Tablet', 'East', 640.00);


-- COMMAND ----------

describe detail sales_data_external

-- COMMAND ----------

desc extended sales_external_v1

-- COMMAND ----------

drop table sales_data_external
