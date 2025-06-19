-- Databricks notebook source
CREATE TABLE employees (
    employee_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department VARCHAR(50),
    salary DECIMAL(10, 2)
);

-- COMMAND ----------

DESCRIBE EXTENDED employees;

-- COMMAND ----------

desc detail employees;

-- COMMAND ----------

INSERT INTO employees (employee_id, first_name, last_name, department, salary) VALUES
(1, 'Alice', 'Johnson', 'Engineering', 75000.00),
(2, 'Bob', 'Smith', 'Marketing', 65000.00),
(3, 'Charlie', 'Brown', 'Sales', 70000.00),
(4, 'Diana', 'Prince', 'HR', 68000.00),
(5, 'Ethan', 'Clark', 'Finance', 72000.00);

-- COMMAND ----------

desc history employees;

-- COMMAND ----------

DELETE FROM employees
WHERE employee_id > 3;

-- COMMAND ----------

desc detail employees;

-- COMMAND ----------

desc history employees;

-- COMMAND ----------

select * from employees

-- COMMAND ----------

desc detail employees;

-- COMMAND ----------

optimize employees
zorder by (employee_id)

-- COMMAND ----------

DELETE FROM employees
;

-- COMMAND ----------

DESC history employees;


-- COMMAND ----------

restore table employees to version as of 1

-- COMMAND ----------

select * from employees;

-- COMMAND ----------

DESC history employees;


-- COMMAND ----------

-- Insert additional data into the employees table
INSERT INTO employees (employee_id, first_name, last_name, department, salary) VALUES
(6, 'Fiona', 'Lee', 'Engineering', 80000.00);


-- COMMAND ----------

INSERT INTO employees (employee_id, first_name, last_name, department, salary) VALUES
(7, 'George', 'Martinez', 'Sales', 67000.00);


-- COMMAND ----------

INSERT INTO employees (employee_id, first_name, last_name, department, salary) VALUES
(8, 'Hannah', 'Nguyen', 'Finance', 73000.00);


-- COMMAND ----------

vacuum employees retain 0 hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false
