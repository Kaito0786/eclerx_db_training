-- Databricks notebook source
-- MAGIC %fs ls

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df =  spark.read.csv("dbfs:/databricks-datasets/bikeSharing/data-001/day.csv",header=True,inferSchema=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.saveAsTable("bike_day")

-- COMMAND ----------

select * from bike_day

-- COMMAND ----------

create or replace view max_month as 
select mnth,round(max(temp),2) as max from bike_day group by mnth order by max desc

-- COMMAND ----------

show views

-- COMMAND ----------

select * from max_month

-- COMMAND ----------

describe extended max_month

-- COMMAND ----------

describe extended bike_day

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW holiday_count_temp AS
select mnth,count(*) as count from bike_day where  holiday = 1 group by mnth

-- COMMAND ----------

select * from holiday_count_temp

-- COMMAND ----------

show views

-- COMMAND ----------

select * from bike_day

-- COMMAND ----------


create or replace function fullname(first_name string, last_name string)
returns string
return concat(first_name, " ", last_name)

-- COMMAND ----------


SELECT fullname("ABC","DEF")

-- COMMAND ----------

describe FUNCTION EXTENDED fullname;

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS person_data (
  id INT,
  forename STRING,
  lastname STRING,
  age INT
);

-- COMMAND ----------

INSERT INTO person_data VALUES 
  (1, 'Alice', 'Johnson', 28),
  (2, 'Bob', 'Smith', 35),
  (3, 'Charlie', 'Lee', 42);

-- COMMAND ----------

SELECT id, fullname(forename,lastname),age_category(age) from person_data

-- COMMAND ----------

CREATE OR REPLACE FUNCTION age_category(age INT)
RETURNS STRING
RETURN CASE
  WHEN age > 60 THEN 'Senior'
  WHEN age BETWEEN 20 AND 60 THEN 'Adult'
  WHEN age < 20 THEN 'Teenager'
  ELSE 'Unknown'
END;


-- COMMAND ----------

INSERT INTO person_data VALUES
  (4,  'David',     'Miller',   17),
  (5,  'Eva',       'Brown',    63),
  (6,  'Frank',     'Davis',    45),
  (7,  'Grace',     'Wilson',   12),
  (8,  'Henry',     'Moore',    75),
  (9,  'Isla',      'Taylor',   58),
  (10, 'Jack',      'Anderson', 22),
  (11, 'Karen',     'Thomas',   19),
  (12, 'Liam',      'White',    66),
  (13, 'Mia',       'Martin',   8);

