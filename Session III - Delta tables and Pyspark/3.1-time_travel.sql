-- Databricks notebook source
SELECT * FROM students

-- COMMAND ----------

DESCRIBE HISTORY students

-- COMMAND ----------

select * from students version as of 1

-- COMMAND ----------

select * from students timestamp as of '2023-02-24T13:37:40.000+0000'

-- COMMAND ----------

TRUNCATE TABLE students

-- COMMAND ----------

select * from students

-- COMMAND ----------

RESTORE TABLE students to version as of 4

-- COMMAND ----------

select * from students

-- COMMAND ----------


