-- Databricks notebook source
CREATE OR REPLACE TABLE students
  (id INT, name STRING, value DOUBLE)

-- COMMAND ----------

INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

-- COMMAND ----------

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3)

-- COMMAND ----------

select * from students

-- COMMAND ----------

describe extended students

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/students/'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/students/_delta_log/'))

-- COMMAND ----------

DROP TABLE students

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/students/'))
-- MAGIC # rerun first 3 cells

-- COMMAND ----------


