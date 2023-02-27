# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# Suppose this is your dataset
simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Filter df where department is Sales in state NY

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Filter df where department is Sales and bonus is higher than 20000

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Sorting

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Sort df on bonus descending

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Sort df on bonus and age

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Grouping

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Group by department and find the sum of salary for each department

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Calculate the number of employees working in each department

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Calculate sum of salary and bonus for each department in each state

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Find the departments where the sum of bonus is less than 50000

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Use SQL query to do question 8
# MAGIC Hint: use spark.sql(), this returns a spark dataframe

# COMMAND ----------

# Create a temp view called 'df' that you can query against using spark.sql()
df.createOrReplaceTempView('df')

# COMMAND ----------


