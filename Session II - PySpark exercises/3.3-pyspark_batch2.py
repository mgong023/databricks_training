# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# source: https://www.kaggle.com/datasets/shwetabh123/mall-customers?resource=download
df = spark.table('db_training_mall_customers')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Filter (where)

# COMMAND ----------

df.filter(df.Genre == 'Male')
df.filter("Genre == 'Male'")
df.filter(col('genre') == 'Female')
df.filter( (df.Genre  == "Female") & (col("Annual Income (k$)")  == 16) ).display()

# COMMAND ----------

li=[16, 17, 18]
df.filter(col("Annual Income (k$)").isin(li)).display()

# COMMAND ----------

df.filter(df.Genre.startswith("M")).display()

# COMMAND ----------

df.filter(df.Genre.contains("ale")).display()

# COMMAND ----------

df.filter(df.Genre.like("%ale%")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Sort

# COMMAND ----------

df.sort('age').display()

# COMMAND ----------

df.sort('Annual Income (k$)').display()

# COMMAND ----------

df.orderBy('Annual Income (k$)').display()

# COMMAND ----------

df.sort(df.Age.desc()).display()

# COMMAND ----------

df.sort(col('age').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Group by

# COMMAND ----------

df.groupBy('genre').display()

# COMMAND ----------

df.groupBy('genre').count()
df.groupBy('genre').min()
df.groupBy('genre').max()
df.groupBy('genre').sum()
df.groupBy('genre').avg()
df.groupBy('genre').agg(sum('Annual Income (k$)').alias('Sum annual income')).display()

# COMMAND ----------


