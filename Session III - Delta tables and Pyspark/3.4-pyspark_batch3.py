# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

valuesA = [('Pirate',1),('Monkey',2),('Ninja',3),('Spaghetti',4)]
df1 = spark.createDataFrame(valuesA,['name','id'])
 
valuesB = [(1, 'Rutabaga'),(2, 'Pirate'),(3, 'Ninja'),(4, 'Darth Vader')]
df2 = spark.createDataFrame(valuesB,['id', 'name'])
 
df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Joins

# COMMAND ----------

df1.join(df2, df1.name==df2.name).display()

# COMMAND ----------

df1.join(df2, df1.name==df2.name, how='left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Unions

# COMMAND ----------

df1.union(df2).display() # duplicates are not removed

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Timestamps
# MAGIC Standard format yyyy-mm-dd hh:mm:ss

# COMMAND ----------

df=spark.createDataFrame(
        data = [ ("1","25-01-2000 12:01:19")],
        schema=["id","input_timestamp"])
df.display()

# COMMAND ----------

df.withColumn("timestamp",to_timestamp("input_timestamp", 'dd-MM-yyyy HH:mm:ss')).display()

# COMMAND ----------

data=[["1","2023-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
df=spark.createDataFrame(data,["id","input"])
df.display()

# COMMAND ----------

df.select(current_date()).display()

# COMMAND ----------

df.select(col("input"), date_format(col("input"), "MM-dd-yyyy").alias("date_format")).display() # still strings

# COMMAND ----------

df.select(col("input"), to_date(col("input"), "yyy-MM-dd").alias("to_date")).display()

# COMMAND ----------

df.select(col("input"), year(col('input'))).display()
#month, day

# COMMAND ----------

df.select(col("input"), datediff(current_date(),col("input")).alias("datediff")).display()

# COMMAND ----------


