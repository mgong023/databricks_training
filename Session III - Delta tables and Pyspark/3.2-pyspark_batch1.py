# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# output as spark dataframe
spark.table('students').display()

# COMMAND ----------

cars = [("Kia","Cee'd", "2020", "130", "5"), 
        ("Audi","A3", "2003", "150", "5"), 
        ("Ferrari","Roma", "2021", "600", "2"), 
        ("Volkswagen", "Transporter", "2019", "180", "7"), 
      ]

cars_columns = ["car_brand", "model", "build_date", "power", "seats"]
df = spark.createDataFrame(data=cars, schema = cars_columns)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Selecting columns

# COMMAND ----------

# single columns
df.select('car_brand')
df.select(df.car_brand)
df.select(df['car_brand']).display()

# COMMAND ----------

# multiple columns
df.select("car_brand",col("model")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Adding columns

# COMMAND ----------

# Update value
df_news = df.withColumn("seats",col("seats")*100)
df_news.display()

# COMMAND ----------

# changing types
df.withColumn("power",col("power").cast("String")).printSchema()

# COMMAND ----------

# Create new column
df.withColumn("has_four_wheels", lit(True)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Renaming columns

# COMMAND ----------

df.withColumnRenamed("car_brand","manufacturer").display()

# COMMAND ----------

df.withColumnRenamed("car_brand","manufacturer").withColumnRenamed("model", "type").display()

# COMMAND ----------

df.select(col("car_brand").alias("manufacturer")).display()

# COMMAND ----------


