# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

cars = [("Kia","Cee'd", "2020", "130", "5"), 
        ("Audi","A3", "2003", "150", "5"), 
        ("Ferrari","Roma", "2021", "600", "2"), 
        ("Volkswagen", "Transporter", "2019", "180", "7"), 
      ]

cars_columns = ["car_brand", "model", "build_date", "power", "seats"]
df = spark.createDataFrame(data=cars, schema = cars_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Select car_brand and model columns

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Add a new column 'addition' that adds 50 to the values in 'power' column

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Add a new column 'is_available' that with value 'yes'

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Rename the column 'is_available' to 'in_stock'

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Display the first column as 'brands' AND also display the second column

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Lastly, use a SQL expression to get the same output asked in question 4
# MAGIC Hint: Use selectExpr() function \
# MAGIC docs: https://sparkbyexamples.com/spark/spark-select-vs-selectexpr-with-examples/

# COMMAND ----------


