# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]
empDF = spark.createDataFrame(data=emp, schema = empColumns)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)

# COMMAND ----------

empDF.display()
deptDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Joins

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Perform an inner join on the two dataframes on department id

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Perform a left join on the two dataframes on department id

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Unions

# COMMAND ----------

# Given
simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)

simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns2= ["employee_name","department","state","salary","age","bonus"]

df2 = spark.createDataFrame(data = simpleData2, schema = columns2)

# COMMAND ----------

df.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Merge above two dataframes without duplicates

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Timestamps and dates

# COMMAND ----------

# Given
timestampDf=spark.createDataFrame(
        data = [ ("1","31-01-2020 16:33:19")],
        schema=["id","input_timestamp"])
timestampDf.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Add new column 'timestamp' that converts 'input_timestamp' to a proper timestamp type
# MAGIC Save the output in a variable called timestampDf2

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Calculate the number of months between current date and the given date

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Extract the hour, minute and second of 'timestamp' column

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Bonus: row number generation

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Window functions are used to calculate results such as the rank, row number e.t.c over a range of input rows. \
# MAGIC There is also the row_number() function that you must use. \
# MAGIC Feel free to use the internet :)

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Add new column with the row number

# COMMAND ----------

simpleData = (("Harry", "Sales", 2000), \
    ("Mike", "Sales", 2200),  \
    ("Lisa", "Sales", 3000),   \
    ("Claire", "Finance", 3000),  \
    ("James", "Sales", 2800),    \
    ("John", "Finance", 3500),  \
    ("Ken", "Finance", 3900),    \
    ("Baljeet", "Marketing", 4000), \
    ("Michael", "Marketing", 4500),\
    ("Ali", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Now generate row numbers for each department, so your output should look like this
# MAGIC <pre>
# MAGIC +-------------+----------+------+----------+
# MAGIC |employee_name|department|salary|row_number|
# MAGIC +-------------+----------+------+----------+
# MAGIC |Claire       |Finance   |3000  |1         |
# MAGIC |John         |Finance   |3500  |2         |
# MAGIC |Ken          |Finance   |3900  |3         |
# MAGIC |Baljeet      |Marketing |4000  |1         |
# MAGIC |Michael      |Marketing |4500  |2         |
# MAGIC |Harry        |Sales     |2000  |1         |
# MAGIC |Mike         |Sales     |2200  |2         |
# MAGIC |James        |Sales     |2800  |3         |
# MAGIC |Lisa         |Sales     |3000  |4         |
# MAGIC |Ali          |Sales     |4100  |5         |
# MAGIC +-------------+----------+------+----------+
# MAGIC </pre>
# MAGIC Hint: use partitionBy()

# COMMAND ----------


