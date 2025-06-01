# Databricks notebook source
data = [
    ("Alice", "HR", 5000),
    ("Bob", "HR", 6000),
    ("Charlie", "IT", 8000),
    ("David", "IT", 8500),
    ("Eve", "IT", 8000)
]


# COMMAND ----------

schema='name','dept','salary'

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark=SparkSession.builder.appName('WindowsFunction').getOrCreate()
df=spark.createDataFrame(data,schema=schema)
display(df)

# COMMAND ----------

windowspec=Window.partitionBy('dept').orderBy(df['salary'].desc())
df_rank=df.withColumn('Rank',rank().over(windowspec)).show()

# COMMAND ----------

windowrank=Window.partitionBy('dept').orderBy(df['salary'].desc())
df_rank1=df.withColumn('rank',rank().over(windowrank)).show()

# COMMAND ----------

df_presal=df.withColumn("prev_salary",lag('salary',1).over(windowspec)).show()

# COMMAND ----------

df_rt=df.withColumn('Running_Total',sum('salary').over(windowspec)).show()

# COMMAND ----------

df_two=df.withColumn('rank',rank().over(windowspec))
df_two=df_two.filter(df_two.rank<=1).show()

# COMMAND ----------

display(df)

# COMMAND ----------

windowrule=Window.partitionBy('dept')
df_A=df.withColumn('dept_avg',avg('salary').over(windowrule))





# COMMAND ----------

display(df_A)

# COMMAND ----------

df_result = df_A.withColumn("higher_salary", df_A["salary"] > df_A["dept_avg"])
display(df_result)

# COMMAND ----------

from pyspark.sql.functions import col, lag, datediff
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    (101, "2024-06-01"),
    (101, "2024-06-02"),
    (101, "2024-06-05"),
    (101, "2024-06-08"),
    (102, "2024-06-01"),
    (102, "2024-06-02"),
    (102, "2024-06-03"),
]

# COMMAND ----------

schema=['id','login_date']

# COMMAND ----------

df_emp=spark.createDataFrame(data,schema=schema)

# COMMAND ----------

display(df_emp)

# COMMAND ----------

window_emp=Window.partitionBy('id').orderBy('login_date')

# COMMAND ----------

df_emp=df_emp.withColumn('previous_day',lag('login_date',1).over(window_emp))
display(df_emp)

# COMMAND ----------

df_emp=df_emp.withColumn('login_date',col('login_date').cast('date'))
df_emp=df_emp.withColumn('previous_day',col('previous_day').cast('date'))

# COMMAND ----------

df_emp=df_emp.withColumn('numberdiff',datediff(col('login_date'),col('previous_day')))
display(df_emp.filter(col('numberdiff')==1))
