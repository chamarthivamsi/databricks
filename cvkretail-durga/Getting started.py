# Databricks notebook source
print('hello world')

# COMMAND ----------

# MAGIC %fs ls /public/retail_db

# COMMAND ----------

dbutils.fs.ls('dbfs:/public/retail_db')

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs ls abfss://data@cvkretail.dfs.core.windows.net/retail_db

# COMMAND ----------

spark.conf.set('fs.azure.account.key','xz4/hfsljL1fgkzwd8N0mbr56RFj+jzei/DgZKYh1KlpojqkbH3fEYIiLVE3+V9oWZlBBKIJDjNh+AStjRX+IA==')

# COMMAND ----------

# MAGIC %fs ls abfss://data@cvkretail.dfs.core.windows.net/retail_db

# COMMAND ----------

# MAGIC %sql
# MAGIC set fs.azure.account.key=xz4/hfsljL1fgkzwd8N0mbr56RFj+jzei/DgZKYh1KlpojqkbH3fEYIiLVE3+V9oWZlBBKIJDjNh+AStjRX+IA==

# COMMAND ----------

# MAGIC %fs ls abfss://data@cvkretail.dfs.core.windows.net/retail_db
# MAGIC

# COMMAND ----------

# MAGIC %fs help
# MAGIC

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs rm dbfs:/public/ -r

# COMMAND ----------

spark.sql('select current_date').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TEMPORARY VIEW ORDERS(
# MAGIC   order_id int,
# MAGIC   order_date date ,
# MAGIC   order_customer_id INT ,
# MAGIC   order_status STRING
# MAGIC )
# MAGIC using csv 
# MAGIC options(
# MAGIC
# MAGIC   path='abfss://data@cvkretail.dfs.core.windows.net/retail_db/orders'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders

# COMMAND ----------

# MAGIC %fs ls /public/retail_db
# MAGIC

# COMMAND ----------


