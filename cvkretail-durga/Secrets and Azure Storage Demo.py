# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()



# COMMAND ----------

dbutils.secrets.get(scope='sacvkretail',key='sacvkretailkey')

# COMMAND ----------

dbutils.secrets.list(scope='sacvkretail')

# COMMAND ----------

cvkretail_key=dbutils.secrets.get('sacvkretail','sacvkretailkey')

# COMMAND ----------

spark.conf.set('fs.azure.account.key',cvkretail_key)

# COMMAND ----------

# MAGIC %fs ls abfss://data@cvkretail.dfs.core.windows.net/retail_db

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TEXT.`abfss://data@cvkretail.dfs.core.windows.net/retail_db/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE Temporary view orders(
# MAGIC
# MAGIC   order_id INT ,
# MAGIC   order_date date ,
# MAGIC   order_customer_id INT ,
# MAGIC   order_status string 
# MAGIC ) using csv
# MAGIC options (
# MAGIC   path='abfss://data@cvkretail.dfs.core.windows.net/retail_db/orders'
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE Temporary view order_items(
# MAGIC order_item_id INT ,
# MAGIC   order_item_order_id INT ,
# MAGIC   order_item_product_id INT ,
# MAGIC   order_item_quantity INT ,
# MAGIC   order_item_subtotal FLOAT ,
# MAGIC   order_item_product_price FLOAT 
# MAGIC ) using csv
# MAGIC options (
# MAGIC   path='abfss://data@cvkretail.dfs.core.windows.net/retail_db/order_items/'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from order_items

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from orders o join order_items oi on o.order_id=oi.order_item_order_id
# MAGIC where o.order_status IN ("COMPLETE",'CLOSED')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC o.order_date
# MAGIC ,oi.order_item_product_id
# MAGIC ,round(sum(oi.order_item_subtotal),2) as revenue
# MAGIC from orders o join order_items oi on o.order_id=oi.order_item_order_id
# MAGIC where o.order_status IN ("COMPLETE",'CLOSED')
# MAGIC group by 
# MAGIC o.order_date
# MAGIC ,oi.order_item_product_id
# MAGIC order by 1,3 DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT OVERWRITE DIRECTORY 'abfss://data@cvkretail.dfs.core.windows.net/retail_db/daily_product_revenue'
# MAGIC USING PARQUET
# MAGIC select 
# MAGIC o.order_date
# MAGIC ,oi.order_item_product_id
# MAGIC ,round(sum(oi.order_item_subtotal),2) as revenue
# MAGIC from orders o join order_items oi on o.order_id=oi.order_item_order_id
# MAGIC where o.order_status IN ("COMPLETE",'CLOSED')
# MAGIC group by 
# MAGIC o.order_date
# MAGIC ,oi.order_item_product_id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from PARQUET.`abfss://data@cvkretail.dfs.core.windows.net/retail_db/daily_product_revenue`
# MAGIC order by 1,3 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW daily_product_revenue
# MAGIC USING PARQUET
# MAGIC OPTIONS (
# MAGIC   path='abfss://data@cvkretail.dfs.core.windows.net/retail_db/daily_product_revenue'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from daily_product_revenue
# MAGIC order by 1,3 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC dpr.* 
# MAGIC ,rank() OVER (ORDER BY revenue desc) as rnk
# MAGIC ,dense_rank() OVER (ORDER BY revenue desc) as drnk
# MAGIC from daily_product_revenue dpr 
# MAGIC where dpr.order_date='2013-07-26'
# MAGIC order by 1,3 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ( select 
# MAGIC dpr.* 
# MAGIC ,rank() OVER (PARTITION BY order_date ORDER BY revenue desc) as rnk
# MAGIC from daily_product_revenue dpr 
# MAGIC --where dpr.order_date='2013-07-26'
# MAGIC )
# MAGIC where rnk<=5
# MAGIC order by 1 desc,4 asc

# COMMAND ----------


