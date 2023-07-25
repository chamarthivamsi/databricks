# Databricks notebook source
# MAGIC %fs ls dbfs:/public/retail_db

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db/schemas.json

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TEXT.`dbfs:/public/retail_db/schemas.json`

# COMMAND ----------

help(spark.read.text)

# COMMAND ----------

df=spark.read.text('dbfs:/public/retail_db/schemas.json')
df.collect()

# COMMAND ----------

spark.read.text('dbfs:/public/retail_db/schemas.json',wholetext=True).first().value


# COMMAND ----------

schemas_text=spark.read.text('dbfs:/public/retail_db/schemas.json',wholetext=True).first().value

# COMMAND ----------

type(schemas_text)

# COMMAND ----------

import json

# COMMAND ----------

test=json.loads(schemas_text)

# COMMAND ----------

type(test)


# COMMAND ----------

test

# COMMAND ----------

column_details=json.loads(schemas_text)['orders']

# COMMAND ----------

type(column_details)

# COMMAND ----------

column_details

# COMMAND ----------

sorted(column_details,key=lambda col:col['column_position'])

# COMMAND ----------

columns=[col['column_name'] for col in sorted(column_details,key=lambda col:col['column_position'])]

# COMMAND ----------

columns

# COMMAND ----------

spark.read.csv('dbfs:/public/retail_db/orders',inferSchema=True).toDF(*columns).show()


# COMMAND ----------

orders=spark.read.csv('dbfs:/public/retail_db/orders',inferSchema=True).toDF(*columns)

# COMMAND ----------

orders

# COMMAND ----------

orders.show()

# COMMAND ----------

from pyspark.sql.functions import count,col

# COMMAND ----------

orders.\
groupBy('order_status').\
agg(count('*').alias('order_count')).\
orderBy(col('order_count').desc()).\
show()

# COMMAND ----------

import json
def get_columns(schemas_file,ds_name):
    schemas_text=spark.read.text(schemas_file,wholetext=True).first().value
    schemas=json.loads(schemas_text)
    column_details=schemas[ds_name]
    columns=[col['column_name'] for col in sorted(column_details,key=lambda col:col['column_position'])]
    return columns


# COMMAND ----------

get_columns('dbfs:/public/retail_db/schemas.json','orders')

# COMMAND ----------

ds_list =['departments','categories','products','customers','orders','order_items']

# COMMAND ----------

base_dir ='dbfs:/public/retail_db'
tgt_base_dir='dbfs:/public/retail_db_parquet'


# COMMAND ----------

orders=spark.read.csv('dbfs:/public/retail_db/orders',inferSchema=True).toDF(*columns)

# COMMAND ----------

orders.\
    write.\
    mode('overwrite').\
    parquet('dbfs:/public/retail_db_parquet/orders')

# COMMAND ----------

for ds in ds_list:
    print(f'processing {ds} data')
    columns = get_columns(f'{base_dir}/schemas.json',ds)
    df=spark.read.csv(f'{base_dir}/{ds}',inferSchema=True).toDF(*columns)
    df.write.mode('overwrite').parquet(f'{tgt_base_dir}/{ds}')

# COMMAND ----------

# MAGIC %fs ls dbfs:/public/retail_db_parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from PARQUET.`dbfs:/public/retail_db_parquet/orders/`

# COMMAND ----------


