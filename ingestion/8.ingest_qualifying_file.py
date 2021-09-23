# Databricks notebook source
# MAGIC %md
# MAGIC ### Read Multi Line Json

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import IntegerType,StructField,StructType,StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField('qualifyId',IntegerType(),False),
                                      StructField('raceId',IntegerType(),False),
                                      StructField('driverId',IntegerType(),False),
                                      StructField('constructorId',IntegerType(),False),
                                      StructField('number',IntegerType(),False),
                                      StructField('position',IntegerType(),True),
                                      StructField('q1',StringType(),True),
                                       StructField('q2',StringType(),True),
                                       StructField('q3',StringType(),True)
  
])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine",True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename and add

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed('driverId','driver_id') \
.withColumnRenamed('raceId','race_id') \
.withColumnRenamed('qualifyId','qualify_id') \
.withColumnRenamed('constructorId','constructor_id') \
.withColumn('ingestion_date',current_timestamp()) \
.withColumn('data_source',lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Write to Parquet

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = 'tgt.race_id = src.race_id and tgt.qualify_id = src.qualify_id'
merge_delta_data(final_df,'f1_processed','qualifying',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')