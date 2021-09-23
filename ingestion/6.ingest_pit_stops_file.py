# Databricks notebook source
# MAGIC %md
# MAGIC ### Read Multi Line Json

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import IntegerType,StructField,StructType,StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField('raceId',IntegerType(),False),
                                      StructField('driverId',IntegerType(),False),
                                      StructField('stop',StringType(),False),
                                      StructField('lap',IntegerType(),False),
                                      StructField('time',StringType(),False),
                                      StructField('duration',StringType(),False),
                                      StructField('milliseconds',IntegerType(),False),
  
])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename and add

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Write to Parquet

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = 'tgt.race_id = src.race_id and tgt.driver_id = src.driver_id and tgt.stop = src.stop'
merge_delta_data(final_df,'f1_processed','pit_stops',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')