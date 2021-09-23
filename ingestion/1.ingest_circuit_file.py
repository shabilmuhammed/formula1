# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Circuit.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC  #### Read csv using spark dataframe reader

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType

# COMMAND ----------

# Struct type represents rows, Struct Field represents columns
circuits_schema = StructType(fields = [StructField("circuitid",IntegerType(),False),
                                       StructField("circuitRef",StringType(),True),
                                       StructField("name",StringType(),True),
                                       StructField("location",StringType(),True),
                                       StructField("country",StringType(),True),
                                       StructField("lat",DoubleType(),True),
                                       StructField("lng",DoubleType(),True),
                                       StructField("alt",IntegerType(),True),
                                       StructField("url",StringType(),True)
  
])

# COMMAND ----------

circuits_df = spark.read.option("header",True).schema(circuits_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only required columns

# COMMAND ----------

#circuits_selected_df = circuits_df.select("circuitid","circuitRef","name","location","country","lat","lng","alt") 

# COMMAND ----------

#circuits_selected_df = circuits_df.select(circuits_df.circuitid,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt) 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col('circuitid'),col('circuitRef'),col('name'),col('location'),col('country'),col('lat'),col('lng'),col('alt')) 

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitid","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude") \
.withColumn('data_source',lit(v_data_source)) \
.withColumn('file_date',lit(v_file_date))



# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to data lake as parquet 

# COMMAND ----------

# circuits_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.circuits')
circuits_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit('Success')