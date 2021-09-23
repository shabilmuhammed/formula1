# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Drivers File

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 Read

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

from pyspark.sql.types import StringType,StructField,StructType,DateType,IntegerType

# COMMAND ----------

name_schema = StructType(fields = [StructField("forename",StringType(),True),
                                   StructField("surname",StringType(),True)
  
])

# COMMAND ----------

drivers_schema = StructType(fields = [StructField("driverId",IntegerType(),False),
                                      StructField("driverRef",StringType(),False),
                                      StructField("number",IntegerType(),False),
                                      StructField("code",StringType(),False),
                                      StructField("name",name_schema),
                                      StructField("dob",DateType(),False),
                                      StructField("nationality",StringType(),False),
                                      StructField("url",StringType(),False),
  
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f'{raw_folder_path}/{v_file_date}/drivers.json')

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import concat,current_timestamp,col,lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed('driverId','driver_id') \
                                    .withColumnRenamed('driverRef','driver_ref') \
                                    .withColumn('ingestion_date',current_timestamp()) \
                                    .withColumn('name',concat(drivers_df.name.forename,lit(' '),drivers_df.name.surname)) \
                                    .withColumn('data_source',lit(v_data_source)) \
                                    .withColumn('file_date',lit(v_file_date))



# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop column

# COMMAND ----------

 drivers_final_df = drivers_with_columns_df.drop(col('url'))

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.drivers')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races

# COMMAND ----------

dbutils.notebook.exit('Success')