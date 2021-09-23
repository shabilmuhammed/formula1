# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read csv using spark reader

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

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType,DateType

# COMMAND ----------

races_schema = StructType(fields = [StructField('raceId',IntegerType(),False),
                         StructField('year',IntegerType(),True),
                         StructField('round',IntegerType(),True),
                         StructField('circuitId',IntegerType(),True),
                         StructField('name',StringType(),True),
                         StructField('date',DateType(),True),
                         StructField('time',StringType(),True),
                         StructField('url',StringType(),True)])

# COMMAND ----------

races_df = spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### select only required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_df.select(col('raceId'),col('year'),col('round'),col('circuitId'),col('name'),col('date'),col('time'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed('raceId','race_id') \
.withColumnRenamed('year','race_year') \
.withColumnRenamed('circuitId','circuit_id') 

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion and race_timestamp column

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,current_timestamp,concat,lit

# COMMAND ----------

races_pre_final_df = races_renamed_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')) \
.withColumn('ingestion_date',current_timestamp()).withColumn('data_source',lit(v_data_source)).withColumn('file_date',lit(v_file_date))

# COMMAND ----------

display(races_pre_final_df)

# COMMAND ----------

races_final_df = races_pre_final_df.select(col('race_id'),col('race_year'),col('round'),col('circuit_id'),col('name'),col('ingestion_date'),col('race_timestamp'))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to parquet

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').format('delta').saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit('Success')