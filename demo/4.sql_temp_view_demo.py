# Databricks notebook source
# MAGIC %md
# MAGIC ### Create temporary view on a DF

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

race_results_df.createOrReplaceTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results

# COMMAND ----------

races_results_2019 = spark.sql('select * from v_race_results where race_year = 2019')

# COMMAND ----------

display(races_results_2019)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Global Temporary View

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView('gv_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------

sql_objec