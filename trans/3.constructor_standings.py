# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce Constructor standings

# COMMAND ----------

from pyspark.sql.functions import sum,when,col,count

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

v_file_date

# COMMAND ----------

race_results_list = spark.read.format('delta').load(f"{presentation_folder_path}/race_results") \
.filter(f'file_date = "{v_file_date}"') \
.select('race_year') \
.distinct() \
.collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
  race_year_list.append(race_year.race_year)
  

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy('race_year','team') \
.agg(sum('points').alias('total_points'),
    count(when(col('position') == 1,True)).alias('wins'))


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

constructor_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))

# COMMAND ----------

final_df = constructor_standings_df.withColumn('rank',rank().over(constructor_rank_spec))

# COMMAND ----------

# overwrite_partition(final_df,'f1_presentation','constructor_standings','race_year')

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings order by race_year desc