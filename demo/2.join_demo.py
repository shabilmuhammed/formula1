# Databricks notebook source
sql%run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").filter("circuit_id < 70").withColumnRenamed('name','circuit_name')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"inner") \
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outer join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"left") \
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"right") \
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"outer") \
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### semi join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"semi") \
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

