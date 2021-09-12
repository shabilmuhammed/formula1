-- Databricks notebook source
create database if not exists demo;

-- COMMAND ----------

 show databases

-- COMMAND ----------

describe database demo

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managed Tables

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode('overwrite').format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

desc demo.race_results_python

-- COMMAND ----------

create table demo.race_results_sql
as
select * from demo.race_results_python
where race_year = 2020

-- COMMAND ----------

select * from demo.race_results_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### External Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').option('path',f'{presentation_folder_path}/race_results_ext_py').saveAsTable('demo.race_results_ext_py')

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

create table demo.race_results_ext_sql
()
USING parquet 
location '/mnt/formula1dl007/presentation/race_results_ext_sql'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Views

-- COMMAND ----------

create or replace temp view v_race_results
as 
select * from demo.race_results_python

-- COMMAND ----------

create or replace global temp view gv_race_results
as 
select * from demo.race_results_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Permanent view

-- COMMAND ----------

create or replace view pv_race_results
as 
select * from demo.race_results_python

-- COMMAND ----------

show tables

-- COMMAND ----------

