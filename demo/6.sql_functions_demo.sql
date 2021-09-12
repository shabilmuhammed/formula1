-- Databricks notebook source
use f1_processed

-- COMMAND ----------

select nationality,name,dob,rank() over(partition by nationality order by dob desc) as age_rank from drivers
order by nationality,age_rank

-- COMMAND ----------

