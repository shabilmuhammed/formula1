-- Databricks notebook source
select driver_name,
       count(1) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by driver_name
having count(1) >= 50
ORDER BY avg_points desc

-- COMMAND ----------

select driver_name,
       count(1) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year BETWEEN 2011 and 2020
group by driver_name
having count(1) >= 50
ORDER BY avg_points desc

-- COMMAND ----------

