-- Databricks notebook source
select * from f1_presentation.calculated_race_results

-- COMMAND ----------

select team_name,
       count(1) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by team_name
having count(1) >= 100
ORDER BY avg_points desc

-- COMMAND ----------

select team_name,
       count(1) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year BETWEEN 2011 and 2020
group by team_name
having count(1) >= 100
ORDER BY avg_points desc

-- COMMAND ----------

