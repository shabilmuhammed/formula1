-- Databricks notebook source
create database if not exists f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Circuits Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
circuitid INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
options (header true)
LOCATION '/mnt/formula1dl007/raw/circuits.csv'

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING csv
options (header true)
LOCATION '/mnt/formula1dl007/raw/races.csv';

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Constructors table (json)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
using json
LOCATION '/mnt/formula1dl007/raw/constructors.json';

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC  %md
-- MAGIC  ### Create Drivers Table JSON

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING)
using json
LOCATION '/mnt/formula1dl007/raw/drivers.json';

-- COMMAND ----------

SELECT * FROM  f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Results Table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText String,
PositionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
StatusId String)
using json
LOCATION '/mnt/formula1dl007/raw/results.json';

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create pitstops table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
using json
options(multiLine true)
LOCATION '/mnt/formula1dl007/raw/pit_stops.json';

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create lap_times table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time string,
milliseconds INT
)
USING csv
LOCATION '/mnt/formula1dl007/raw/lap_times';

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create qualifying table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 string,
q2 string,
q3 string,
qualifyId INT,
raceId int
)
USING json
options(multiLine true)
LOCATION '/mnt/formula1dl007/raw/qualifying';

-- COMMAND ----------

select count(*) from f1_raw.qualifying;

-- COMMAND ----------

