-- Databricks notebook source
CREATE WIDGET TEXT catalog DEFAULT "main";
CREATE WIDGET TEXT schema DEFAULT "on_shelf_availability";

-- COMMAND ----------

USE CATALOG ${catalog}

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${schema};

-- COMMAND ----------

USE SCHEMA ${schema};

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS data;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.jobs.taskValues.set(key = 'volumeName', value = 'data')
