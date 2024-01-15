-- Databricks notebook source
CREATE WIDGET TEXT catalog DEFAULT "main";
CREATE WIDGET TEXT schema DEFAULT "on_shelf_availability";

-- COMMAND ----------

USE CATALOG ${catalog}
USE SCHEMA ${schema};

-- COMMAND ----------

DROP VOLUME IF NOT EXISTS data;
DROP SCHEMA IF NOT EXISTS ${schema} CASCADE;
