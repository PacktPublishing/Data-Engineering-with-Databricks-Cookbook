-- Databricks notebook source
USE CATALOG de_book;
USE SCHEMA credit_card;

-- COMMAND ----------

CREATE
OR REPLACE TABLE usa_customers AS
SELECT
  *
FROM
  customer
WHERE
  country = 'USA';

-- COMMAND ----------

CREATE
  OR REPLACE TABLE uk_customers AS
SELECT
  *
FROM
  customer
WHERE
  country = 'UK';

-- COMMAND ----------


