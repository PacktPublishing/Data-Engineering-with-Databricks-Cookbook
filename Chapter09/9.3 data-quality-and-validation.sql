-- Databricks notebook source
CREATE
OR REFRESH LIVE TABLE customers (
  CONSTRAINT valid_customer_key EXPECT (c_custkey IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT
  *
FROM
  samples.tpch.customer

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE duplicate_customers_test (
  CONSTRAINT unique_customer_key EXPECT (cnt = 1) ON VIOLATION DROP ROW
) AS
SELECT
  c_custkey, count(*) as cnt
FROM
  live.customers
GROUP BY ALL;

-- COMMAND ----------

CREATE
OR REFRESH LIVE TABLE orders (
  CONSTRAINT valid_order_key EXPECT (o_orderkey IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_key EXPECT (o_custkey IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_reference_customer EXPECT (cust.c_custkey IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT
  ord.*,
  cust.c_custkey
FROM
  samples.tpch.orders ord
  LEFT OUTER JOIN live.customers cust on cust.c_custkey = ord.o_custkey

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE duplicate_orders_test (
  CONSTRAINT unique_order_key EXPECT (cnt = 1) ON VIOLATION DROP ROW
) AS
SELECT
  o_orderkey, count(*) as cnt
FROM
  live.orders
GROUP BY ALL;
