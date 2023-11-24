-- Databricks notebook source
CREATE
OR REFRESH STREAMING LIVE TABLE raw_farmers_market AS
SELECT
  *
FROM
  cloud_files(
    "/databricks-datasets/data.gov/farmers_markets_geographic_data/data-001/",
    "csv",
    map(
      "cloudFiles.inferColumnTypes",
      "true"
    )
  )

-- COMMAND ----------

CREATE
OR REFRESH STREAMING LIVE TABLE farmers_market_clean (
  CONSTRAINT valid_website EXPECT (Website IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_location EXPECT (Location IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT
  *
FROM
  STREAM(live.raw_farmers_market) 

-- COMMAND ----------

CREATE
OR REFRESH STREAMING LIVE TABLE farmers_market_quarantine (
  CONSTRAINT valid_website EXPECT (NOT(Website IS NOT NULL)) ON VIOLATION DROP ROW,
  CONSTRAINT valid_location EXPECT (NOT(Location IS NOT NULL)) ON VIOLATION DROP ROW
) AS
SELECT
  *
FROM
  STREAM(live.raw_farmers_market)
