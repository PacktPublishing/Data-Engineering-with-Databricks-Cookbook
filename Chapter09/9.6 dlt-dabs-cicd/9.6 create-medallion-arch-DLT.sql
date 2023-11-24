-- Databricks notebook source
CREATE
OR REFRESH STREAMING TABLE device_data AS
SELECT
  *
FROM
  cloud_files(
    "/databricks-datasets/iot-stream/data-device",
    "json",
    map("cloudFiles.inferColumnTypes", "true")
  )

-- COMMAND ----------

CREATE
OR REFRESH STREAMING TABLE user_data AS
SELECT
  *
FROM
  cloud_files(
    "/databricks-datasets/iot-stream/data-user",
    "csv",
    map("cloudFiles.inferColumnTypes", "true")
  )

-- COMMAND ----------

CREATE
OR REFRESH STREAMING TABLE user_data_prepared (
  CONSTRAINT valid_user EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
) -- COMMENT ""
AS
SELECT
  users.userid as user_id,
  CASE
    WHEN users.gender = 'F' THEN 'Female'
    WHEN users.gender = 'M' THEN 'Male'
  END AS gender,
  users.age,
  users.height,
  users.weight,
  CAST(users.smoker as BOOLEAN) AS isSmoker,
  CAST(users.familyhistory as BOOLEAN) AS hasFamilyHistory,
  users.cholestlevs AS cholestrolLevels,
  users.bp AS bloodPressure,
  users.risk
FROM
  STREAM(live.user_data) users;

-- COMMAND ----------

CREATE
OR REFRESH STREAMING TABLE device_data_prepared (
  CONSTRAINT valid_timestamp EXPECT (timestamp IS NOT NULL)
) -- COMMENT ""
AS
SELECT
  device.id,
  device.device_id,
  device.user_id,
  device.calories_burnt,
  device.miles_walked,
  device.num_steps,
  CAST(device.timestamp as TIMESTAMP) AS timestamp
FROM
  STREAM(live.device_data) device

-- COMMAND ----------

CREATE
OR REFRESH LIVE TABLE user_metrics AS
SELECT
  users.user_id,
  users.gender,
  users.age,
  users.height,
  users.weight,
  users.isSmoker,
  users.hasFamilyHistory,
  users.cholestrolLevels,
  users.bloodPressure,
  users.risk,
  SUM(devices.calories_burnt) AS totalCaloriesBurnt,
  SUM(devices.miles_walked) AS totalMilesWalked,
  SUM(devices.num_steps) AS totalNumberOfSteps
FROM
  live.user_data_prepared users
  LEFT OUTER JOIN LIVE.device_data_prepared devices on devices.user_id = users.user_id
GROUP BY
  ALL;
