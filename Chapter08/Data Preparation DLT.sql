-- Databricks notebook source
CREATE
OR REFRESH STREAMING LIVE TABLE inventory_raw AS
SELECT
  *
FROM
  cloud_files(
    "/Volumes/dbdemos/on-shelf-availability/data/osa_raw_data*.csv",
    "csv",
    map(
      "cloudFiles.inferColumnTypes",
      "true",
      "dateFormat",
      "yyyyMMdd",
      "cloudFiles.schemaHints",
      "date DATE"
    )
  )

-- COMMAND ----------

CREATE
OR REFRESH STREAMING LIVE TABLE vendor AS
SELECT
  *
FROM
  cloud_files(
    "/Volumes/dbdemos/on-shelf-availability/data/vendor_leadtime_info*.csv",
    "csv",
    map("cloudFiles.inferColumnTypes", "true")
  )

-- COMMAND ----------



-- COMMAND ----------

CREATE
OR REFRESH LIVE TABLE inventory AS
SELECT
  cross_view.date,
  cross_view.store_id,
  cross_view.sku,
  int_data.product_category,
  int_data.total_sales_units,
  int_data.on_hand_inventory_units,
  int_data.replenishment_units,
  int_data.inventory_pipeline,
  int_data.units_in_transit,
  int_data.units_in_dc,
  int_data.units_on_order,
  int_data.units_under_promotion,
  int_data.shelf_capacity,
  CASE WHEN int_data.units_under_promotion > 0 THEN 1 ELSE 0 END as promotion_flag,
  CASE WHEN int_data.replenishment_units > 0 THEN 1 ELSE 0 END as replenishment_flag
FROM
  (
    SELECT
      to_date(
        date_add('2019-01-01', cast(abs(t.id) as int)),
        'yy-MM-dd'
      ) as date,
      store_id,
      sku
    FROM
      range(datediff('2019-01-01', '2021-05-03'), 1) AS t
      CROSS JOIN (
        SELECT
          store_id,
          sku
        FROM
          live.inventory_raw
        GROUP BY
          ALL
      )
  ) cross_view
  LEFT OUTER JOIN live.inventory_raw int_data ON cross_view.date = int_data.date
  AND cross_view.store_id = int_data.store_id
  AND cross_view.sku = int_data.sku

-- COMMAND ----------


