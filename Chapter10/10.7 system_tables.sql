-- Databricks notebook source
-- MAGIC %sh 
-- MAGIC curl -v -X GET -H "Authorization: Bearer <PAT Token>" "https://<workspace>.cloud.databricks.com/api/2.0/unity-catalog/metastores/<metastore-id>/systemschemas"

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC curl -v -X POST -H "Authorization: Bearer <PAT Token>" "https://<workspace>.cloud.databricks.com/api/2.0/unity-catalog/metastores/<metastore-id>/systemschemas/<schema-name>/enable"

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC curl -v -X POST -H "Authorization: Bearer <PAT Token>" "https://<workspace>.cloud.databricks.com/api/2.0/unity-catalog/metastores/<metastore-id>/systemschemas/system.access/enable"

-- COMMAND ----------

GRANT SELECT ON TABLE system.access.audit TO analysts

-- COMMAND ----------

REVOKE SELECT ON TABLE system.billing.usage FROM developers

-- COMMAND ----------

SELECT
  user_identity.email as user_id,
  COUNT(*) AS event_count
FROM
  system.access.audit
WHERE
  event_time >= current_date - interval 30 days
GROUP BY
  user_id
ORDER BY
  event_count DESC
LIMIT
  10

-- COMMAND ----------



-- COMMAND ----------

SELECT
  b.sku_name,
  SUM(b.usage_quantity) AS usage_hours,
  SUM(b.usage_quantity * p.pricing.default) AS cost
FROM
  system.billing.usage AS b
  JOIN system.billing.list_prices AS p ON b.sku_name = p.sku_name
  AND b.usage_date BETWEEN p.price_start_time
  AND coalesce(p.price_end_time, current_timestamp())
WHERE
  b.usage_start_time >= date_trunc('month', current_date) - interval 1 month
  AND b.usage_start_time < date_trunc('month', current_date)
GROUP BY
  b.sku_name
ORDER BY
  cost DESC

-- COMMAND ----------

SELECT
  source_table_full_name,
  target_table_full_name,
  event_time,
  created_by,
  entity_type,
  entity_id,
  entity_run_id
FROM
  system.access.table_lineage
WHERE
  target_table_full_name = 'de_book.credit_card.usa_customers'
ORDER BY
  event_time DESC
