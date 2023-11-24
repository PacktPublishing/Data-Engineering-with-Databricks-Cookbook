-- Databricks notebook source
CREATE OR REPLACE VIEW main.pkc_farmers_market.event_log_raw AS SELECT * FROM event_log("5f5e0278-f9c8-49dc-bfd2-ade8c07b4453");

-- COMMAND ----------

-- DBTITLE 1,Query data quality from the event log
SELECT
  update_id,
  row_expectations.dataset as dataset,
  row_expectations.name as expectation,
  SUM(row_expectations.passed_records) as passing_records,
  SUM(row_expectations.failed_records) as failing_records
FROM
  (
    SELECT
      origin.update_id,explode(
        from_json(
          details :flow_progress :data_quality :expectations,
          "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
        )
      ) row_expectations
    FROM
      main.pkc_farmers_market.event_log_raw
    WHERE
      event_type = 'flow_progress'
  )
GROUP BY
  update_id,
  row_expectations.dataset,
  row_expectations.name

-- COMMAND ----------

-- DBTITLE 1,Monitor compute resource utilization
SELECT
  origin.update_id,
  timestamp,
  Double(details :cluster_resources.avg_num_queued_tasks) as queue_size,
  Double(
    details :cluster_resources.avg_task_slot_utilization
  ) as utilization,
  Double(details :cluster_resources.num_executors) as current_executors,
  Double(
    details :cluster_resources.latest_requested_num_executors
  ) as latest_requested_num_executors,
  Double(details :cluster_resources.optimal_num_executors) as optimal_num_executors,
  details :cluster_resources.state as autoscaling_state
FROM
  main.pkc_farmers_market.event_log_raw
WHERE
  event_type = 'cluster_resources'
ORDER BY
  origin.update_id,
  timestamp

-- COMMAND ----------

-- DBTITLE 1,Query user actions in the event log
SELECT
  timestamp,
  details :user_action :action,
  details :user_action :user_name
FROM
  main.pkc_farmers_market.event_log_raw
WHERE
  event_type = 'user_action'
