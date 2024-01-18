-- Databricks notebook source
CREATE WIDGET TEXT catalog DEFAULT "main";
CREATE WIDGET TEXT schema DEFAULT "on_shelf_availability";


-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC os.environ['catalog']=dbutils.widgets.get("catalog")
-- MAGIC os.environ['schema']=dbutils.widgets.get("schema")
-- MAGIC os.environ['volumeName']=dbutils.jobs.taskValues.get(taskKey = "Setup", key = "volumeName", default = "data", debugValue = "data") 

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC cd /Volumes/$catalog/$schema/${volumeName}
-- MAGIC wget https://raw.githubusercontent.com/tredenceofficial/OSA-Data/main/osa_raw_data.csv
-- MAGIC wget https://raw.githubusercontent.com/tredenceofficial/OSA-Data/main/vendor_leadtime_info.csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC file_path = f'/Volumes/{dbutils.widgets.get("catalog")}/{dbutils.widgets.get("schema")}/{os.environ["volumeName"]}/osa_raw_data.csv'
-- MAGIC if os.path.exists(file_path):
-- MAGIC     file_size = os.path.getsize(file_path)
-- MAGIC     dbutils.jobs.taskValues.set(key='file_size', value=file_size)
-- MAGIC     print(file_size)
-- MAGIC else:
-- MAGIC     print(f"{file_path} does not exist")
-- MAGIC     dbutils.jobs.taskValues.set(key='file_size', value=0)

-- COMMAND ----------


