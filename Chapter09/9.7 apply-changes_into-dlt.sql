-- Databricks notebook source
CREATE TEMPORARY STREAMING LIVE VIEW movie_and_show_titles AS
SELECT
  *,
  now() as ts
FROM
  cloud_files(
    "/Volumes/main/netflix/data",
    "csv",
    map("header", "true")
  );

-- COMMAND ----------

CREATE
OR REFRESH STREAMING TABLE movie_and_show_titles_scd_1;

APPLY CHANGES INTO 
  live.movie_and_show_titles_scd_1
FROM 
  STREAM(LIVE.movie_and_show_titles)
KEYS 
  (type, title, director) 
SEQUENCE BY 
  ts 
COLUMNS * EXCEPT (ts)
STORED AS 
  SCD TYPE 1;

-- COMMAND ----------

CREATE
OR REFRESH STREAMING TABLE movie_and_show_titles_scd_2;

APPLY CHANGES INTO 
  live.movie_and_show_titles_scd_2
FROM 
  STREAM(LIVE.movie_and_show_titles)
KEYS 
  (type, title, director)
SEQUENCE BY 
  ts 
COLUMNS * EXCEPT (ts)
STORED AS 
  SCD TYPE 2;
