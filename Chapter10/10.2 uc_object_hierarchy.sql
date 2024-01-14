-- Databricks notebook source
CREATE CATALOG de_book MANAGED LOCATION 's3://data-lake/de-book-ext-data';

-- COMMAND ----------

USE CATALOG de_book;
CREATE SCHEMA credit_card;

-- COMMAND ----------

USE CATALOG de_book;
USE SCHEMA credit_card;
CREATE TABLE IF NOT EXISTS transactions_table (
  Transaction_ID STRING,
  Transaction_Date STRING,
  Credit_Card_ID STRING,
  Transaction_Value FLOAT,
  Transaction_Segment STRING
);
INSERT INTO
  transactions_table
VALUES
  (    'CTID28830551',    '24-Apr-16',    '1629-9566-3285-2123',    23649,    'SEG25'  ),
  (    'CTID45504917',    '11-Feb-16',    '3697-6001-4909-5350',    26726,    'SEG16'  ),
  (    'CTID47312290',    '1-Nov-16',     '5864-4475-3659-1440',    22012,    'SEG14'  ),
  (    'CTID25637718',    '28-Jan-16',    '5991-4421-8476-3804',    37637,    'SEG17'  );

-- COMMAND ----------

USE CATALOG de_book;
USE SCHEMA credit_card;
CREATE OR REPLACE VIEW transactions_view (Credit_Card_ID, total_Transaction_Value)
COMMENT 'A view that shows the total transaction value by credit card'
AS SELECT Credit_Card_ID, SUM(Transaction_Value) AS total_Transaction_Value FROM de_book.credit_card.transactions_table GROUP BY Credit_Card_ID;

-- COMMAND ----------

SELECT * FROM transactions_table WHERE Transaction_Value > 25000;

-- COMMAND ----------

SELECT *  FROM transactions_view;

-- COMMAND ----------

CREATE EXTERNAL VOLUME de_book.credit_card.files
LOCATION 's3://data-lake/de-book-ext-data/files';

-- COMMAND ----------


