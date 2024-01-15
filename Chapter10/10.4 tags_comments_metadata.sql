-- Databricks notebook source
COMMENT ON TABLE de_book.credit_card.transactions_table IS 'This table contains transaction information from the credit_card database';

-- COMMAND ----------

ALTER TABLE
  de_book.credit_card.transactions_table
SET
  TAGS (
    'business_unit' = 'finance',
    'data_sensitivity' = 'medium',
    'data_quality' = 'high'
  );

-- COMMAND ----------

ALTER TABLE
  de_book.credit_card.transactions_table
ALTER COLUMN
  Transaction_ID COMMENT 'A unique identifier for the transaction.';

-- COMMAND ----------

ALTER TABLE
  de_book.credit_card.transactions_table
ALTER COLUMN
  Transaction_ID
SET
  TAGS (
    'data_protection' = 'non-PII',
    'isIdentifier' = 'true'
  );

-- COMMAND ----------

DESCRIBE DETAIL de_book.credit_card.transactions_table;

-- COMMAND ----------

SELECT
  catalog_name,
  schema_name,
  table_name,
  tag_name,
  tag_value
FROM
  de_book.information_schema.table_tags
WHERE
  catalog_name = 'de_book'
  and schema_name = 'credit_card'
  and table_name = 'transactions_table';

-- COMMAND ----------

ALTER TABLE
  de_book.credit_card.transactions_table UNSET TAGS ('business_unit', 'data_sensitivity');

-- COMMAND ----------

ALTER TABLE
  de_book.credit_card.transactions_table
ALTER COLUMN
  Transaction_ID UNSET TAGS ('data_type');

-- COMMAND ----------


