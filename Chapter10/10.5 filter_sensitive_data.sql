-- Databricks notebook source
-- Create a sample table with customer information
USE CATALOG de_book;
USE SCHEMA credit_card;

CREATE TABLE customer (
  id INT,
  name STRING,
  email STRING,
  phone STRING,
  ssn STRING,
  country STRING
);

-- Insert some sample data into the table
INSERT INTO customer VALUES
(1, 'Alice', 'alice@example.com', '+1-111-1111', '111--111-1111','USA'),
(2, 'Bob', 'bob@example.com', '+1-222-2222', '222-222-2222','USA'),
(3, 'Charlie', 'charlie@example.com', '+1-333-3333', '333-333-3333','USA'),
(4, 'David', 'david@example.com', '+44-444-4444','444-444-4444', 'UK'),
(5, 'Eve', 'eve@example.com', '+44-555-5555', '+555-555-5555','UK');


-- COMMAND ----------

CREATE FUNCTION country_filter(country STRING)
RETURN IF(IS_ACCOUNT_GROUP_MEMBER('admin'), true, country='USA');

-- COMMAND ----------

ALTER TABLE customer SET ROW FILTER country_filter ON (country);

-- COMMAND ----------

CREATE
OR REPLACE FUNCTION country_filter(country STRING) RETURN IF(
  IS_ACCOUNT_GROUP_MEMBER('admin'),
  true,
  IF(
    IS_ACCOUNT_GROUP_MEMBER('usteam')
    AND country = 'USA',
    true,
    IF(
      IS_ACCOUNT_GROUP_MEMBER('ukteam')
      AND country = 'UK',
      true,
      false
    )
  )
);

-- COMMAND ----------

ALTER TABLE customer DROP ROW FILTER;

-- COMMAND ----------

DROP FUNCTION country_filter;

-- COMMAND ----------

-- Create a UDF that masks the email column by replacing the domain part with '***'
CREATE FUNCTION mask_email (email STRING) RETURN CASE
  WHEN is_account_group_member('hr_dept') THEN email
  ELSE CONCAT(SPLIT(email, '@')[0], '@***')
END;

-- COMMAND ----------

ALTER TABLE customer ALTER COLUMN email SET MASK mask_email;

-- COMMAND ----------

ALTER TABLE customer ALTER COLUMN email DROP MASK;

-- COMMAND ----------

CREATE
OR REPLACE FUNCTION mask_email (email STRING) RETURN CASE
  WHEN is_account_group_member('hr_dept')
  OR is_account_group_member('finance_dept') THEN email
  ELSE CONCAT(SPLIT(email, '@') [0], '@***')
END;

-- COMMAND ----------

ALTER TABLE customer ALTER COLUMN email DROP MASK;
DROP FUNCTION mask_email;

-- COMMAND ----------


