-- ============================================================
-- Script 01: Environment Setup & Data Load
-- Project  : SA Retail Bank Customer Churn Analysis
-- Author   : Dominic Dandajena
-- Description:
--   Establishes the project database and schema architecture,
--   defines raw table structures, and loads source data via
--   BULK INSERT.
--
--   Two schemas are used to create a clear separation of
--   environments. The raw schema preserves source data exactly
--   as received — unmodified and auditable. The staging schema
--   will hold cleaned, transformed, analysis-ready tables.
--
--   All columns are defined as NVARCHAR to absorb the full
--   range of source data without type conversion failures.
--   Type enforcement and constraint logic is applied
--   deliberately in the staging layer, not at load time.
--
--   BULK INSERT is used in place of GUI import wizards to
--   ensure the load process is scripted, repeatable, and
--   version controlled. File paths reference local CSV exports
--   from the data generation script. Source files are available
--   in the /data folder of the repository.
-- ============================================================

USE master;
GO

CREATE DATABASE BankChurnAnalysis;
GO

USE BankChurnAnalysis;
GO

CREATE SCHEMA raw;
GO

CREATE SCHEMA staging;
GO

CREATE TABLE raw.customers (
    customer_id     NVARCHAR(50),
    first_name      NVARCHAR(100),
    last_name       NVARCHAR(100),
    date_of_birth   NVARCHAR(50),
    gender          NVARCHAR(50),
    province        NVARCHAR(50),
    branch_code     NVARCHAR(50),
    join_date       NVARCHAR(50),
    account_status  NVARCHAR(50),
    monthly_income  NVARCHAR(50)
);

CREATE TABLE raw.products (
    customer_id     NVARCHAR(50),
    product_name    NVARCHAR(50),
    open_date       NVARCHAR(50),
    product_status  NVARCHAR(50)
);

CREATE TABLE raw.activity (
    customer_id             NVARCHAR(50),
    avg_monthly_balance     NVARCHAR(50),
    credit_score            NVARCHAR(50),
    num_transactions_3m     NVARCHAR(50),
    is_active_member        NVARCHAR(50),
    last_txn_date           NVARCHAR(50),
    churned                 NVARCHAR(50),
    churn_date              NVARCHAR(50)
);

BULK INSERT raw.customers
FROM 'D:\Documents (D)\Work\Portfolio Projects\Customer Churn\raw customers.csv'
WITH (
    FORMAT          = 'CSV',
    FIRSTROW        = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR   = '0x0a',
    TABLOCK
);

BULK INSERT raw.products
FROM 'D:\Documents (D)\Work\Portfolio Projects\Customer Churn\raw products.csv'
WITH (
    FORMAT          = 'CSV',
    FIRSTROW        = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR   = '0x0a',
    TABLOCK
);

BULK INSERT raw.activity
FROM 'D:\Documents (D)\Work\Portfolio Projects\Customer Churn\raw activity.csv'
WITH (
    FORMAT          = 'CSV',
    FIRSTROW        = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR   = '0x0a',
    TABLOCK
);

-- Row counts validated against source files post-load
-- Expected: raw.customers = 8030, raw.products = 17282, raw.activity = 8050
SELECT 
    'raw.customers' AS table_name, 
     COUNT(*) AS row_count 
FROM raw.customers
UNION ALL
SELECT 
    'raw.products',
     COUNT(*) 
FROM raw.products
UNION ALL
SELECT
    'raw.activity', 
     COUNT(*) 
FROM raw.activity;
