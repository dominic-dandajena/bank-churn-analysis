-- ============================================================
-- Script 06: Star Schema Build (Demonstrative)
-- Project  : SA Retail Bank Customer Churn Analysis
-- Author   : Dominic Dandajena
-- Description:
--   Builds a star schema alongside the wide analytical table
--   created in Script 05. This schema is not used as the
--   primary Power BI data source for this report — that role
--   is filled by staging.churn_analysis for performance
--   reasons documented in Script 05.
--
--   This script exists purely to demonstrate my data modelling
--   knowledge. In a production environment serving multiple
--   reports, teams, or larger data volumes, a star schema
--   with separate fact and dimension tables would be the
--   preferred architecture over a single wide table.
--   It offers greater flexibility, reusability across
--   multiple analytical use cases, more efficient storage,
--   and cleaner separation of business dimensions from
--   transactional facts.
--
--   The schema consists of:
--   staging.fact_churn      — one row per customer,
--                             numeric measures and foreign keys
--   staging.dim_customer    — customer demographic attributes
--   staging.dim_product     — product holding attributes
--                             per customer
--   staging.dim_date        — date dimension spanning the
--                             full dataset window
--
--   All dimension tables use surrogate keys rather than
--   natural keys from the source system. This is standard
--   practice in data warehouse design — natural keys are
--   not guaranteed to be stable or unique across source
--   systems, particularly after deduplication and cleaning.
--
-- Date Reference:
--   Date dimension spans 2015-01-01 to 2024-12-31,
--   covering the full dataset window from the earliest
--   valid join date to the dataset reference date.
-- ============================================================

USE BankChurnAnalysis;
GO

-- ============================================================
-- DIMENSION 1: dim_customer
-- Customer demographic and relationship attributes.
-- One row per customer.
-- ============================================================

IF OBJECT_ID('staging.dim_customer', 'U') IS NOT NULL
    DROP TABLE staging.dim_customer;

SELECT
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY customer_id)           AS customer_key,

    -- Natural key retained for traceability back to source
    customer_id,

    -- Demographics
    first_name,
    last_name,
    date_of_birth,
    age,
    age_band,
    gender,
    province,
    branch_code,

    -- Account attributes
    join_date,
    is_valid_join_date,
    account_status,
    tenure_days,
    tenure_band,
    monthly_income,
    income_tier

INTO staging.dim_customer
FROM staging.customers;

SELECT * FROM staging.dim_customer

-- ============================================================
-- DIMENSION 2: dim_product
-- Product holding attributes per customer.
-- One row per customer summarising their product portfolio.
-- Product-level detail is pre-aggregated here consistent
-- with the approach taken in Script 05 — a fully
-- normalised product dimension with one row per product
-- per customer would require a bridge table to resolve
-- the many-to-many relationship between customers and
-- products, which adds complexity beyond the scope of
-- this project.
-- ============================================================

IF OBJECT_ID('staging.dim_product', 'U') IS NOT NULL
    DROP TABLE staging.dim_product;

WITH product_summary AS (
    SELECT
        customer_id,
        COUNT(*)                                       AS total_products,
        COUNT(CASE WHEN product_status = 'Active'
            THEN 1 END)                                AS active_products,
        COUNT(CASE WHEN product_status = 'Closed'
            THEN 1 END)                                AS closed_products,
        MAX(CASE WHEN product_name = 'Cheque Account'
            THEN 1 ELSE 0 END)                         AS has_cheque_account,
        MAX(CASE WHEN product_name = 'Savings Account'
            THEN 1 ELSE 0 END)                         AS has_savings_account,
        MAX(CASE WHEN product_name = 'Home Loan'
            THEN 1 ELSE 0 END)                         AS has_home_loan,
        MAX(CASE WHEN product_name = 'Credit Card'
            THEN 1 ELSE 0 END)                         AS has_credit_card,
        MAX(CASE WHEN product_name = 'Personal Loan'
            THEN 1 ELSE 0 END)                         AS has_personal_loan
    FROM staging.products
    GROUP BY customer_id
)
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id)           AS product_key,
    customer_id,
    total_products,
    active_products,
    closed_products,
    has_cheque_account,
    has_savings_account,
    has_home_loan,
    has_credit_card,
    has_personal_loan
INTO staging.dim_product
FROM product_summary;

-- ============================================================
-- DIMENSION 3: dim_date
-- Standard date dimension spanning the full dataset window
-- from the earliest valid join date to the dataset
-- reference date. Provides date attributes for time
-- intelligence in Power BI.
-- ============================================================

IF OBJECT_ID('staging.dim_date', 'U') IS NOT NULL
    DROP TABLE staging.dim_date;

WITH date_series AS (
    SELECT CAST('2015-01-01' AS DATE)                  AS date_value
    UNION ALL
    SELECT DATEADD(DAY, 1, date_value)
    FROM date_series
    WHERE date_value < '2024-12-31'
)
SELECT
    -- Surrogate key in YYYYMMDD format — standard convention
    -- for date dimension keys, readable and sortable
    CONVERT(INT, FORMAT(date_value, 'yyyyMMdd'))       AS date_key,
    date_value                                         AS full_date,
    YEAR(date_value)                                   AS year,
    MONTH(date_value)                                  AS month_number,
    DATENAME(MONTH, date_value)                        AS month_name,
    DATEPART(QUARTER, date_value)                      AS quarter,
    CONCAT('Q', DATEPART(QUARTER, date_value), ' ',
        YEAR(date_value))                              AS quarter_label,
    FORMAT(date_value, 'yyyy-MM')                      AS year_month,
    DATEPART(WEEK, date_value)                         AS week_number,
    DATEPART(WEEKDAY, date_value)                      AS day_of_week_number,
    DATENAME(WEEKDAY, date_value)                      AS day_of_week_name,
    DAY(date_value)                                    AS day_of_month,
    CASE WHEN DATEPART(WEEKDAY, date_value) IN (1, 7)
        THEN 1 ELSE 0 END                              AS is_weekend,
    CASE WHEN date_value = EOMONTH(date_value)
        THEN 1 ELSE 0 END                              AS is_month_end
INTO staging.dim_date
FROM date_series
OPTION (MAXRECURSION 4000);

-- ============================================================
-- FACT TABLE: fact_churn
-- One row per customer. Contains numeric measures and
-- foreign keys to all dimension tables.
-- The churn_date_key and join_date_key link to dim_date,
-- enabling time intelligence across both the customer
-- acquisition and churn timelines.
-- NULL date keys are used where dates are invalid or
-- absent. A NULL foreign key is preferred to
-- a dummy unknown date record for this project scope.
-- ============================================================

IF OBJECT_ID('staging.fact_churn', 'U') IS NOT NULL
    DROP TABLE staging.fact_churn;

SELECT
    -- Surrogate fact key
    ROW_NUMBER() OVER (ORDER BY a.customer_id)         AS churn_fact_key,

    -- Foreign keys to dimensions
    dc.customer_key,
    dp.product_key,

    -- Date foreign keys in YYYYMMDD format matching dim_date
    -- NULL where date is invalid or absent
    CASE WHEN c.is_valid_join_date = 1
        THEN CONVERT(INT, FORMAT(c.join_date, 'yyyyMMdd'))
        ELSE NULL
    END                                                AS join_date_key,

    CASE WHEN a.churn_date IS NOT NULL
        THEN CONVERT(INT, FORMAT(a.churn_date, 'yyyyMMdd'))
        ELSE NULL
    END                                                AS churn_date_key,

    -- Natural key retained for traceability
    a.customer_id,

    -- Measures
    a.avg_monthly_balance,
    a.credit_score,
    a.num_transactions_3m,
    c.monthly_income,
    c.tenure_days,

    -- Flags — stored as INT for DAX measure compatibility
    -- BIT columns require explicit casting in some DAX
    -- contexts — INT avoids this friction
    CAST(a.churned AS INT)                             AS churned,
    CAST(a.is_active_member AS INT)                    AS is_active_member,
    CAST(a.is_outlier_balance AS INT)                  AS is_outlier_balance,
    CAST(a.is_valid_last_txn AS INT)                   AS is_valid_last_txn,
    CAST(c.is_valid_join_date AS INT)                  AS is_valid_join_date,

    -- Band and tier attributes carried through for
    -- cases where the star schema is used for analysis
    -- rather than the wide table
    a.balance_tier,
    a.credit_band,
    c.income_tier,
    c.tenure_band,
    c.age_band

INTO staging.fact_churn
FROM staging.activity a
JOIN staging.customers c
    ON a.customer_id = c.customer_id
JOIN staging.dim_customer dc
    ON a.customer_id = dc.customer_id
JOIN staging.dim_product dp
    ON a.customer_id = dp.customer_id;

-- ============================================================
-- VALIDATION
-- ============================================================

-- Row counts across all four star schema tables
SELECT 'staging.dim_customer' AS table_name,
    COUNT(*) AS row_count
FROM staging.dim_customer
UNION ALL
SELECT 'staging.dim_product',
    COUNT(*)
FROM staging.dim_product
UNION ALL
SELECT 'staging.dim_date',
    COUNT(*)
FROM staging.dim_date
UNION ALL
SELECT 'staging.fact_churn',
    COUNT(*)
FROM staging.fact_churn;

-- Confirm fact table row count matches analytical table
-- Both should return 8000
SELECT COUNT(*) AS fact_churn_rows
FROM staging.fact_churn;

SELECT COUNT(*) AS churn_analysis_rows
FROM staging.churn_analysis;

-- Confirm all foreign keys in fact_churn resolve
-- to dim_customer — no orphan customer keys
SELECT COUNT(*) AS unmatched_customer_keys
FROM staging.fact_churn f
WHERE NOT EXISTS (
    SELECT 1 FROM staging.dim_customer d
    WHERE d.customer_key = f.customer_key
);

-- Confirm all foreign keys in fact_churn resolve
-- to dim_product — no orphan product keys
SELECT COUNT(*) AS unmatched_product_keys
FROM staging.fact_churn f
WHERE NOT EXISTS (
    SELECT 1 FROM staging.dim_product d
    WHERE d.product_key = f.product_key
);

-- Confirm date keys resolve to dim_date
-- where dates are valid
SELECT COUNT(*) AS unmatched_join_date_keys
FROM staging.fact_churn f
WHERE join_date_key IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM staging.dim_date d
    WHERE d.date_key = f.join_date_key
);

SELECT COUNT(*) AS unmatched_churn_date_keys
FROM staging.fact_churn f
WHERE churn_date_key IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM staging.dim_date d
    WHERE d.date_key = f.churn_date_key
);

-- Confirm churn rate is consistent between
-- fact table and analytical table
SELECT
    'fact_churn'                                       AS source,
    COUNT(*)                                           AS total_customers,
    SUM(churned)                                       AS total_churned,
    ROUND(CAST(SUM(churned) AS FLOAT)
        / COUNT(*) * 100, 2)                           AS churn_rate_pct
FROM staging.fact_churn
UNION ALL
SELECT
    'churn_analysis',
    COUNT(*),
    SUM(CAST(churned AS INT)),
    ROUND(CAST(SUM(CAST(churned AS INT)) AS FLOAT)
        / COUNT(*) * 100, 2)
FROM staging.churn_analysis;