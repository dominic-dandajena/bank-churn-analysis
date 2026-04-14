-- ============================================================
-- Script 02: SQL Data Profiling
-- Project  : SA Retail Bank Customer Churn Analysis
-- Author   : Dominic Dandajena
-- Description:
--   Quantifies and documents data quality issues across all
--   three raw tables. Every cleaning decision made in Script 03
--   is based on the findings identified here.
--
--   Profiling is intentionally separated from transformation.
--   This script only observes and measures — it does not modify
--   any data. Results are recorded in the Data Quality Log
--   in the project README.
--
--   Exploratory observations identified during this process
--   are documented separately in Script 04 (EDA), keeping
--   a clear distinction between what is wrong with the data
--   and what is analytically interesting about it.
--
--   Each issue is assigned a severity rating:
--   Critical — breaks joins, causes row loss, or corrupts
--              analytical results if unresolved
--   High     — biases analysis or produces misleading
--              aggregations if unresolved
--   Medium   — cleanable inconsistencies that reduce data
--              usability but do not corrupt results
--   Low      — cosmetic formatting issues with no
--              analytical impact
--
-- Date Reference:
--   All date validations use the dataset reference date of
--   2024-12-31, which represents present day within the
--   fictional bank's timeline. Date issues are assessed
--   against this anchor, not the project execution date.
-- ============================================================

USE BankChurnAnalysis;
GO

-- ============================================================
-- SECTION 1: RAW CUSTOMERS
-- ============================================================

-- [DQ-01] Baseline row count
-- Confirms the load captured all expected source records.
-- Severity: N/A — diagnostic baseline
SELECT
    COUNT(*) AS total_rows
FROM raw.customers;

-- [DQ-02] NULL count across all columns
-- A broad sweep to establish which columns carry nulls
-- before targeted checks are applied below.
-- Severity: varies by column — see targeted checks below
SELECT
    SUM(CASE WHEN customer_id    IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN date_of_birth  IS NULL THEN 1 ELSE 0 END) AS null_dob,
    SUM(CASE WHEN gender         IS NULL THEN 1 ELSE 0 END) AS null_gender,
    SUM(CASE WHEN province       IS NULL THEN 1 ELSE 0 END) AS null_province,
    SUM(CASE WHEN join_date      IS NULL THEN 1 ELSE 0 END) AS null_join_date,
    SUM(CASE WHEN account_status IS NULL THEN 1 ELSE 0 END) AS null_account_status,
    SUM(CASE WHEN monthly_income IS NULL THEN 1 ELSE 0 END) AS null_monthly_income
FROM raw.customers;

-- [DQ-03a] Duplicate CustomerIDs
-- Severity: High — duplicate records inflate customer counts
-- and bias all aggregations if unresolved.
-- A CustomerID appearing more than once indicates either
-- a true duplicate row or a false duplicate where the same
-- ID was assigned to two different records. DQ-03b
-- investigates which case applies.
SELECT
    customer_id,
    COUNT(*) AS occurrence_count
FROM raw.customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;

-- [DQ-03b] Duplicate CustomerID — false duplicate verification
-- DQ-03 confirmed 30 duplicate CustomerIDs. This query
-- investigates whether they represent identical rows or
-- distinct rows sharing the same ID.
-- Partitioning by customer_id only and ordering by
-- monthly_income ASC reveals that each duplicate pair
-- differs only in monthly_income — the duplicate row
-- carries a 5% income uplift on the original, consistent
-- with a migration artifact where the same record was
-- exported twice at different points in time.
-- The lower monthly_income value is therefore reliably
-- the original record across all 30 pairs.
-- This ordering logic is carried forward into the
-- staging deduplication in Script 03, where row_num = 1
-- is retained and row_num = 2 is discarded.
WITH duplicate_check AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY TRY_CONVERT(FLOAT, monthly_income) ASC
        ) AS row_num
    FROM raw.customers
)
SELECT *
FROM duplicate_check
WHERE customer_id IN (
    SELECT customer_id
    FROM raw.customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
)
ORDER BY customer_id, row_num;

-- [DQ-04] Gender value variants
-- Severity: Medium — inconsistent formatting reduces usability
-- but does not corrupt results. Standardisation to Male/Female
-- is handled in staging.
SELECT
    gender,
    COUNT(*) AS count
FROM raw.customers
GROUP BY gender
ORDER BY count DESC;

-- [DQ-05] Province value variants
-- Severity: High — province fragmentation across 26 variants
-- biases any geographic analysis run against the raw data.
-- A mapping CTE in staging collapses these to 9 clean values.
SELECT
    province,
    COUNT(*) AS count
FROM raw.customers
GROUP BY province
ORDER BY count DESC;

-- [DQ-06] Future join dates
-- Severity: High — join dates beyond the dataset reference
-- date of 2024-12-31 are invalid within the bank's timeline
-- and corrupt tenure calculations if included.
-- These records are flagged in staging and excluded from
-- any date-dependent derivations.
SELECT
    COUNT(*) AS future_join_dates
FROM raw.customers
WHERE TRY_CONVERT(DATE, join_date) > '2024-12-31';

-- [DQ-07] Monthly-income invalid value check
-- Severity: Low — investigation confirmed no zero or negative
-- income values are present. The minimum income floor of
-- R3,500 represents South Africa's approximate minimum wage
-- and is a valid data point, not an error.
SELECT
    COUNT(CASE WHEN TRY_CONVERT(FLOAT, monthly_income) IS NULL
               THEN 1 END) AS unparseable_income_value,
    COUNT(CASE WHEN TRY_CONVERT(FLOAT, monthly_income) <= 0
               THEN 1 END) AS zero_or_negative_income
FROM raw.customers;

-- [DQ-08] Account status null check
-- Severity: Medium — account status is a business critical
-- field for segmentation. A customer without a status cannot
-- be correctly classified as active, inactive, or closed
-- in churn analysis. Nulls are imputed as Unknown in staging
-- rather than dropping the row.
SELECT
    COUNT(CASE WHEN account_status IS NULL
               THEN 1 END) AS null_account_status
FROM raw.customers;

-- ============================================================
-- SECTION 2: RAW PRODUCTS
-- ============================================================

-- [DQ-09] Baseline row count
-- Confirms the load captured all expected product records.
-- Multiple rows per customer are expected given the
-- one-row-per-product structure of this table.
-- Severity: N/A — diagnostic baseline
SELECT COUNT(*) AS total_rows
FROM raw.products;

-- [DQ-10] Product name variants
-- Severity: Medium — inconsistent naming reduces usability
-- but does not corrupt results. A mapping CTE in staging
-- standardises these to 5 clean product names.
SELECT
    product_name,
    COUNT(*) AS count
FROM raw.products
GROUP BY product_name
ORDER BY count DESC;

-- [DQ-11] Product status null check
-- Severity: Medium — product status is business critical
-- for classifying active vs closed product holdings,
-- which feeds into the product count churn predictor.
-- Nulls are imputed as Unknown in staging.
SELECT
    COUNT(CASE WHEN product_status IS NULL
               THEN 1 END) AS null_product_status
FROM raw.products;

-- [DQ-12] Product open dates pre-dating bank open date
-- Severity: High — open dates preceding 2015-01-01 are
-- likely default placeholder dates from a legacy migration.
-- Including these in product tenure calculations would
-- produce misleading results. Flagged with
-- is_valid_open_date = 0 in staging.
SELECT
    COUNT(*) AS suspect_open_dates
FROM raw.products
WHERE TRY_CONVERT(DATE, open_date) < '2015-01-01';

-- [DQ-13] Product open date before customer join date
-- Severity: Critical — a product cannot have been opened
-- before the customer joined the bank. This is a
-- referential integrity violation that corrupts any
-- product tenure or relationship duration calculation.
-- A subquery is used to deduplicate customers and exclude
-- invalid join dates before the comparison, preventing
-- inflated counts from duplicate CustomerIDs and
-- future-dated join records.
SELECT COUNT(*) AS open_before_join
FROM raw.products p
JOIN (
    SELECT
        customer_id,
        MIN(TRY_CONVERT(DATE, join_date)) AS join_date
    FROM raw.customers
    WHERE TRY_CONVERT(DATE, join_date) IS NOT NULL
    GROUP BY customer_id
) c ON p.customer_id = c.customer_id
WHERE TRY_CONVERT(DATE, p.open_date) < c.join_date;

-- ============================================================
-- SECTION 3: RAW ACTIVITY
-- ============================================================

-- [DQ-14] Baseline row count
-- Confirms the load captured all expected activity records.
-- The slight excess over 8000 is consistent with the
-- deliberate inclusion of orphan records in the source data.
-- Severity: N/A — diagnostic baseline
SELECT COUNT(*) AS total_rows
FROM raw.activity;

-- [DQ-15] NULL count across all columns
-- Broad null sweep across the activity table before
-- targeted checks are applied below.
-- Severity: varies by column — see targeted checks below
SELECT
    SUM(CASE WHEN customer_id           IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN avg_monthly_balance   IS NULL THEN 1 ELSE 0 END) AS null_balance,
    SUM(CASE WHEN credit_score          IS NULL THEN 1 ELSE 0 END) AS null_credit_score,
    SUM(CASE WHEN num_transactions_3m   IS NULL THEN 1 ELSE 0 END) AS null_txn_count,
    SUM(CASE WHEN is_active_member      IS NULL THEN 1 ELSE 0 END) AS null_active_member,
    SUM(CASE WHEN last_txn_date         IS NULL THEN 1 ELSE 0 END) AS null_last_txn,
    SUM(CASE WHEN churned               IS NULL THEN 1 ELSE 0 END) AS null_churned,
    SUM(CASE WHEN churn_date            IS NULL THEN 1 ELSE 0 END) AS null_churn_date
FROM raw.activity;

-- [DQ-16] Orphan CustomerIDs
-- Severity: Critical — activity records with no corresponding
-- customer in raw.customers cannot be linked to demographic
-- or product data. Including them breaks joins and produces
-- incomplete analytical records. Excluded entirely from
-- staging via an EXISTS filter.
-- NOT EXISTS is used in preference to a LEFT JOIN for
-- readability and performance — it stops searching the
-- moment a match is found rather than processing all
-- joins before filtering.
SELECT COUNT(*) AS orphan_records
FROM raw.activity a
WHERE NOT EXISTS (
    SELECT 1
    FROM raw.customers c
    WHERE c.customer_id = a.customer_id
);

-- [DQ-17] Last transaction date before customer join date
-- Severity: High — transaction dates pre-dating the customer's
-- join date are impossible and bias any recency-based
-- analysis if included. The same approach that I used in
-- DQ-13 is applied here to exclude duplicates and
-- invalid join dates before the comparison.
SELECT COUNT(*) AS txn_before_join
FROM raw.activity a
JOIN (
    SELECT
        customer_id,
        MIN(TRY_CONVERT(DATE, join_date)) AS join_date
    FROM raw.customers
    WHERE TRY_CONVERT(DATE, join_date) IS NOT NULL
    GROUP BY customer_id
) c ON a.customer_id = c.customer_id
WHERE TRY_CONVERT(DATE, a.last_txn_date) < c.join_date;

-- [DQ-18] Churn date integrity check
-- Severity: Critical — validates the internal consistency
-- of the churn flag and churn date pairing. A churned
-- customer must have a churn date and a non-churned
-- customer must not. Both the positive confirmation and
-- the violation check are included so the result is
-- self-evidencing regardless of the outcome.
SELECT
    COUNT(CASE WHEN churned = '1' AND churn_date IS NOT NULL
               THEN 1 END) AS churned_with_date,
    COUNT(CASE WHEN churned = '0' AND churn_date IS NULL
               THEN 1 END) AS not_churned_without_date,
    COUNT(CASE WHEN churned = '1' AND churn_date IS NULL
               THEN 1 END) AS churned_missing_date,
    COUNT(CASE WHEN churned = '0' AND churn_date IS NOT NULL
               THEN 1 END) AS not_churned_has_date
FROM raw.activity;

-- [DQ-19] Credit score validity check
-- Severity: High — credit scores stored as decimal strings
-- by the source system (e.g. "518.0") cause a direct INT
-- conversion to return NULL for every value, without
-- indicating why, silently removing all credit score data
-- from the analysis. A two-step conversion — FLOAT first
-- to parse the decimal string, then INT — resolves this.
-- This pattern is applied consistently wherever credit_score
-- is cast throughout the project.
-- Values outside South Africa's valid scoring range of
-- 300-850 are also flagged here as invalid.
SELECT
    COUNT(CASE WHEN TRY_CONVERT(INT, TRY_CONVERT(FLOAT, credit_score)) < 300
               THEN 1 END) AS below_minimum,
    COUNT(CASE WHEN TRY_CONVERT(INT, TRY_CONVERT(FLOAT, credit_score)) > 850
               THEN 1 END) AS above_maximum
FROM raw.activity
WHERE TRY_CONVERT(FLOAT, credit_score) IS NOT NULL;