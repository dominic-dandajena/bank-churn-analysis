-- ============================================================
-- Script 05: Analytical Table Build
-- Project  : SA Retail Bank Customer Churn Analysis
-- Author   : Dominic Dandajena
-- Description:
--   Assembles the wide analytical table used by Power BI
--   from the three clean staging tables. This table is
--   purpose-built for the churn analysis report and is
--   optimised for DAX calculations and visual rendering.
--
--   NB! Architecture decision — single wide table vs star schema:
--   A star schema with separate fact and dimension tables
--   is the standard architecture for enterprise data
--   warehouses, offering greater flexibility, reusability
--   across multiple reports, and more efficient storage
--   at scale. For this project a single denormalised wide
--   table is used instead. This decision is deliberate
--   and based on three considerations specific to this
--   project's scope:
--
--   1. Performance — DAX calculations run faster against
--      a flat structure than across multiple table
--      relationships at this dataset size.
--   2. Simplicity — a single table reduces model
--      complexity and makes the report easier to
--      maintain for a single-purpose analytical use case.
--   3. Scope — this table serves one report. 

--   It is noted that a star schema
--   would be the correct choice in a production
--   environment serving multiple reports, teams, or
--   larger data volumes.
--
--   This decision would be revisited and reversed in a
--   production environment.
--
--   Product count is pre-aggregated at SQL level rather
--   than calculated in DAX to avoid repeated computation
--   on every visual render. Individual product flags are
--   included to enable product-level filtering without
--   requiring a join back to staging.products on every
--   visual.
--
--   Sort order for all band and tier columns is handled
--   in the Power BI layer via DAX calculated columns.
--   Numeric prefixes are deliberately excluded from SQL
--   to keep the transformation layer free of presentation
--   logic.
--
--   The outlier balance flag is included to allow the
--   R4.2M whale customer to be toggled via a slicer in
--   Power BI without affecting the underlying data.
--
-- Date Reference:
--   All date logic uses the dataset reference date of
--   2024-12-31, representing present day within the
--   bank's timeline.
-- ============================================================

USE BankChurnAnalysis;
GO

IF OBJECT_ID('staging.churn_analysis', 'U') IS NOT NULL
    DROP TABLE staging.churn_analysis;

-- Product count CTE
-- Pre-aggregates product holdings per customer from
-- staging.products. Calculated once here rather than
-- repeatedly in DAX to improve Power BI report performance.
-- Both total and active product counts are included —
-- active products represent current relationship anchors
-- while closed products may indicate prior disengagement.
WITH product_counts AS (
    SELECT
        customer_id,
        COUNT(*)                                       AS total_products,
        COUNT(CASE WHEN product_status = 'Active'
            THEN 1 END)                                AS active_products,
        COUNT(CASE WHEN product_status = 'Closed'
            THEN 1 END)                                AS closed_products,

        -- Individual product flags allow product-level
        -- filtering in Power BI without joining back
        -- to the products table on every visual render
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
    -- ── Customer identifiers ────────────────────────────
    c.customer_id,
    c.first_name,
    c.last_name,

    -- ── Demographics ────────────────────────────────────
    c.date_of_birth,
    c.age,
    c.age_band,
    c.gender,
    c.province,
    c.branch_code,

    -- ── Account attributes ──────────────────────────────
    c.join_date,
    c.is_valid_join_date,
    c.account_status,
    c.tenure_days,
    c.tenure_band,
    c.monthly_income,
    c.income_tier,

    -- ── Product attributes ──────────────────────────────
    p.total_products,
    p.active_products,
    p.closed_products,
    p.has_cheque_account,
    p.has_savings_account,
    p.has_home_loan,
    p.has_credit_card,
    p.has_personal_loan,

    -- ── Financial behaviour ─────────────────────────────
    a.avg_monthly_balance,
    a.balance_tier,
    a.is_outlier_balance,
    a.credit_score,
    a.credit_band,
    a.num_transactions_3m,
    a.is_active_member,
    a.last_txn_date,
    a.is_valid_last_txn,

    -- ── Churn attributes ────────────────────────────────
    -- churned is the primary measure field.
    -- churn_date enables time series analysis in Power BI.
    -- NULL churn_date indicates a retained customer —
    -- this is intentional, not missing data.
    a.churned,
    a.churn_date,

    -- ── Derived churn time fields ────────────────────────
    -- Pre-calculated to simplify time series grouping
    -- in Power BI. A Date table will be built in Power BI
    -- for DAX time intelligence functions — these fields
    -- provide an alternative grouping mechanism for
    -- churn-specific time series visuals.
    YEAR(a.churn_date)                                 AS churn_year,
    MONTH(a.churn_date)                                AS churn_month,
    FORMAT(a.churn_date, 'yyyy-MM')                    AS churn_year_month,
    DATENAME(MONTH, a.churn_date)                      AS churn_month_name,
    DATEPART(QUARTER, a.churn_date)                    AS churn_quarter

INTO staging.churn_analysis
FROM staging.customers c
JOIN staging.activity a
    ON c.customer_id = a.customer_id
LEFT JOIN product_counts p
    ON c.customer_id = p.customer_id;

-- ============================================================
-- VALIDATION
-- ============================================================

-- Row count — expected 8000
-- One row per customer, no fan-out from product join
-- because product_counts CTE pre-aggregates to
-- customer level before the join
SELECT COUNT(*) AS total_rows
FROM staging.churn_analysis;

-- Confirm no customer appears more than once
SELECT
    customer_id,
    COUNT(*) AS occurrence_count
FROM staging.churn_analysis
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Confirm churn flag distribution
SELECT
    churned,
    COUNT(*) AS customer_count
FROM staging.churn_analysis
GROUP BY churned
ORDER BY churned;

-- Confirm product count distribution
SELECT
    total_products,
    COUNT(*) AS customer_count
FROM staging.churn_analysis
GROUP BY total_products
ORDER BY total_products;

-- Confirm province distribution
-- Should show 9 clean values
SELECT
    province,
    COUNT(*) AS customer_count
FROM staging.churn_analysis
GROUP BY province
ORDER BY customer_count DESC;

-- Confirm churn date range
-- Expected: 2022-01-01 to 2024-12-31
SELECT
    MIN(churn_date) AS earliest_churn,
    MAX(churn_date) AS latest_churn
FROM staging.churn_analysis
WHERE churn_date IS NOT NULL;