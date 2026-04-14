-- ============================================================
-- Script 04: Exploratory Data Analysis
-- Project  : SA Retail Bank Customer Churn Analysis
-- Author   : Dominic Dandajena
-- Description:
--   Explores patterns, distributions, and relationships
--   across the three clean staging tables to surface
--   directional insights ahead of the Power BI build.
--
--   All queries run against staging tables — not raw.
--   Results here are analytically valid and directly
--   inform the report narrative and visualisation decisions
--   in Power BI.
--
--   A single temp table (#base) joining staging.customers
--   and staging.activity is created once at the top of
--   the script and referenced throughout. This avoids
--   repeating the join definition in every query and creates
--   a more reusable base dataset, as would be in a production
--   environment. staging.products is joined inline only
--   where product-level analysis requires it, keeping
--   the base dataset lean and performant.
--
--   Query-specific transformations — such as median
--   calculations using PERCENTILE_CONT — are handled
--   in dedicated CTEs within the relevant query rather
--   than in the base temp table, maintaining a clear
--   separation between reusable base data and
--   query-specific logic.
--
--   All findings in this script are directional and
--   exploratory. Language throughout is deliberately
--   hedged as these are hypotheses (my actual analysis
--   and recommendations will come post BI analysis).
--   The datase is synthetic and designed to illustrate 
--   patterns rather than represent a real bank's customer base.
--   Causal claims are not made on the basis of this
--   analysis alone.
--
--   A recurring theme is the competitive context — the
--   imminent entry of Revolut into the SA retail banking
--   market following its Section 12 application to the
--   SARB Prudential Authority in November 2025, alongside
--   the growth of domestic digital challengers including
--   Discovery Bank, TymeBank, and Bank Zero. 
--   Findings are interpreted through this lens where relevant,
--   with the caveat that the synthetic nature of the data
--   means any such interpretation is illustrative rather
--   than evidential.
--
-- Date Reference:
--   All date calculations use the dataset reference date
--   of 2024-12-31, representing present day within the
--   bank's timeline.
-- ============================================================

USE BankChurnAnalysis;
GO

-- ============================================================
-- BASE TEMP TABLE
-- Created once and referenced throughout the script.
-- Dropped and recreated at script start to ensure a clean
-- session state on each execution.
-- Joins staging.customers and staging.activity at the
-- customer level. staging.products is joined inline only
-- where product-level analysis requires it.
-- ============================================================

IF OBJECT_ID('tempdb..#base') IS NOT NULL
    DROP TABLE #base;

SELECT
    c.customer_id,
    c.province,
    c.gender,
    c.age,
    c.age_band,
    c.join_date,
    c.tenure_days,
    c.tenure_band,
    c.monthly_income,
    c.income_tier,
    c.account_status,
    c.is_valid_join_date,
    a.churned,
    a.churn_date,
    a.avg_monthly_balance,
    a.balance_tier,
    a.credit_score,
    a.credit_band,
    a.num_transactions_3m,
    a.is_active_member,
    a.last_txn_date,
    a.is_valid_last_txn,
    a.is_outlier_balance
INTO #base
FROM staging.customers c
JOIN staging.activity a ON c.customer_id = a.customer_id;

-- ============================================================
-- SECTION 1: CUSTOMER PROFILE & CHURN OVERVIEW

-- Establishes the baseline attrition picture and identifies
-- which customer demographics are most associated with
-- churn. Province and age are of particular interest given
-- the digital challenger hypothesis — younger, urban,
-- higher income customers may have more to gain from
-- switching to a Revolut-style alternative, though this
-- remains a hypothesis to be tested rather than assumed.
-- ============================================================

-- [EDA-01] Overall churn rate
-- The headline figure. Establishes the scale of the
-- attrition problem and anchors all subsequent analysis.
SELECT
    COUNT(*)                                            AS total_customers,
    SUM(CAST(churned AS INT))                          AS total_churned,
    COUNT(*) - SUM(CAST(churned AS INT))               AS total_retained,
    ROUND(
        CAST(SUM(CAST(churned AS INT)) AS FLOAT)
        / COUNT(*) * 100
    , 2)                                               AS churn_rate_pct
FROM #base;

-- [EDA-02] Churn rate by province
-- Identifies geographic patterns in attrition.
-- Gauteng and Western Cape are of particular interest
-- given their higher income profiles and greater exposure
-- to digital banking alternatives, though the relationship
-- between urbanisation and churn is not assumed — it is
-- tested here. The distinction between churn rate and
-- absolute churn volume is important — a low rate in a
-- large province may still represent a significant
-- number of lost customers.
SELECT
    province,
    COUNT(*)                                            AS total_customers,
    SUM(CAST(churned AS INT))                          AS churned,
    COUNT(*) - SUM(CAST(churned AS INT))               AS retained,
    ROUND(
        CAST(SUM(CAST(churned AS INT)) AS FLOAT)
        / COUNT(*) * 100
    , 2)                                               AS churn_rate_pct
FROM #base
GROUP BY province
ORDER BY churn_rate_pct DESC;

-- [EDA-03] Churn rate by gender
SELECT
    gender,
    COUNT(*)                                            AS total_customers,
    SUM(CAST(churned AS INT))                          AS churned,
    COUNT(*) - SUM(CAST(churned AS INT))               AS retained,
    ROUND(
        CAST(SUM(CAST(churned AS INT)) AS FLOAT)
        / COUNT(*) * 100
    , 2)                                               AS churn_rate_pct
FROM #base
GROUP BY gender
ORDER BY churn_rate_pct DESC;

-- [EDA-04] Churn rate by age band
-- Age is often cited as a proxy for digital adoption
-- propensity. This query tests whether younger customers
-- show higher churn rates, which would be consistent with
-- the digital challenger hypothesis. However the
-- relationship between age and churn is not assumed —
-- older customers may also be attracted to digital
-- banking for different reasons such as ease of use
-- and reduced branch dependency.
SELECT
    age_band,
    COUNT(*)                                            AS total_customers,
    SUM(CAST(churned AS INT))                          AS churned,
    COUNT(*) - SUM(CAST(churned AS INT))               AS retained,
    ROUND(
        CAST(SUM(CAST(churned AS INT)) AS FLOAT)
        / COUNT(*) * 100
    , 2)                                               AS churn_rate_pct
FROM #base
WHERE age_band != 'Unknown'
GROUP BY age_band
ORDER BY age_band;

-- [EDA-05] Account status distribution
-- Inactive accounts may indicate disengagement preceding
-- churn rather than confirmed exit. Understanding the
-- proportion of inactive customers provides context
-- for the overall churn figure and may indicate the
-- size of the at-risk pipeline above the churned segment.
SELECT
    account_status,
    COUNT(*)                                            AS total_customers,
    ROUND(
        CAST(COUNT(*) AS FLOAT)
        / SUM(COUNT(*)) OVER () * 100
    , 2)                                               AS pct_of_total
FROM #base
GROUP BY account_status
ORDER BY total_customers DESC;

-- [EDA-06] Customer acquisition trend by year
-- A declining acquisition trend alongside rising churn
-- would indicate net customer loss — a critical business
-- risk. Contextualised against the 2022-2024 churn window
-- (2022 is when the bank began measuring churn),
-- this may reveal whether attrition is accelerating
-- as digital competition intensifies.
SELECT
    YEAR(join_date)                                    AS join_year,
    COUNT(*)                                           AS new_customers
FROM #base
WHERE is_valid_join_date = 1
GROUP BY YEAR(join_date)
ORDER BY join_year;

-- [EDA-07] Income statistics by income tier and churn status
-- Applies statistical measures within each income tier
-- split by churn status to test whether churned customers
-- within the same earning bracket differ from retained ones.
-- If higher income tiers show elevated churn this may be
-- consistent with the digital challenger hypothesis —
-- higher earners may have more to gain from fee-efficient
-- alternatives. A separate medians CTE isolates
-- PERCENTILE_CONT from the GROUP BY aggregation to avoid
-- SQL Server returning identical values across all rows.
WITH medians AS (
    SELECT DISTINCT
        income_tier,
        churned,
        PERCENTILE_CONT(0.5) WITHIN GROUP
            (ORDER BY CAST(monthly_income AS FLOAT))
            OVER (PARTITION BY income_tier, churned)   AS median_income
    FROM #base
    WHERE monthly_income IS NOT NULL
    AND income_tier != 'Unknown'
)
SELECT
    b.income_tier,
    b.churned,
    COUNT(*)                                           AS customer_count,
    ROUND(MIN(CAST(b.monthly_income AS FLOAT)), 0)    AS min_income,
    ROUND(MAX(CAST(b.monthly_income AS FLOAT)), 0)    AS max_income,
    ROUND(AVG(CAST(b.monthly_income AS FLOAT)), 0)    AS avg_income,
    ROUND(STDEV(CAST(b.monthly_income AS FLOAT)), 0)  AS stdev_income,
    ROUND(m.median_income, 0)                          AS median_income
FROM #base b
JOIN medians m ON b.income_tier = m.income_tier
    AND b.churned = m.churned
WHERE b.monthly_income IS NOT NULL
AND b.income_tier != 'Unknown'
GROUP BY b.income_tier, b.churned, m.median_income
ORDER BY b.income_tier, b.churned;

-- [EDA-08] Churn rate by income tier
-- Surfaces which earning bracket is most associated with
-- attrition at a summary level, complementing the
-- statistical detail in EDA-07.
SELECT
    income_tier,
    COUNT(*)                                            AS total_customers,
    SUM(CAST(churned AS INT))                          AS churned,
    COUNT(*) - SUM(CAST(churned AS INT))               AS retained,
    ROUND(
        CAST(SUM(CAST(churned AS INT)) AS FLOAT)
        / COUNT(*) * 100
    , 2)                                               AS churn_rate_pct
FROM #base
WHERE income_tier != 'Unknown'
GROUP BY income_tier
ORDER BY income_tier;

-- ============================================================
-- SECTION 2: PRODUCT ANALYSIS

-- Product depth is largely seen as a retention anchor
-- in retail banking. This section tests that hypothesis
-- against the data — whether customers with more products
-- show lower churn rates — rather than assuming it holds.
-- The nature of products held may matter more than the
-- count alone, particularly in the context of digital
-- challengers who lead with transactional products.
-- ============================================================

-- [EDA-09] Product status distribution
-- Establishes the split between active and closed products.
-- A high proportion of closed products may indicate
-- disengagement preceding churn, though this relationship
-- is explored further in Power BI by cross-referencing
-- closed products with churned customers.
SELECT
    p.product_status,
    COUNT(*)                                            AS total_products,
    ROUND(
        CAST(COUNT(*) AS FLOAT)
        / SUM(COUNT(*)) OVER () * 100
    , 2)                                               AS pct_of_total
FROM #base b
JOIN staging.products p ON b.customer_id = p.customer_id
GROUP BY p.product_status
ORDER BY total_products DESC;

-- [EDA-10] Products per customer distribution
-- Shows how product holdings are distributed across
-- the customer base. Single product customers represent
-- the segment with the lowest switching cost and may
-- be most exposed to a compelling digital alternative.
SELECT
    product_count,
    COUNT(*)                                            AS customer_count,
    ROUND(
        CAST(COUNT(*) AS FLOAT)
        / SUM(COUNT(*)) OVER () * 100
    , 2)                                               AS pct_of_customers
FROM (
    SELECT
        b.customer_id,
        COUNT(p.product_name)                          AS product_count
    FROM #base b
    JOIN staging.products p ON b.customer_id = p.customer_id
    GROUP BY b.customer_id
) sub
GROUP BY product_count
ORDER BY product_count;

-- [EDA-11] Churn rate by number of products held
-- Tests whether product depth is inversely related to
-- churn risk. The results should be interpreted carefully
-- as small differences in churn rate across product count
-- bands may not be analytically meaningful and should
-- not be overstated as a finding.
SELECT
    product_count,
    COUNT(*)                                            AS total_customers,
    SUM(CAST(churned AS INT))                          AS churned,
    COUNT(*) - SUM(CAST(churned AS INT))               AS retained,
    ROUND(
        CAST(SUM(CAST(churned AS INT)) AS FLOAT)
        / COUNT(*) * 100
    , 2)                                               AS churn_rate_pct
FROM (
    SELECT
        b.customer_id,
        b.churned,
        COUNT(p.product_name)                          AS product_count
    FROM #base b
    JOIN staging.products p ON b.customer_id = p.customer_id
    GROUP BY b.customer_id, b.churned
) sub
GROUP BY product_count
ORDER BY product_count;

-- [EDA-12] Churn rate by product type
-- Identifies which product types are most associated
-- with churned customers. Customers holding only
-- transactional products may be more susceptible to
-- migration to a digital-only bank. Secured products
-- such as home loans may act as a structural retention
-- anchor, though this is tested here rather than assumed.
SELECT
    p.product_name,
    COUNT(DISTINCT b.customer_id)                      AS total_customers,
    COUNT(DISTINCT CASE WHEN b.churned = 1
        THEN b.customer_id END)                        AS churned_customers,
    ROUND(
        CAST(COUNT(DISTINCT CASE WHEN b.churned = 1
            THEN b.customer_id END) AS FLOAT)
        / COUNT(DISTINCT b.customer_id) * 100
    , 2)                                               AS churn_rate_pct
FROM #base b
JOIN staging.products p ON b.customer_id = p.customer_id
GROUP BY p.product_name
ORDER BY churn_rate_pct DESC;

-- ============================================================
-- SECTION 3: BEHAVIOURAL & FINANCIAL ANALYSIS
--
-- Behavioural signals — transaction frequency, engagement
-- status, and balance levels — may be early indicators
-- of churn intent. This section tests the strength of
-- those signals rather than assuming their predictive
-- value. Where relationships are found they are described
-- as associations rather than causal drivers.
-- ============================================================

-- [EDA-13] Balance statistics by balance tier and churn status
-- Applies statistical measures within each balance tier
-- split by churn status. The R4.2M whale customer will
-- inflate the mean for the Above R200K tier — the median
-- is therefore the more reliable central tendency measure
-- for that segment. A separate medians CTE isolates
-- PERCENTILE_CONT from the GROUP BY aggregation.
WITH medians AS (
    SELECT DISTINCT
        balance_tier,
        churned,
        PERCENTILE_CONT(0.5) WITHIN GROUP
            (ORDER BY avg_monthly_balance)
            OVER (PARTITION BY balance_tier, churned)  AS median_balance
    FROM #base
)
SELECT
    b.balance_tier,
    b.churned,
    COUNT(*)                                           AS customer_count,
    ROUND(MIN(b.avg_monthly_balance), 2)              AS min_balance,
    ROUND(MAX(b.avg_monthly_balance), 2)              AS max_balance,
    ROUND(AVG(b.avg_monthly_balance), 2)              AS avg_balance,
    ROUND(STDEV(b.avg_monthly_balance), 2)            AS stdev_balance,
    ROUND(m.median_balance, 2)                        AS median_balance
FROM #base b
JOIN medians m ON b.balance_tier = m.balance_tier
    AND b.churned = m.churned
GROUP BY b.balance_tier, b.churned, m.median_balance
ORDER BY b.balance_tier, b.churned;

-- [EDA-14] Churn rate by balance tier
-- Surfaces which balance tier is most associated with
-- attrition at a summary level, complementing the
-- statistical detail in EDA-13.
SELECT
    balance_tier,
    COUNT(*)                                            AS total_customers,
    SUM(CAST(churned AS INT))                          AS churned,
    COUNT(*) - SUM(CAST(churned AS INT))               AS retained,
    ROUND(
        CAST(SUM(CAST(churned AS INT)) AS FLOAT)
        / COUNT(*) * 100
    , 2)                                               AS churn_rate_pct
FROM #base
GROUP BY balance_tier
ORDER BY balance_tier;

-- [EDA-15] Credit score statistics by credit band
-- and churn status
-- Applies statistical measures within each credit band
-- split by churn status. Lower scoring customers may be
-- more financially stressed and therefore more sensitive
-- to banking costs, which may be consistent with higher
-- churn in lower bands — though this is tested here
-- rather than assumed. Same medians CTE pattern applied
-- as EDA-13.
WITH medians AS (
    SELECT DISTINCT
        credit_band,
        churned,
        PERCENTILE_CONT(0.5) WITHIN GROUP
            (ORDER BY CAST(credit_score AS FLOAT))
            OVER (PARTITION BY credit_band, churned)   AS median_score
    FROM #base
    WHERE credit_score IS NOT NULL
    AND credit_band != 'Unknown'
)
SELECT
    b.credit_band,
    b.churned,
    COUNT(*)                                           AS customer_count,
    ROUND(MIN(CAST(b.credit_score AS FLOAT)), 0)      AS min_score,
    ROUND(MAX(CAST(b.credit_score AS FLOAT)), 0)      AS max_score,
    ROUND(AVG(CAST(b.credit_score AS FLOAT)), 0)      AS avg_score,
    ROUND(STDEV(CAST(b.credit_score AS FLOAT)), 0)    AS stdev_score,
    ROUND(m.median_score, 0)                           AS median_score
FROM #base b
JOIN medians m ON b.credit_band = m.credit_band
    AND b.churned = m.churned
WHERE b.credit_score IS NOT NULL
AND b.credit_band != 'Unknown'
GROUP BY b.credit_band, b.churned, m.median_score
ORDER BY b.credit_band, b.churned;

-- [EDA-16] Churn rate by credit band
-- Surfaces which credit band is most associated with
-- attrition at a summary level, complementing the
-- statistical detail in EDA-15.
SELECT
    credit_band,
    COUNT(*)                                            AS total_customers,
    SUM(CAST(churned AS INT))                          AS churned,
    COUNT(*) - SUM(CAST(churned AS INT))               AS retained,
    ROUND(
        CAST(SUM(CAST(churned AS INT)) AS FLOAT)
        / COUNT(*) * 100
    , 2)                                               AS churn_rate_pct
FROM #base
WHERE credit_band != 'Unknown'
GROUP BY credit_band
ORDER BY credit_band;

-- [EDA-17] Active vs inactive member churn comparison
-- Tests whether inactivity is associated with higher
-- churn rates. Inactive members may have already begun
-- disengaging from the bank's products and services,
-- making them potentially more susceptible to a
-- compelling alternative — though the distinction between
-- causality and correlation between inactivity and churn 
-- cannot be determined from this data alone.
SELECT
    is_active_member,
    COUNT(*)                                            AS total_customers,
    SUM(CAST(churned AS INT))                          AS churned,
    COUNT(*) - SUM(CAST(churned AS INT))               AS retained,
    ROUND(
        CAST(SUM(CAST(churned AS INT)) AS FLOAT)
        / COUNT(*) * 100
    , 2)                                               AS churn_rate_pct
FROM #base
GROUP BY is_active_member
ORDER BY is_active_member;

-- [EDA-18] Transaction frequency and churn relationship
-- Tests whether lower transaction frequency is associated
-- with higher churn rates. Very low transaction counts
-- may indicate reduced primary banking usage, which
-- could be an early behavioural signal preceding churn.
-- The relationship is described as an association rather
-- than a causal driver.
SELECT
    txn_band,
    COUNT(*)                                            AS total_customers,
    SUM(CAST(churned AS INT))                          AS churned,
    COUNT(*) - SUM(CAST(churned AS INT))               AS retained,
    ROUND(
        CAST(SUM(CAST(churned AS INT)) AS FLOAT)
        / COUNT(*) * 100
    , 2)                                               AS churn_rate_pct
FROM (
    SELECT
        churned,
        CASE
            WHEN num_transactions_3m = 0  THEN 'No Activity'
            WHEN num_transactions_3m < 5  THEN '1-4'
            WHEN num_transactions_3m < 10 THEN '5-9'
            WHEN num_transactions_3m < 20 THEN '10-19'
            ELSE                               '20+'
        END AS txn_band
    FROM #base
    WHERE num_transactions_3m IS NOT NULL
) sub
GROUP BY txn_band
ORDER BY txn_band;

-- [EDA-19] Churn rate by tenure band
-- Tests whether newer customers show higher churn rates.
-- A higher rate among less than '1 year customers' may
-- indicate that the onboarding period is a critical
-- retention window. A slight uptick in the 2-5 year
-- band relative to 1-2 years is noted and warrants
-- further investigation in Power BI.
SELECT
    tenure_band,
    COUNT(*)                                            AS total_customers,
    SUM(CAST(churned AS INT))                          AS churned,
    COUNT(*) - SUM(CAST(churned AS INT))               AS retained,
    ROUND(
        CAST(SUM(CAST(churned AS INT)) AS FLOAT)
        / COUNT(*) * 100
    , 2)                                               AS churn_rate_pct
FROM #base
WHERE tenure_band != 'Unknown'
GROUP BY tenure_band
ORDER BY tenure_band;

-- [EDA-20] High value customers at churn risk
-- Identifies retained customers combining a high balance
-- with low engagement signals — inactive membership and
-- low transaction frequency. These customers may represent
-- a disproportionate revenue risk if they churn, given
-- the concentration of deposits in this segment.
-- The outlier balance flag excludes the R4.2M whale
-- to prevent a single record from distorting the
-- segment picture.
SELECT TOP 20
    customer_id,
    province,
    age,
    ROUND(avg_monthly_balance, 0)                      AS avg_balance,
    num_transactions_3m,
    is_active_member,
    credit_band,
    tenure_days,
    income_tier
FROM #base
WHERE churned = 0
AND avg_monthly_balance > 50000
AND is_active_member = 0
AND num_transactions_3m < 5
AND is_outlier_balance = 0
ORDER BY avg_monthly_balance DESC;

-- ============================================================
-- SECTION 4: TIME SERIES ANALYSIS

-- Time series analysis reveals whether churn is a stable
-- structural pattern or an accelerating trend across the
-- 2022-2024 analysis window. The window captures a period
-- during which digital banking competition in South Africa
-- intensified — TymeBank scaling rapidly, Discovery Bank
-- reaching profitability, and the broader market showing
-- signs of readiness for further digital disruption.
-- A rising churn trend across this window may be
-- consistent with competitive pressure as a contributing
-- factor, though this interpretation is illustrative
-- given the synthetic nature of the data.
-- ============================================================

-- [EDA-21] Monthly churn volume (2022-2024)
-- Granular monthly view to identify seasonal patterns
-- and directional trends across the full analysis window.
SELECT
    FORMAT(churn_date, 'yyyy-MM')                      AS churn_month,
    COUNT(*)                                           AS churned_customers
FROM #base
WHERE churn_date IS NOT NULL
GROUP BY FORMAT(churn_date, 'yyyy-MM')
ORDER BY churn_month;

-- [EDA-22] Quarterly churn volume
-- Aggregates to quarterly level to smooth monthly
-- variation and surface broader directional trends
-- across the analysis window.
SELECT
    YEAR(churn_date)                                   AS churn_year,
    DATEPART(QUARTER, churn_date)                      AS churn_quarter,
    COUNT(*)                                           AS churned_customers
FROM #base
WHERE churn_date IS NOT NULL
GROUP BY
    YEAR(churn_date),
    DATEPART(QUARTER, churn_date)
ORDER BY churn_year, churn_quarter;

-- [EDA-23] Annual churn volume with year on year change
-- Calculates the absolute and percentage change in churn
-- volume between years using the LAG window function.
-- An accelerating year on year increase may be consistent
-- with competitive disruption as a contributing factor,
-- though this interpretation requires caution given the
-- synthetic nature of the data.
WITH annual_churn AS (
    SELECT
        YEAR(churn_date)                               AS churn_year,
        COUNT(*)                                       AS churned_customers
    FROM #base
    WHERE churn_date IS NOT NULL
    GROUP BY YEAR(churn_date)
)
SELECT
    churn_year,
    churned_customers,
    LAG(churned_customers) OVER
        (ORDER BY churn_year)                          AS prior_year_churned,
    churned_customers
        - LAG(churned_customers) OVER
        (ORDER BY churn_year)                          AS yoy_change,
    ROUND(
        CAST(churned_customers
            - LAG(churned_customers) OVER
            (ORDER BY churn_year) AS FLOAT)
        / LAG(churned_customers) OVER
        (ORDER BY churn_year) * 100
    , 2)                                               AS yoy_change_pct
FROM annual_churn
ORDER BY churn_year;

-- [EDA-24] Churn by month of year
-- Aggregates across all years to identify recurring
-- seasonal patterns in attrition. Recurring monthly
-- spikes may indicate salary cycle effects, product
-- renewal periods, or external factors — further
-- investigation in Power BI is conducted for months
-- showing consistent elevation across years.
SELECT
    MONTH(churn_date)                                  AS churn_month_number,
    DATENAME(MONTH, churn_date)                        AS churn_month_name,
    COUNT(*)                                           AS churned_customers
FROM #base
WHERE churn_date IS NOT NULL
GROUP BY
    MONTH(churn_date),
    DATENAME(MONTH, churn_date)
ORDER BY churn_month_number;

-- ============================================================
-- SECTION 5: STATISTICAL DEPTH

-- This section extends the analysis beyond summary
-- statistics to examine distributional characteristics
-- that affect how findings should be interpreted.
-- Skew, concentration risk, and outlier sensitivity
-- are explored here to provide a more complete picture of
-- the data's statistical properties before the Power BI
-- build. These observations inform which measures and
-- aggregations are most appropriate for each dimension
-- in the report.
-- ============================================================

-- [EDA-25] Income and balance distribution skew
-- Compares mean vs median for income and balance to
-- assess distributional skew. Where mean significantly
-- exceeds median the distribution is positively-skewed —
-- a small number of high value customers pull the mean
-- above the typical customer income. In skewed
-- distributions the median is the more representative
-- central tendency measure and should be preferred in
-- executive reporting.
WITH skew_stats AS (
    SELECT DISTINCT
        PERCENTILE_CONT(0.5) WITHIN GROUP
            (ORDER BY CAST(monthly_income AS FLOAT))
            OVER ()                                    AS median_income,
        PERCENTILE_CONT(0.5) WITHIN GROUP
            (ORDER BY avg_monthly_balance)
            OVER ()                                    AS median_balance
    FROM #base
    WHERE monthly_income IS NOT NULL
)
SELECT
    ROUND(AVG(CAST(b.monthly_income AS FLOAT)), 0)    AS mean_income,
    ROUND(MAX(s.median_income), 0)                     AS median_income,
    ROUND(AVG(CAST(b.monthly_income AS FLOAT)), 0)
        - ROUND(MAX(s.median_income), 0)               AS income_mean_median_gap,
    ROUND(AVG(b.avg_monthly_balance), 2)               AS mean_balance,
    ROUND(MAX(s.median_balance), 2)                    AS median_balance,
    ROUND(AVG(b.avg_monthly_balance), 2)
        - ROUND(MAX(s.median_balance), 2)              AS balance_mean_median_gap
FROM #base b
CROSS JOIN skew_stats s
WHERE b.monthly_income IS NOT NULL;

-- [EDA-26] Balance concentration risk
-- Measures what proportion of total deposits are held
-- by the top 10% of customers by balance. High
-- concentration indicates that losing a small number
-- of high value customers would have a
-- disproportionate revenue impact — a critical risk
-- consideration for any retention strategy.
-- The outlier balance is excluded to prevent the
-- R4.2M whale from distorting the concentration picture.
WITH ranked AS (
    SELECT
        customer_id,
        avg_monthly_balance,
        NTILE(10) OVER
            (ORDER BY avg_monthly_balance DESC)        AS balance_decile
    FROM #base
    WHERE is_outlier_balance = 0
)
SELECT
    balance_decile,
    COUNT(*)                                           AS customer_count,
    ROUND(SUM(avg_monthly_balance), 0)                AS total_balance,
    ROUND(
        SUM(avg_monthly_balance)
        / SUM(SUM(avg_monthly_balance)) OVER () * 100
    , 2)                                               AS pct_of_total_balance
FROM ranked
GROUP BY balance_decile
ORDER BY balance_decile;

-- [EDA-27] Outlier sensitivity analysis
-- Tests how sensitive key aggregations are to the
-- inclusion or exclusion of extreme balance values.
-- Compares mean balance, churn rate, and customer
-- count with and without the top 1% of balances.
-- A large difference between the two sets of metrics
-- indicates high outlier sensitivity and suggests
-- the outlier flag slicer in Power BI will materially
-- affect report figures.
WITH percentiles AS (
    SELECT DISTINCT
        PERCENTILE_CONT(0.99) WITHIN GROUP
            (ORDER BY avg_monthly_balance)
            OVER ()                                    AS p99_balance
    FROM #base
)
SELECT
    'Including top 1%'                                 AS dataset,
    COUNT(*)                                           AS customer_count,
    ROUND(AVG(b.avg_monthly_balance), 2)               AS mean_balance,
    ROUND(
        CAST(SUM(CAST(b.churned AS INT)) AS FLOAT)
        / COUNT(*) * 100
    , 2)                                               AS churn_rate_pct
FROM #base b
UNION ALL
SELECT
    'Excluding top 1%',
    COUNT(*),
    ROUND(AVG(b.avg_monthly_balance), 2),
    ROUND(
        CAST(SUM(CAST(b.churned AS INT)) AS FLOAT)
        / COUNT(*) * 100
    , 2)
FROM #base b
CROSS JOIN percentiles p
WHERE b.avg_monthly_balance < p.p99_balance;