-- ============================================================================
-- Snowflake DML — Upsert (MERGE) Logic for Dimension Tables
--
-- These MERGE statements handle both INSERT and UPDATE to ensure
-- dimension tables stay current without creating duplicates.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- UPSERT: dim_user
-- Source: staging table or DataFrame loaded via Snowflake connector
-- ---------------------------------------------------------------------------
MERGE INTO dim_user AS target
USING (
    SELECT
        user_id,
        user_name,
        email,
        signup_date
    FROM dim_user_staging
) AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN
    UPDATE SET
        target.user_name   = source.user_name,
        target.email       = source.email,
        target.signup_date = source.signup_date
WHEN NOT MATCHED THEN
    INSERT (user_id, user_name, email, signup_date)
    VALUES (source.user_id, source.user_name, source.email, source.signup_date);


-- ---------------------------------------------------------------------------
-- UPSERT: dim_product
-- Source: staging table or DataFrame loaded via Snowflake connector
-- ---------------------------------------------------------------------------
MERGE INTO dim_product AS target
USING (
    SELECT
        product_id,
        product_name,
        category,
        price
    FROM dim_product_staging
) AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN
    UPDATE SET
        target.product_name = source.product_name,
        target.category     = source.category,
        target.price        = source.price
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, category, price)
    VALUES (source.product_id, source.product_name, source.category, source.price);
