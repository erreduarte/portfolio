WITH ecommerce_exploded AS (
    SELECT
        explode(from_json(variants, schema_of_json('["str"]')))                     AS exploded_value,
        exploded_value:sku                                                          AS sku,
        title                                                                       AS name,
        handle                                                                      AS product_description,
        created_at                                                                  AS created_at,
        'hardware'                                                                  AS type,
        'ecommerce'                                                                   AS store            
        -- run macro for OEM
    FROM
        {{ref ('ecommerce_products') }}      
),

subscriptions_product AS (
    SELECT
        product_code                                                                AS sku,
        created_at                                                                  AS created_at,
        NULL                                                                        AS name,
        description                                                                 AS product_description,
        CASE
            WHEN subscription_id is NOT NULL THEN 'subscription'
            WHEN subscription_id IS NULL THEN 'hardware'
            ELSE 'other'
        END                                                                         AS type,
        'subscriptions'                                                                   AS store
    FROM
        {{ref ('subscriptions_adjustments') }}
    WHERE
        --to remove refunds:
        product_code IS NOT NULL
    AND
        description NOT LIKE ANY ('%efund%', '%redit%')
),

combined_data AS (
    SELECT
        sku,
        created_at                                                                  AS created_at,
        name,
        product_description,
        type,
        store,
        ROW_NUMBER() OVER (PARTITION BY sku ORDER BY created_at)                    AS row_num
    FROM
        ecommerce_exploded

    UNION ALL

    SELECT
        sku,
        created_at                                                                  AS created_at,
        name,
        product_description,
        type,
        store,
        ROW_NUMBER() OVER (PARTITION BY sku ORDER BY created_at)                    AS row_num
    FROM
        subscriptions_product
),

sales_products as (
    SELECT
        sku,
        created_at,
        name,
        product_description,
        type,
        store
    FROM
        combined_data
    WHERE
        row_num = 1
)

SELECT * FROM sales_products
