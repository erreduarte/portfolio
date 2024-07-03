WITH ecommerce_orders AS (
    SELECT
        id                                                                           AS id,
        created_at                                                                   AS created_at,
        customer:default_address:customer_id                                         AS customer_id,
        'ecommerce'                                                                  AS store,
        source_name                                                                  AS sales_channel,
        from_json(fulfillments, schema_of_json('["str"]'))[0]:shipment_status        AS shipping_status,
        financial_status                                                             AS financial_status,
        if(fulfillment_status is null, 'unfulfilled', fulfillment_status)            AS fulfillment_status,
        total_price::float                                                           AS total_price,
        subtotal_price::float                                                        AS subtotal_price,
        total_tax::float                                                             AS taxes,
        total_discounts::float                                                       AS discounts,    
        total_shipping_price_set:presentment_money:amount::float                     AS shipping,
        currency                                                                     AS currency
    FROM
        {{ref ('ecommerce_orders') }}
),

subscriptions_orders AS (
  SELECT uuid                                                                        AS id,
         created_at                                                                  AS created_at,
         account_code                                                                AS account_code,
         'subscriptions'                                                             AS store,
         NULL                                                                        AS sales_channel,
         NULL                                                                        AS shipping_status,
         NULL                                                                        AS financial_status,
         NULL                                                                        AS fulfillment_status,
         NULL                                                                        AS total_price,
         subtotal                                                                    AS subtotal_price,
         NULL                                                                        AS taxes,
         NULL                                                                        AS discounts,
         NULL                                                                        AS shipping,
         NULL                                                                        AS currency
   FROM {{ref ('subscriptions_subscriptions') }}
),

sales_orders AS (
    SELECT * FROM ecommerce_orders
    UNION
    SELECT * FROM subscriptions_orders
)

SELECT * FROM sales_orders
