WITH ecommerce_orders AS (
  SELECT
  ID                                                                  AS order_id,
  explode(from_json(line_items, schema_of_json('["str"]')))           AS exploded_value,
  exploded_value:id                                                   AS line_item_id,
  exploded_value:sku                                                  AS SKU,
  exploded_value:quantity::int                                        AS quantity,
  exploded_value:price::float                                         AS price_per_item,
  current_total_tax::float                                            AS tax,
  total_shipping_price_set:presentment_money:amount::float            AS shipping_cost,
  currency
  FROM
    {{ref ('ecommerce_orders') }}
),

subscriptions_orders AS (
 SELECT
  id                                                                   AS line_item_id,
  invoice_id                                                           AS order_id,
  product_code                                                         AS SKU,
  quantity::int                                                        AS quantity,
  unit_amount::float                                                   AS price_per_item,
  tax::float                                                           AS tax,
  NULL                                                                 AS shipping_cost,
  currency
FROM
  {{ref ('subscriptions_adjustments') }}
),

sales_orders AS (
 SELECT
    line_item_id,
    order_id,
    SKU,
    quantity,
    price_per_item,
    tax,
    shipping_cost,
    currency
  FROM
    ecommerce_orders
 
  UNION ALL

  SELECT
    *
  FROM
    subscriptions_orders

)

SELECT
  *
FROM
  sales_orders
