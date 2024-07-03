WITH ecommerce_exploded AS (
  SELECT
    order_id,
    explode(from_json(refund_line_items, schema_of_json('["str"]')))                AS exploded_value,
    exploded_value:line_item:sku                                                    AS SKU,
    created_at,
    exploded_value:quantity::int                                                    AS quantity
  FROM
    {{ref ('ecommerce_refunds') }}
),

ecommerce_returns AS (
  SELECT
    order_id,
    SKU,
    created_at,
    sum(quantity)                                                                   AS quantity
  FROM
    ecommerce_exploded
  GROUP BY
    order_id, sku, created_at
),

subscriptions_a AS (
  SELECT
    id                                                                              AS order_id,
    created_at,
    description,
    SUBSTRING(description, CHARINDEX('#', description) + 1, LEN(description))       AS original_invoice_number,
    ABS(quantity)                                                                   AS quantity
  FROM
    {{ref ('subscriptions_adjustments') }}
),

subscriptions_returns AS (
  SELECT
    a.order_id                                                                      AS order_id,
    b.line_items:data[0]:product_code                                               AS SKU,
    a.created_at                                                                    AS created_at,
    a.quantity                                                                      AS quantity
  FROM
    subscriptions_a a
  JOIN
    {{ref ('subscriptions_invoices') }} b on b.line_items:data[0]:invoice_number = a.original_invoice_number
),

sales_refunds AS (
  SELECT
    *
  FROM
    ecommerce_returns
  UNION ALL
  SELECT
    order_id, SKU, created_at, quantity
  FROM
    subscriptions_returns
)

SELECT
  *
FROM
  sales_refunds
