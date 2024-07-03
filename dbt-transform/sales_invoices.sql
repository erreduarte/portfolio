WITH ecommerce_payments AS (
  SELECT
    id                                                  AS transaction_id,
    receipt:id                                          AS invoice_id
  FROM
    {{ref ('ecommerce_transactions') }}  
),

subscriptions_payments AS (
  SELECT
    get_json_object(transactions, '$[0].id')            AS transaction_id,
    id                                                  AS invoice_id
  FROM
    {{ref ('subscriptions_invoices') }}
  WHERE
    get_json_object(transactions, '$[0].id') IS NOT NULL
),

sales_payment_invoices AS (
  SELECT * FROM ecommerce_payments
  UNION
  SELECT * FROM subscriptions_payments
  )

SELECT * FROM sales_payment_invoices

