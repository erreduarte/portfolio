WITH ecommerce_orders AS (
  SELECT
    id                                                                                        AS id, 
    'ecommerce'                                                                               AS store,
    email                                                                                     AS email,
    get_json_object(addresses, '$[0].zip')                                                    AS zip_code,
    get_json_object(addresses, '$[0].address1')                                               AS street,
    get_json_object(addresses, '$[0].city')                                                   AS city,
    get_json_object(addresses, '$[0].country')                                                AS country
  FROM
    {{ref ('ecommerce_customers') }}
),

subscriptions_orders AS (
  SELECT
    a.code                                                                                    AS id, 
    'subscriptions'                                                                           AS store,
    a.email                                                                                   AS email,
    b.address:postal_code                                                                     AS zip_code,
    b.address:street1                                                                         AS street,
    b.address:city                                                                            AS city,
    b.address:country                                                                         AS country
  FROM
    {{ref ('subscriptions_accounts') }} a
  JOIN
    {{ref ('subscriptions_invoices') }} b ON a.code = b.account:code
  GROUP BY
    1, 3, 4, 5, 6, 7
),

sales_customers AS (
  SELECT
    id,
    store,
    email,
    zip_code,
    street,
    city,
    country
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
  sales_customers
