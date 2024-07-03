WITH ecommerce_transactions AS (
    SELECT
        p.id,
        q.customer:default_address:customer_id                                          AS customer_id,
        p.created_at                                                                    AS created_at,
        case
          when p.gateway = '' THEN 'subscriptions_payments'
          when p.gateway is null THEN 'subscriptions_payments'
          else p.gateway
        END                                                                             AS payment_gateway,
        receipt:charges:data[0]:payment_method_details:type                             AS payment_method,
        receipt:id                                                                      AS payment_gateway_transaction_id,
        if(kind = 'refund', amount*-1::float, amount::float)                            AS amount,
        p.currency                                                                      AS currency,
        p.status                                                                        AS status,
        p.kind                                                                          AS type,
        receipt:metadata:order_transaction_id                                           AS reference_transaction_id,
        p.order_id                                                                      AS order_id
    FROM
        {{ref ('ecommerce_transactions') }} p
        JOIN {{ref ('ecommerce_orders') }} q ON p.order_id = q.id
),

subscriptions_transactions AS (
    SELECT
        id                                                                              AS ID,
        account:code                                                                    AS customer_id,
        created_at                                                                      AS created_at,
        NULL                                                                            AS payment_gateway,
        CASE
        WHEN payment_method:object = 'credit_card' THEN 'card'
        END                                                                             AS payment_method,
        payment_gateway:id                                                              AS payment_gateway_transaction_id,
        if(type = 'refund', amount*-1::float, amount::float)                            AS amount,
        currency                                                                        AS currency,
        status,
        type,
        original_transaction_id                                                         AS reference_transaction_id,
        from_json(subscription_ids, schema_of_json('["str"]'))[0]                       AS order_id
    FROM
        {{ref ('subscriptions_transactions') }}
),
sales_transactions AS (
    SELECT
        *
    FROM
        ecommerce_transactions
    UNION ALL
    SELECT
        *
    FROM
        subscriptions_transactions
)

SELECT * FROM sales_transactions
