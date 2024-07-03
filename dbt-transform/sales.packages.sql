WITH ecommerce_line_items AS (
  SELECT
    id                                                                                          AS order_id,
    note_attributes,
    explode(from_json(line_items, schema_of_json('["str"]')))                                   AS exploded_line_item,
    exploded_line_item:sku                                                                      AS SKU,
    exploded_line_item:id                                                                       AS line_item_id
  FROM
    {{ref ('ecommerce_orders') }}
),

serial_exploded AS (
  SELECT order_id,
         line_item_id,
         sku,
         explode(from_json(note_attributes, schema_of_json('["str"]')))                         AS exploded_note
  FROM
    ecommerce_line_items
),

serial_filtered AS (
  SELECT order_id,
         line_item_id,
         exploded_note:value                                                                    AS serial_number,
         if(split(exploded_note:name, ':')[0] rlike '\\d{13,}',
            sku,'unknown')                                                                      AS updated_sku,
         if(split(exploded_note:name, ':')[0] rlike '\\d{13,}',
            split(exploded_note:name, ':')[0],
            line_item_id)                                                                       AS note_line_item_id
  FROM
    serial_exploded
  WHERE
    exploded_note:name like any ('%serial%','%camera_sn%')
),

table_merged as (
  SELECT
    order_id,
    coalesce(updated_sku) as sku,
    serial_number
FROM
    serial_filtered a
WHERE
    line_item_id = note_line_item_id
),

sales_packages as (
  SELECT
    a.order_id                                                                                   AS order_id,
    a.sku                                                                                        AS SKU,
    coalesce(b.camera_serial, a.serial_number)                                                   AS serial_number
  FROM
    table_merged a
  LEFT JOIN
    {{ref ('pkg_table') }} b on a.serial_number = b.sleeve_serial
  GROUP BY ALL
)

  SELECT * FROM sales_packages
