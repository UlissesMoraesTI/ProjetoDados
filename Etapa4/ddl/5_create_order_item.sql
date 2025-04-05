CREATE EXTERNAL TABLE IF NOT EXISTS ingestion.order_item (
    id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DOUBLE,
    creation_date TIMESTAMP
)
PARTITIONED BY (ingestion_datetime STRING)
STORED AS PARQUET
LOCATION './ingestion/order_item';
