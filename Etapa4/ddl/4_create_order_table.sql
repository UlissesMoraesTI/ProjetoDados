CREATE EXTERNAL TABLE IF NOT EXISTS ingestion.`order` (
    id INT,
    user_id INT,
    order_date TIMESTAMP,
    status STRING,
    total_value DOUBLE,
    creation_date TIMESTAMP
)
PARTITIONED BY (ingestion_datetime STRING)
STORED AS PARQUET
LOCATION './ingestion/order';
