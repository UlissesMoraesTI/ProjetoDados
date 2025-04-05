CREATE EXTERNAL TABLE IF NOT EXISTS ingestion.product (
    id INT,
    name STRING,
    description STRING,
    price DOUBLE,
    category STRING,
    stock INT,
    creation_date TIMESTAMP
)
PARTITIONED BY (ingestion_datetime STRING)
STORED AS PARQUET
LOCATION './ingestion/product';
