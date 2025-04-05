CREATE EXTERNAL TABLE IF NOT EXISTS ingestion.user (
    id INT,
    name STRING,
    email STRING,
    phone STRING,
    birth_date DATE,
    creation_date TIMESTAMP
)
PARTITIONED BY (ingestion_datetime STRING)
STORED AS PARQUET
LOCATION './ingestion/user';
