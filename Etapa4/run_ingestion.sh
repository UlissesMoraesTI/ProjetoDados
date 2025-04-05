#!bin/bash
# This script is used to run the ingestion process for the data pipeline.

# command template 

template="python ingestion.py --source_database_url jdbc:sqlite:./../data/ecommerce.db"

$template --source_database_name main --source_table_name user --target_database_name ingestion --target_table_name user
$template --source_database_name main --source_table_name product --target_database_name ingestion --target_table_name product
$template --source_database_name main --source_table_name order --target_database_name ingestion --target_table_name order
$template --source_database_name main --source_table_name order_item --target_database_name ingestion --target_table_name order_item
