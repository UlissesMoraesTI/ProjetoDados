from pathlib import Path
from pyspark.sql import SparkSession

DDL_FOLDER = Path(__file__).parent / 'ddl'

spark: SparkSession = (
    SparkSession
    .builder
    .enableHiveSupport() # type: ignore
    .master('local[*]')
    .getOrCreate()
)

# Lista os arquivos .sql no diretório DDL_FOLDER
sql_files = DDL_FOLDER.glob('*.sql')

# Executa cada arquivo SQL
for sql_file in sql_files:
    with open(sql_file, 'r') as file:
        sql = file.read()
        print(f"Executing SQL from {sql_file}:")
        print(sql)
        spark.sql(sql)
        print(f"Executed SQL from {sql_file} successfully.")

