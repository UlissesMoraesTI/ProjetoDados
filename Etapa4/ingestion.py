import argparse
from datetime import datetime
import logging as log
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class CommandLineArguments:
    def __init__(self, 
                 source_database_url: str, 
                 source_database_name: str, 
                 source_table_name: str, 
                 target_database_name: str,
                 target_table_name: str, 
    ) -> None:
        self.source_database_url = source_database_url
        self.source_database_name = source_database_name
        self.source_table_name = source_table_name
        self.target_database_name = target_database_name
        self.target_table_name = target_table_name

    @staticmethod
    def from_args() -> 'CommandLineArguments':
        parser = argparse.ArgumentParser(description='Ingest data from a source database to a target database.')
        parser.add_argument('--source_database_url', type=str, required=True, help='URL of the source database')
        parser.add_argument('--source_database_name', type=str, required=True, help='Name of the source database')
        parser.add_argument('--source_table_name', type=str, required=True, help='Name of the source table')
        parser.add_argument('--target_database_name', type=str, required=True, help='Name of the target database')
        parser.add_argument('--target_table_name', type=str, required=True, help='Name of the target table')

        args = parser.parse_args()
        return CommandLineArguments(
            source_database_url=args.source_database_url,
            source_database_name=args.source_database_name,
            source_table_name=args.source_table_name,
            target_database_name=args.target_database_name,
            target_table_name=args.target_table_name
        )

class Pipeline:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def get_last_ingested_datetime(self, target_database_name: str, target_table_name: str, partition_column: str) -> str:
        last_ingested_datetime: str | None = (
            self.spark
            .sql(f"SELECT MAX({partition_column}) FROM {target_database_name}.{target_table_name}")
            .collect()[0][0]
        )

        converted_ingested_datetime = (
            datetime.strptime(last_ingested_datetime, '%Y-%m-%d %H:%M:%S')
            if last_ingested_datetime
            else None
        )
        return converted_ingested_datetime.strftime('%Y-%m-%d %H:%M:%S') if converted_ingested_datetime else '1970-01-01 00:00:00'
    
    def create_sql_query(self, source_database_name: str, source_table_name: str, last_ingested_datetime: str) -> str:
        return f"""SELECT * FROM "{source_database_name}"."{source_table_name}" WHERE creation_date > '{last_ingested_datetime}'"""

    def read_source_data(self, sql: str, source_database_url: str) -> DataFrame:
        df = (
            self.spark
            .read
            .format("jdbc")
            .option('driver', 'org.sqlite.JDBC')
            .option("url", source_database_url)
            .option("query", sql)
            .load()
        )

        return df
    
    def create_partition_column(self, df: DataFrame, partition_column: str, datetime_: datetime) -> DataFrame:
        formatted_datetime = datetime_.strftime('%Y-%m-%d %H:%M:%S')
        df = df.withColumn(partition_column, F.lit(formatted_datetime))
        return df

    def equalize_schemas(self, df: DataFrame, target_database_name: str, target_table_name: str) -> DataFrame:
        target_df = self.spark.read.table(f"{target_database_name}.{target_table_name}")
        for column in target_df.columns:
            if column not in df.columns:
                df = df.withColumn(column, F.lit(None))

        for column in target_df.columns:
            df = df.withColumn(column, F.col(column).cast(target_df.schema[column].dataType))

        return df

    def write_data(self, df: DataFrame, target_database_name: str, target_table_name: str) -> None:
        df.write.insertInto(f"{target_database_name}.{target_table_name}", overwrite=True)

    def run(self, args: CommandLineArguments) -> None:
        # Get the last ingested datetime 
        current_datetime = datetime.now()
        log.info("starting data ingestion pipeline...")
        log.info("source database URL: %s", args.source_database_url)
        log.info("source database name: %s", args.source_database_name)
        log.info("source table name: %s", args.source_table_name)
        log.info("target database name: %s", args.target_database_name)
        log.info("target table name: %s", args.target_table_name)

        log.info("getting last ingested datetime...")
        last_ingested_datetime = self.get_last_ingested_datetime(
            args.target_database_name, 
            args.target_table_name, 
            'ingestion_datetime'
        )
        
        log.info(f"last ingested datetime: {last_ingested_datetime}")

        # Create the SQL query to read the source data
        log.info("creating SQL query...")
        sql = self.create_sql_query(args.source_database_name, args.source_table_name, last_ingested_datetime)

        log.info(f"SQL query: {sql}")

        # Read the source data
        log.info("reading source data...")
        df = self.read_source_data(sql, args.source_database_url)

        count = df.count()
        log.info(f"source data count: {count}")

        if count == 0:
            log.info("no new data to ingest")
            return
        
        # Create the partition column
        log.info('creating partition column with current datetime: %s', current_datetime)
        df = self.create_partition_column(df, 'ingestion_datetime', current_datetime)
        
        # Equalize the schemas
        df = self.equalize_schemas(df, args.target_database_name, args.target_table_name)

        # Write the data to the target table
        log.info("writing data to target table...")
        self.write_data(df, args.target_database_name, args.target_table_name)

        log.info("data written successfully")

def main():
    # Initialize logging
    log.basicConfig(
        level=log.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename='./pipeline.log',
    )

    # Parse command line arguments
    args = CommandLineArguments.from_args()

    # Create a Spark session
    spark = (
        SparkSession
        .builder
        .enableHiveSupport() # type: ignore
        .appName('Data Ingestion Pipeline')
        .config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.34.0') # type: ignore
        .master('local[*]')
        .getOrCreate()
    )

    # Create a pipeline instance and run it
    pipeline = Pipeline(spark)
    pipeline.run(args)

if __name__ == '__main__':
    main()