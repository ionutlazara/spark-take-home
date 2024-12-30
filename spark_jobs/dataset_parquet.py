import argparse

from config import dataset_schema
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def parquet_write(input_path: str, output_path: str, schema: StructType) -> None:
    """
    Reads a CSV file from the specified input path, converts it into a DataFrame
    using the given schema, and writes it to a Parquet file at the output path.

    Args:
        input_path (str): The path to the input CSV file.
        output_path (str): The path where the Parquet file should be written.
        schema (StructType): The schema to apply when reading the CSV file.

    Returns:
        None
    """
    # Read csv into dataframe
    df = spark.read.csv(input_path, schema=schema, header=True)

    # Write to parquet
    df.write.mode("overwrite").parquet(output_path, compression="snappy")


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("ParquetDataset").getOrCreate()

    parser = argparse.ArgumentParser()
    parser.add_argument("--datasets", nargs="+", help="List of dataset names to process.")

    args = parser.parse_args()
    datasets = args.datasets

    for dataset in datasets:
        input_path = getattr(dataset_schema, f"input_{dataset}_path")
        output_path = getattr(dataset_schema, f"output_{dataset}_path")
        schema = getattr(dataset_schema, f"schema_{dataset}")

        parquet_write(input_path=input_path, output_path=output_path, schema=schema)

    spark.stop()
