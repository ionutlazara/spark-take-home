import pytest
from unittest import mock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


from spark_jobs.dataset_parquet import parquet_write


@pytest.fixture
def mock_spark():
    # Mock Spark session
    spark_mock = mock.MagicMock(spec=SparkSession)
    return spark_mock


@pytest.fixture
def mock_df():
    # Mock DataFrame
    df_mock = mock.MagicMock()
    return df_mock


@pytest.fixture
def mock_schema():
    # Define a simple schema for testing
    return StructType([StructField("name", StringType(), True)])


def test_parquet_write(mock_spark, mock_df, mock_schema):
    input_path = "input.csv"
    output_path = "output.parquet"

    # Mock spark.read.csv to return a mocked DataFrame
    mock_spark.read.csv.return_value = mock_df

    # Call the function with mocked Spark session, DataFrame, and schema
    with mock.patch('spark_jobs.dataset_parquet.spark', mock_spark):
        parquet_write(input_path, output_path, mock_schema)

    # Check that the spark.read.csv method was called with correct parameters
    mock_spark.read.csv.assert_called_once_with(input_path, schema=mock_schema, header=True)

    # Check that the DataFrame's write method was called with the expected arguments
    mock_df.write.mode.assert_called_once_with("overwrite")
    mock_df.write.mode().parquet.assert_called_once_with(output_path, compression="snappy")
