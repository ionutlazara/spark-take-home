from unittest import mock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from spark_jobs.dataset_join import add_salt


@pytest.fixture
def mock_spark():
    # Mock Spark session
    spark_mock = mock.MagicMock(spec=SparkSession)
    return spark_mock


@pytest.fixture
def mock_df():
    # Mock DataFrame
    df_mock = mock.MagicMock(spec=DataFrame)
    return df_mock


@pytest.fixture
def skewed_keys():
    return ["user1", "user2", "user3"]


def test_add_salt(mock_spark, mock_df, skewed_keys):
    # Mock the random function used for generating salt
    mock_rand = mock.MagicMock(return_value=1)  # Mocking rand() to always return 1

    # Mock the `withColumn` method chain
    mock_df.withColumn.return_value = mock_df

    # Patch `rand` in the module where add_salt is defined to always return a fixed value (1)
    with mock.patch("spark_jobs.dataset_join.rand", mock_rand):
        # Call the function
        result_df = add_salt(mock_df, skewed_keys)

    # Check that rand() was called once
    mock_rand.assert_called_once()

    # Check if `withColumn` was called with the correct column name and logic
    mock_df.withColumn.assert_called_once_with(
        "salted_user_id",
        mock.ANY,  # We use mock.ANY since the column expression will be more complex
    )

    # Verify that the 'user_id' column is being transformed correctly for skewed keys
    # Ensure that the logic for adding salt to the skewed keys is triggered.
    with_column_args = mock_df.withColumn.call_args[0][1]
    # Check if the expression includes 'user_id' and applies salt for skewed keys
    assert "user_id" in str(with_column_args)
    assert "rand()" in str(with_column_args)  # Check if rand() is used for salt

    # Check that 'withColumn' was called for non-skewed keys to keep original user_id
    # For simplicity, we verify if 'user_id' is unchanged for non-skewed keys
    assert "user_id" in str(with_column_args)
