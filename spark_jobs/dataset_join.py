from typing import List

from config import dataset_schema
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, concat, lit, rand, when


def add_salt(df: DataFrame, skewed_keys: List[str]) -> DataFrame:
    """
    Adds a salt to skewed user_id keys for even distribution across partitions.

    Args:
        df (DataFrame): Input Spark DataFrame containing a 'user_id' column.
        skewed_keys (List[str]): List of skewed keys (user_id values) to which salt will be applied.

    Returns:
        DataFrame: A new DataFrame with an additional 'salted_user_id' column, where skewed keys are salted for better distribution.
    """
    salt_factor = 10  # Number of salts to spread the skewed keys

    # Generate the salt column
    salt_column = (rand() * salt_factor).cast(
        "int"
    )  # Random salt value for skewed keys

    # Add salted_user_id column
    return df.withColumn(
        "salted_user_id",
        when(
            col("user_id").isin(skewed_keys),  # Add salt only for skewed keys
            concat(col("user_id"), lit("_"), salt_column.cast("string")),
        ).otherwise(
            col("user_id")
        ),  # Keep the original user_id for non-skewed keys
    )


if __name__ == "__main__":
    # Initialize SparkSession
    spark = (
        SparkSession.builder.appName("JoinStrategy")
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .getOrCreate()
    )

    # Define file paths
    input_user_interactions_path = dataset_schema.output_user_interactions_path
    input_user_metadata_path = dataset_schema.output_user_metadata_path

    # Load datasets
    user_interactions = spark.read.parquet(input_user_interactions_path)
    user_metadata = spark.read.parquet(input_user_metadata_path)

    # Step 1: Detect skewed keys (e.g., top 10 most frequent user_ids)
    skewed_keys = (
        user_interactions.groupBy("user_id")
        .count()
        .orderBy(col("count").desc())
        .limit(10)
        .select("user_id")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    # Step 2: Add salt to skewed user_ids in both datasets
    salted_user_interactions = add_salt(user_interactions, skewed_keys)
    salted_user_metadata = add_salt(user_metadata, skewed_keys)

    result = salted_user_interactions.join(
        broadcast(salted_user_metadata),
        salted_user_interactions["salted_user_id"]
        == salted_user_metadata["salted_user_id"],
        "inner",
    ).drop("salted_user_id")

    result.show()

    spark.stop()
