from config import dataset_schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, rand, when


def add_salt(df, skewed_keys):
    """Adds a salt to skewed user_id keys for even distribution."""
    salt_factor = 10  # Number of salts to spread the skewed keys

    # Generate the salt column
    salt_column = (rand(42) * salt_factor).cast(
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
    spark = (
        SparkSession.builder.appName("JoinStrategyOptimized")
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .config("spark.sql.join.preferSortMergeJoin", "true")
        .config("spark.sql.bloomFilterIndex.enabled", "true")
        .getOrCreate()
    )

    # Define file paths
    input_user_interactions_path = dataset_schema.output_user_interactions_path
    input_user_metadata_path = dataset_schema.output_user_metadata_path

    user_interactions = spark.read.parquet(input_user_interactions_path)
    user_metadata = spark.read.parquet(input_user_metadata_path)

    skew_analysis = (
        user_interactions.groupBy("user_id")
        .count()
        .orderBy(col("count").desc())
        .limit(10)
    )
    total_count = user_interactions.count()
    skew_threshold = 0.05 * total_count
    skewed_keys = (
        skew_analysis.filter(col("count") > skew_threshold)
        .select("user_id")
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    salted_user_interactions = add_salt(user_interactions, skewed_keys)
    salted_user_metadata = add_salt(user_metadata, skewed_keys)

    # Repartition datasets
    repartitioned_user_interactions = salted_user_interactions.repartition(
        75, col("salted_user_id")
    )
    repartitioned_user_metadata = salted_user_metadata.repartition(
        75, col("salted_user_id")
    )

    result = repartitioned_user_interactions.join(
        repartitioned_user_metadata,
        repartitioned_user_interactions["salted_user_id"]
        == repartitioned_user_metadata["salted_user_id"],
        "inner",
    ).drop("salted_user_id")

    spark.stop()
