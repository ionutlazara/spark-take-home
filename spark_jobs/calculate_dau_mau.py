from datetime import date, timedelta

from config import dataset_schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, month, to_date, year

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("CalculateMetricsCsv").getOrCreate()

    # Define file paths
    input_user_interactions_path = dataset_schema.input_user_interactions_path
    schema_user_interactions = dataset_schema.schema_user_interactions

    df = spark.read.csv(
        input_user_interactions_path, schema=schema_user_interactions, header=True
    )

    # Filter for the past year
    year_ago = date.today() - timedelta(days=365)
    df_filtered = df.filter(col("timestamp") >= year_ago)

    # Extract date, year, month from timestamp
    df_with_date = (
        df_filtered.withColumn("date", to_date(col("timestamp")))
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
    )

    # Calculate Daily Active Users
    dau_df = df_with_date.groupBy("date").agg(
        countDistinct("user_id").alias("daily_active_users")
    )

    # Calculate Daily Active Users
    mau_df = df_with_date.groupBy("year", "month").agg(
        countDistinct("user_id").alias("monthly_active_users")
    )

    # Show results
    dau_df.orderBy("date").show()
    mau_df.orderBy("year", "month").show()

    # Stop the Spark session
    spark.stop()
