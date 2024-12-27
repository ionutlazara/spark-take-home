from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, year, month, to_date
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from datetime import date, timedelta


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CalculateMetricsCsv") \
        .getOrCreate()

    # Define file paths
    input_user_interactions_path = "/opt/spark/dataset/user_interactions_sample.csv"

    schema_user_interactions = StructType([
        StructField("user_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("action_type", StringType()),
        StructField("page_id", StringType()),
        StructField("duration_ms", LongType()),
        StructField("app_version", StringType()),
    ])

    df = spark.read.csv(input_user_interactions_path, schema=schema_user_interactions, header=True)

    # Filter for the past year
    year_ago = date.today() - timedelta(days=365)
    df_filtered = df.filter(col("timestamp") >= year_ago)

    df_with_date = df_filtered.withColumn("date", to_date(col("timestamp")))

    # Extract the year and month from the timestamp
    df_with_month = df_with_date.withColumn("year", year(col("date"))).withColumn("month", month(col("date")))

    # Calculate Daily Active Users
    dau_df = df_with_date.groupBy("date").agg(countDistinct("user_id").alias("daily_active_users"))

    # Calculate Daily Active Users
    mau_df = df_with_month.groupBy("year", "month").agg(countDistinct("user_id").alias("monthly_active_users"))

    # Show results
    dau_df.orderBy("date").show()
    mau_df.orderBy("year", "month").show()

    # Stop the Spark session
    spark.stop()
