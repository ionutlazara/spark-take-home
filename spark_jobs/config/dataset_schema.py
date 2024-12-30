from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DateType

# Define file paths
input_user_interactions_path = "/opt/spark/dataset/user_interactions_sample.csv"
output_user_interactions_path = "/opt/spark/dataset/output/user_interactions_sample.parquet"
input_user_metadata_path = "/opt/spark/dataset/user_metadata_sample.csv"
output_user_metadata_path = "/opt/spark/dataset/output/user_metadata_sample.parquet"

# Define dataset schemas
schema_user_interactions = StructType([
    StructField("user_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("action_type", StringType()),
    StructField("page_id", StringType()),
    StructField("duration_ms", LongType()),
    StructField("app_version", StringType()),
])

schema_user_metadata = StructType([
    StructField("user_id", StringType()),
    StructField("join_date", DateType()),
    StructField("country", StringType()),
    StructField("device_type", StringType()),
    StructField("subscription_type", StringType()),
])