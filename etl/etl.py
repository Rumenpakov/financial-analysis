import sys
from argparse import ArgumentParser

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, lit, length
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# It's a best practice to define the schema of your raw data.
# This prevents errors from schema inference and makes your job more robust.
RAW_SCHEMA = StructType([
    StructField("articleId", StringType(), True),
    StructField("headline", StringType(), True),
    StructField("articleBody", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("source", StructType([
        StructField("name", StringType(), True)
    ]), True)
])

def process_news_data(spark: SparkSession, input_path: str, output_path: str):
    """
    Main ETL logic for processing raw news JSON data into a clean Parquet format.

    :param spark: The active SparkSession.
    :param input_path: The S3 path to the raw JSON data.
    :param output_path: The S3 path to write the processed Parquet data.
    """
    print(f"Starting ETL process for input path: {input_path}")

    # 1. EXTRACT: Read the raw JSON data from the input path using the predefined schema.
    # The 'multiline' option helps if a single JSON object spans multiple lines.
    raw_df = spark.read \
        .schema(RAW_SCHEMA) \
        .option("multiline", "true") \
        .json(input_path)

    print(f"Successfully read {raw_df.count()} raw records.")

    # 2. TRANSFORM: Clean, structure, and enrich the data.
    # This is where the main business logic resides.

    # Select columns, flatten the nested 'source' struct, and rename for clarity.
    transformed_df = raw_df.select(
        col("articleId").alias("id"),
        col("headline").alias("title"),
        col("articleBody").alias("content"),
        col("publishedAt"),
        col("source.name").alias("source_name")
    )

    # Data Quality and Enrichment
    final_df = transformed_df \
        .withColumn("processed_at", lit(to_timestamp(lit(None)))) \
        .withColumn("publish_timestamp", to_timestamp(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
        .withColumn("publish_date", to_date(col("publish_timestamp"))) \
        .filter(
            col("id").isNotNull() &
            col("content").isNotNull() &
            (length(col("content")) > 50)  # Filter out articles with very short content
        ) \
        .dropDuplicates(["id"]) \
        .drop("publishedAt") # Drop the original raw string timestamp

    print(f"Transformation complete. {final_df.count()} records ready for writing.")
    print("Final DataFrame schema:")
    final_df.printSchema()

    # 3. LOAD: Write the final, clean DataFrame to the output path.
    # Writing in 'overwrite' mode is common for daily batch jobs.
    # Partitioning by 'publish_date' drastically speeds up future queries that filter by date.
    final_df.write \
        .mode("overwrite") \
        .partitionBy("publish_date") \
        .parquet(output_path)

    print(f"Successfully wrote processed data to: {output_path}")


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--input-path", required=True, help="S3 path for raw input data")
    parser.add_argument("--output-path", required=True, help="S3 path for processed output data")
    args = parser.parse_args()

    # Create a SparkSession. When running on AWS Glue, the session is provided,
    # but this builder pattern works both locally and on Glue.
    spark_session = SparkSession.builder \
        .appName("FinancialNewsETL") \
        .getOrCreate()

    process_news_data(spark_session, args.input_path, args.output_path)

    spark_session.stop()