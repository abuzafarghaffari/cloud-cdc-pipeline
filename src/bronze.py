from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp,col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
# 1. Configure source, target and checkpoint paths
SOURCE_PATH = "/Volumes/dev/customer_cdc/landing/incoming/"

SCHEMA_PATH = (
    "/Volumes/dev/customer_cdc/pipeline_state/bronze_schema/"
)

CHECKPOINT_PATH = (
    "/Volumes/dev/customer_cdc/pipeline_state/bronze_checkpoint/"
)
BRONZE_TABLE = "dev.customer_cdc.bronze_customers"

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("operation", StringType(), True)
])

def run_bronze_ingestion():
    spark = SparkSession.builder.getOrCreate()
    print("Starting Bronze ingestion...")

    bronze_df =(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.schemaLocation",SCHEMA_PATH)
        .schema(customer_schema)
        .load(SOURCE_PATH)

    )
    bronze_df =(
        bronze_df.withColumn(
             "ingestion_time",
            current_timestamp()
        )
        .withColumn(
             "source_file",
            col("_metadata.file_path")
        )
    )

    query =(
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
        )
    query.awaitTermination()

    print("Bronze ingestion completed successfully!")


if __name__ == "__main__":
    run_bronze_ingestion()
