
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    initcap,
    lower,
    upper,
    to_timestamp,
    row_number,
    when
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable


BRONZE_TABLE = "dev.customer_cdc.bronze_customers"
SILVER_TABLE = "dev.customer_cdc.silver_customers"

CDC_FILENAME = "customers_batch_2_cdc.csv"


def run_cdc_merge():

    spark = SparkSession.builder.getOrCreate()

    print("Starting CDC MERGE...")

    # Step 1: Read events from the CDC file in Bronze
    events_df = (
        spark.table(BRONZE_TABLE)
        .filter(
            col("source_file").endswith(CDC_FILENAME)
        )
    )

    # Step 2: Clean source values
    cleaned_df = (
        events_df
        .withColumn(
            "customer_id",
            col("customer_id").cast("int")
        )
        .withColumn(
            "name",
            initcap(trim(col("name")))
        )
        .withColumn(
            "email",
            lower(trim(col("email")))
        )
        .withColumn(
            "tier",
            initcap(trim(col("tier")))
        )
        .withColumn(
            "status",
            initcap(trim(col("status")))
        )
        .withColumn(
            "operation",
            upper(trim(col("operation")))
        )
        .withColumn(
            "updated_at",
            to_timestamp(
                col("updated_at"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
    )

    # Step 3: Validate CDC records before merging
    invalid_df = cleaned_df.filter(
        col("customer_id").isNull()
        | (col("customer_id") <= 0)
        | col("name").isNull()
        | (col("name") == "")
        | col("email").isNull()
        | (~col("email").rlike(
            r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
        ))
        | col("updated_at").isNull()
        | col("tier").isNull()
        | (~col("tier").isin(
            "Bronze", "Silver", "Gold", "Platinum"
        ))
        | col("status").isNull()
        | (~col("status").isin("Active", "Inactive"))
        | col("operation").isNull()
        | (~col("operation").isin("I", "U", "D"))
    )

    invalid_count = invalid_df.count()

    if invalid_count > 0:
        raise ValueError(
            f"CDC validation failed: {invalid_count} invalid rows"
        )

    # Step 4: Keep the newest event for each customer
    window_spec = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            col("updated_at").desc(),
            col("ingestion_time").desc()
        )
    )

    updates_df = (
        cleaned_df
        .withColumn(
            "row_num",
            row_number().over(window_spec)
        )
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    print("Validated CDC records:", updates_df.count())

    # Step 5: Reference the existing Delta Silver table
    silver_table = DeltaTable.forName(
        spark,
        SILVER_TABLE
    )

    # Step 6: Execute INSERT, UPDATE and DELETE
    (
        silver_table.alias("target")
        .merge(
            updates_df.alias("source"),
            "target.customer_id = source.customer_id"
        )

        # Delete existing customer if delete event is newer
        .whenMatchedDelete(
            condition="""
                source.operation = 'D'
                AND source.updated_at > target.updated_at
            """
        )

        # Update existing customer if event is newer
        .whenMatchedUpdate(
            condition="""
                source.operation IN ('I', 'U')
                AND source.updated_at > target.updated_at
            """,
            set={
                "name": "source.name",
                "email": "source.email",
                "tier": "source.tier",
                "status": "source.status",
                "updated_at": "source.updated_at",
                "operation": "source.operation",
                "ingestion_time": "source.ingestion_time",
                "source_file": "source.source_file"
            }
        )

        # Insert customers not present in Silver
        .whenNotMatchedInsert(
            condition="source.operation IN ('I', 'U')",
            values={
                "customer_id": "source.customer_id",
                "name": "source.name",
                "email": "source.email",
                "tier": "source.tier",
                "status": "source.status",
                "updated_at": "source.updated_at",
                "operation": "source.operation",
                "ingestion_time": "source.ingestion_time",
                "source_file": "source.source_file"
            }
        )
        .execute()
    )

    print("CDC MERGE completed successfully!")


if __name__ == "__main__":
    run_cdc_merge()
