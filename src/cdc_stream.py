
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


BRONZE = "dev.customer_cdc.bronze_customers"
STATE = "dev.customer_cdc.customer_state_v2"

CHECKPOINT = (
    "/Volumes/dev/customer_cdc/pipeline_state/"
    "cdc_v2_checkpoint"
)


def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    print(f"Processing CDC batch: {batch_id}")

    # 1. Clean and normalize incoming records
    clean_df = (
        batch_df
        .withColumn(
            "customer_id",
            F.expr("try_cast(customer_id AS INT)")
        )
        .withColumn("name", F.initcap(F.trim("name")))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("tier", F.initcap(F.trim("tier")))
        .withColumn("status", F.initcap(F.trim("status")))
        .withColumn("operation", F.upper(F.trim("operation")))
        .withColumn(
            "updated_at",
            F.expr(
                "try_to_timestamp("
                "updated_at, 'yyyy-MM-dd HH:mm:ss')"
            )
        )
    )

    # 2. Check business rules
    valid_condition = (
        F.col("customer_id").isNotNull()
        & (F.col("customer_id") > 0)
        & F.col("updated_at").isNotNull()
        & F.col("operation").isin("I", "U", "D")
        & (
            (F.col("operation") == "D")
            | (
                F.col("name").isNotNull()
                & (F.col("name") != "")
                & F.col("email").rlike(
                    r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
                )
                & F.col("tier").isin(
                    "Bronze", "Silver", "Gold", "Platinum"
                )
                & F.col("status").isin("Active", "Inactive")
            )
        )
    )

    invalid_count = clean_df.filter(
        ~F.coalesce(valid_condition, F.lit(False))
    ).count()

    if invalid_count:
        raise ValueError(
            f"Batch {batch_id}: "
            f"{invalid_count} invalid CDC records"
        )

    # 3. Keep the latest event per customer
    # Delete wins if timestamps are identical.
    window_spec = (
        Window.partitionBy("customer_id")
        .orderBy(
            F.col("updated_at").desc(),
            F.when(F.col("operation") == "D", 1)
             .otherwise(0).desc(),
            F.col("ingestion_time").desc(),
            F.col("source_file").desc()
        )
    )

    latest_df = (
        clean_df
        .withColumn(
            "rn",
            F.row_number().over(window_spec)
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn(
            "is_deleted",
            F.col("operation") == "D"
        )
    )

    # 4. Merge into persistent customer state
    target = DeltaTable.forName(
        batch_df.sparkSession,
        STATE
    )

    values = {
        "customer_id": "s.customer_id",
        "name": "s.name",
        "email": "s.email",
        "tier": "s.tier",
        "status": "s.status",
        "updated_at": "s.updated_at",
        "operation": "s.operation",
        "ingestion_time": "s.ingestion_time",
        "source_file": "s.source_file",
        "is_deleted": "s.is_deleted"
    }

    (
        target.alias("t")
        .merge(
            latest_df.alias("s"),
            "t.customer_id = s.customer_id"
        )
        .whenMatchedUpdate(
            condition="""
                s.updated_at > t.updated_at
                OR (
                    s.updated_at = t.updated_at
                    AND s.is_deleted = true
                    AND t.is_deleted = false
                )
            """,
            set=values
        )
        .whenNotMatchedInsert(
            values=values
        )
        .execute()
    )

    print(f"Completed CDC batch: {batch_id}")


def run_cdc_stream():

    spark = SparkSession.builder.getOrCreate()

    # Append-only Bronze source
    stream_df = (
        spark.readStream
        .table(BRONZE)
    )

    query = (
        stream_df.writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            CHECKPOINT
        )
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    print("Incremental CDC processing completed")


if __name__ == "__main__":
    run_cdc_stream()
