
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    initcap,
    lower,
    upper,
    when,
    to_timestamp,
    row_number,
    lit
)
from pyspark.sql.window import Window


BRONZE_TABLE = "dev.customer_cdc.bronze_customers"
SILVER_TABLE = "dev.customer_cdc.silver_customers"
QUARANTINE_TABLE = "dev.customer_cdc.quarantine_customers"


def run_silver_transformation():

    spark = SparkSession.builder.getOrCreate()

    print("Starting Silver transformation...")

    # Step 1: Read Bronze records
    bronze_df = spark.table(BRONZE_TABLE)

    # Step 2: Clean and convert data types
    clean_df = (
        bronze_df
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

    # Step 3: Apply data quality rules
    validated_df = clean_df.withColumn(
        "validation_error",
        when(
            col("customer_id").isNull()
            | (col("customer_id") <= 0),
            lit("Invalid customer ID")
        )
        .when(
            col("name").isNull()
            | (col("name") == ""),
            lit("Missing name")
        )
        .when(
            col("email").isNull()
            | (~col("email").rlike(
                r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
            )),
            lit("Invalid email")
        )
        .when(
            col("tier").isNull()
            | (~col("tier").isin(
                "Bronze", "Silver", "Gold", "Platinum"
            )),
            lit("Invalid tier")
        )
        .when(
            col("status").isNull()
            | (~col("status").isin(
                "Active", "Inactive"
            )),
            lit("Invalid status")
        )
        .when(
            col("updated_at").isNull(),
            lit("Invalid timestamp")
        )
        .when(
            col("operation").isNull()
            | (~col("operation").isin("I", "U", "D")),
            lit("Invalid operation")
        )
        .otherwise(lit(None).cast("string"))
    )

    # Step 4: Separate valid and invalid records
    valid_df = (
        validated_df
        .filter(col("validation_error").isNull())
        .drop("validation_error")
    )

    invalid_df = validated_df.filter(
        col("validation_error").isNotNull()
    )

    # Step 5: Deduplicate valid customers
    window_spec = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            col("updated_at").desc(),
            col("ingestion_time").desc(),
            col("source_file").desc()
        )
    )

    deduplicated_df = (
        valid_df
        .withColumn(
            "row_num",
            row_number().over(window_spec)
        )
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # Step 6: Create the initial Silver table only once
    if not spark.catalog.tableExists(SILVER_TABLE):

        (
            deduplicated_df.write
            .format("delta")
            .mode("error")
            .saveAsTable(SILVER_TABLE)
        )

        print("Initial Silver table created.")

    else:
        print(
            "Silver table already exists. "
            "Skipping initial snapshot load."
        )

    # Step 7: Create the Quarantine table
    if not spark.catalog.tableExists(QUARANTINE_TABLE):

        (
            invalid_df.write
            .format("delta")
            .mode("error")
            .saveAsTable(QUARANTINE_TABLE)
        )

        print("Quarantine table created.")

    else:
        print(
            "Quarantine table already exists. "
            "Skipping initial snapshot load."
        )

    print("Silver transformation completed!")


if __name__ == "__main__":
    run_silver_transformation()
