import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


STATE_TABLE = "dev.customer_cdc.customer_state_v2"
CURRENT_VIEW = "dev.customer_cdc.silver_customers_current"


def run_data_quality(force_failure=False):
 
    spark = SparkSession.builder.getOrCreate()

    print("Starting customer data quality checks...")

    state_df = spark.table(STATE_TABLE)
    current_df = spark.table(CURRENT_VIEW)

    # Rule 1: Customer IDs must be present and positive
    invalid_ids = state_df.filter(
        F.col("customer_id").isNull()
        | (F.col("customer_id") <= 0)
    ).count()

    # Rule 2: Only one current state per customer
    duplicate_ids = (
        state_df.groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    # Rule 3: Event timestamp must be present
    invalid_timestamps = state_df.filter(
        F.col("updated_at").isNull()
    ).count()

    # Rule 4: Tombstones must correspond to deletes
    invalid_tombstones = state_df.filter(
        F.col("is_deleted").isNull()
        | (
            F.col("is_deleted")
            != (F.col("operation") == "D")
        )
    ).count()

    # Rule 5: Current view must exclude deleted customers
    expected_current = state_df.filter(
        F.col("is_deleted") == False
    ).count()

    actual_current = current_df.count()

    current_count_mismatch = int(
        expected_current != actual_current
    )

    # Print all results
    results = {
        "invalid_customer_ids": invalid_ids,
        "duplicate_customer_ids": duplicate_ids,
        "invalid_timestamps": invalid_timestamps,
        "invalid_tombstones": invalid_tombstones,
        "current_count_mismatch": current_count_mismatch
    }

    for rule, failures in results.items():
        print(f"{rule}: {failures}")

    failed_rules = {
        name: count
        for name, count in results.items()
        if count > 0
    }

    if failed_rules:
        raise ValueError(
            f"Data quality validation failed: {failed_rules}"
        )

    print("All customer data quality checks passed!")
    if force_failure:
     raise RuntimeError(
        "SIMULATED FAILURE: Data Quality Gate Test"
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--force-failure",
        action="store_true",
        help="Simulate failure for workflow testing"
    )

    args = parser.parse_args()

    run_data_quality(
        force_failure=args.force_failure
    )