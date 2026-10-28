
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    when,
    current_timestamp
)


SILVER_VIEW = "dev.customer_cdc.silver_customers_current"
GOLD_TABLE = "dev.customer_cdc.gold_customer_tiers"


def run_gold():

    spark = SparkSession.builder.getOrCreate()

    customers_df = spark.table(SILVER_VIEW)

    gold_df = (
        customers_df
        .groupBy("tier")
        .agg(
            count("*").alias("total_customers"),
            spark_sum(
                when(
                    col("status") == "Active", 1
                ).otherwise(0)
            ).alias("active_customers")
        )
        .withColumn(
            "refreshed_at",
            current_timestamp()
        )
    )

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_TABLE)
    )

    print("Gold customer reporting table refreshed")


if __name__ == "__main__":
    run_gold()
