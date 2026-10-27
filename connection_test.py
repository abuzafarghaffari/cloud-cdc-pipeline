from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("Connected to Databricks successfully!")

print("Spark Version:", spark.version)

df = spark.range(1, 6)

df.show()