"""Simple PySpark partitioning examples."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date


spark = (
    SparkSession.builder
    .appName("PartitioningExamples")
    .getOrCreate()
)

data = [
    (1, "2026-06-20 10:00:00", 100.0),
    (2, "2026-06-20 11:00:00", 150.0),
    (3, "2026-06-21 09:00:00", 200.0),
]

df = spark.createDataFrame(
    data,
    ["transaction_id", "event_timestamp", "amount"],
)

# Create a date column that can be used for partitioning.
df_with_date = df.withColumn(
    "event_date",
    to_date("event_timestamp"),
)

# Date partition example.
# This creates separate output folders for each event_date.
df_with_date.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet("output/transactions_by_date")

# repartition() performs a shuffle.
# Use it when increasing partitions or redistributing data.
repartitioned_df = df_with_date.repartition(
    4,
    "event_date",
)

# coalesce() is mainly used to reduce partitions.
# It usually avoids a full shuffle.
coalesced_df = repartitioned_df.coalesce(2)

print(
    "Repartitioned partition count:",
    repartitioned_df.rdd.getNumPartitions(),
)

print(
    "Coalesced partition count:",
    coalesced_df.rdd.getNumPartitions(),
)

spark.stop()
