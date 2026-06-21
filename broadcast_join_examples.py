"""Simple PySpark broadcast join examples."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast


spark = (
    SparkSession.builder
    .appName("BroadcastJoinExamples")
    .getOrCreate()
)

transactions = [
    (1, 101, 25.0),
    (2, 102, 40.0),
    (3, 101, 15.0),
]

products = [
    (101, "Keyboard"),
    (102, "Mouse"),
]

transactions_df = spark.createDataFrame(
    transactions,
    ["transaction_id", "product_id", "amount"],
)

# Small lookup table.
products_df = spark.createDataFrame(
    products,
    ["product_id", "product_name"],
)

# Broadcast the small lookup table.
# This avoids shuffling both sides of the join.
result_df = transactions_df.join(
    broadcast(products_df),
    on="product_id",
    how="left",
)

result_df.show()

spark.stop()
