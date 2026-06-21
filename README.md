# PySpark Optimization Patterns

## Purpose

This repository contains simple PySpark examples for common optimization patterns used in ETL pipelines.

The goal is to improve performance, reduce unnecessary shuffle, and avoid common scaling issues without overcomplicating the code.

## Partitioning

Partitioning controls how Spark distributes data across executors and how output files are organized.

Common recommendations:

- Partition output data by frequently filtered columns such as dates.
- Use `repartition()` when redistributing data or increasing partitions.
- Use `coalesce()` when reducing the number of partitions.
- Avoid creating too many small partitions.

See `partitioning_examples.py`.

## Broadcast Joins

Broadcast joins are useful when one side of a join is small enough to fit in executor memory.

Broadcasting a small lookup table can:

- Avoid a large shuffle.
- Improve join performance.
- Work well for dimension and reference-table joins.

See `broadcast_join_examples.py`.

## AQE

Adaptive Query Execution allows Spark to adjust the physical execution plan while a job is running.

AQE can help with:

- Skewed join partitions.
- Reducing unnecessary shuffle partitions.
- Dynamically changing join strategies.
- Combining small shuffle partitions.

See `aqe_notes.md`.

## Storage Optimization

For most analytical PySpark workloads:

- Use Parquet instead of CSV or JSON.
- Use compression such as Snappy.
- Partition output by commonly filtered columns.
- Avoid creating too many small files.
- Write only the columns required by downstream consumers.

Example:

```python
df.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet("s3://example-bucket/output/")
```

## Example Snippets

### Repartition by date

```python
partitioned_df = df.repartition("event_date")
```

### Reduce partitions

```python
smaller_df = partitioned_df.coalesce(4)
```

### Broadcast a lookup table

```python
from pyspark.sql.functions import broadcast

result_df = transactions_df.join(
    broadcast(products_df),
    on="product_id",
    how="left",
)
```

### Enable AQE

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

## Lessons Learned

- More partitions do not always mean better performance.
- Use `repartition()` when redistribution is required.
- Use `coalesce()` when reducing partitions without a full shuffle.
- Broadcast only datasets that are genuinely small.
- AQE helps, but it does not replace good partitioning and join design.
- File size and file count affect downstream query performance.
- Small architectural decisions can compound into significant performance and reliability improvements over time.
