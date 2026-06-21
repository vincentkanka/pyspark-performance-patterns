# Adaptive Query Execution Notes

## Enabling AQE

Adaptive Query Execution can be enabled through Spark configuration.

```python
spark.conf.set(
    "spark.sql.adaptive.enabled",
    "true",
)
```

Enable skew join handling:

```python
spark.conf.set(
    "spark.sql.adaptive.skewJoin.enabled",
    "true",
)
```

AQE is enabled by default in many recent Spark environments, but the setting should still be verified.

## Skew Handling

Data skew occurs when a small number of keys contain significantly more data than other keys.

For example, one customer or transaction type may contain millions of records while most keys contain only a few records.

AQE can detect large shuffle partitions and split them into smaller partitions during execution.

Useful settings:

```python
spark.conf.set(
    "spark.sql.adaptive.enabled",
    "true",
)

spark.conf.set(
    "spark.sql.adaptive.skewJoin.enabled",
    "true",
)
```

AQE can reduce the impact of skew, but severe skew may still require:

- Filtering unnecessary records before joins.
- Broadcasting a small lookup table.
- Salting heavily skewed keys.
- Choosing a better partition key.
- Aggregating data before the join.

## Common Mistakes

### Assuming AQE fixes every performance issue

AQE improves execution plans, but it cannot fix poor data modeling, unnecessary transformations, or badly designed joins.

### Using too many manual partitions

A very large partition count can create scheduling overhead and generate many small output files.

### Broadcasting tables that are too large

Broadcasting a large dataset can cause executor memory problems or job failures.

### Ignoring data skew

AQE helps with skew, but extremely uneven keys may still require salting or preprocessing.

### Ignoring the Spark UI

Review the Spark UI before changing configuration.

Look for:

- Large shuffle reads and writes.
- Long-running tasks.
- Skewed partitions.
- Excessive task counts.
- Large differences in task duration.
