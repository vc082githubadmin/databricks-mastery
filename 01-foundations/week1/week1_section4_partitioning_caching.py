# ============================================================
# Week 1 · Section 4 — Partitioning, Caching & Persistence
# Reference code — understand each block, then type it yourself
# ============================================================

# ── BLOCK 1: SparkSession Setup ──────────────────────────────
# Same pattern as previous sections.
# Note: shuffle.partitions = 4 for local dev — we'll observe
# the effect of changing this in Block 3.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, broadcast
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, LongType, DoubleType
)
from pyspark.storagelevel import StorageLevel
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("Week1-S4: Partitioning & Caching")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.ui.port", "4040")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print(f"\nSpark UI → {spark.sparkContext.uiWebUrl}")
print(f"Spark version: {spark.version}")
print(f"Default parallelism: {spark.sparkContext.defaultParallelism}\n")


# ── BLOCK 2: Data Setup ───────────────────────────────────────
# We'll generate a larger synthetic dataset this time
# to make partitioning and caching effects more observable.
# 1000 transactions across 5 departments and 4 regions.

import random
random.seed(42)

departments = ["Engineering", "Marketing", "HR", "Finance", "Sales"]
regions     = ["North", "South", "East", "West"]
statuses    = ["completed", "pending", "cancelled"]

transactions_data = [
    (
        i,
        f"EMP-{random.randint(1, 100)}",
        random.choice(departments),
        random.choice(regions),
        random.choice(statuses),
        random.randint(100, 10000),
        random.uniform(0.5, 5.0)  # exchange rate — DoubleType
    )
    for i in range(1, 1001)
]

transactions_schema = StructType([
    StructField("txn_id",      IntegerType(), nullable=False),
    StructField("emp_id",      StringType(),  nullable=False),
    StructField("dept",        StringType(),  nullable=False),
    StructField("region",      StringType(),  nullable=False),
    StructField("status",      StringType(),  nullable=False),
    StructField("amount",      LongType(),    nullable=False),
    StructField("exchange_rate", DoubleType(), nullable=False),
])

df_txn = spark.createDataFrame(transactions_data, transactions_schema)

# Currency lookup table — small, perfect for broadcast
currency_data = [
    ("North", "USD", 1.0),
    ("South", "GBP", 0.79),
    ("East",  "EUR", 0.85),
    ("West",  "JPY", 110.0),
]

currency_schema = StructType([
    StructField("region",   StringType(),  nullable=False),
    StructField("currency", StringType(),  nullable=False),
    StructField("rate",     DoubleType(),  nullable=False),
])

df_currency = spark.createDataFrame(currency_data, currency_schema)

print("=== Transactions Sample (first 5) ===")
df_txn.show(5)
print(f"Total transactions: {df_txn.count()}")
print(f"Initial partitions: {df_txn.rdd.getNumPartitions()}\n")


# ── BLOCK 3: repartition() vs coalesce() ─────────────────────
# Observe how partition count changes and what it costs.

spark.sparkContext.setJobDescription("Block3a: check initial partitions")
print("=== Partition Counts ===")
print(f"Original partitions:          {df_txn.rdd.getNumPartitions()}")

# repartition() — full shuffle, creates exactly n partitions, even distribution
df_repartitioned = df_txn.repartition(8)
print(f"After repartition(8):         {df_repartitioned.rdd.getNumPartitions()}")

# coalesce() — no shuffle, merges existing partitions, can only decrease
df_coalesced = df_txn.coalesce(2)
print(f"After coalesce(2):            {df_coalesced.rdd.getNumPartitions()}")

# WHY the difference matters — check the query plans
print("\n=== repartition() query plan — look for Exchange node ===")
df_repartitioned.explain(mode="simple")
# You will see: Exchange (shuffle) — data moves across executors

print("\n=== coalesce() query plan — no Exchange node ===")
df_coalesced.explain(mode="simple")
# You will see: Coalesce — no shuffle, just merging local partitions

# Repartition by a specific column — used before joins or writes
# Routes all rows with the same dept to the same partition
df_repartitioned_by_dept = df_txn.repartition(4, col("dept"))
print(f"\nAfter repartition(4, 'dept'): "
      f"{df_repartitioned_by_dept.rdd.getNumPartitions()}")
# WHY: if downstream operations join or groupBy dept frequently,
# pre-partitioning by dept avoids the shuffle in those operations


# ── BLOCK 4: The Small Files Problem ─────────────────────────
# Demonstrate why coalesce() before write matters.

spark.sparkContext.setJobDescription("Block4: small files vs coalesced write")

# Write WITHOUT coalesce — creates one file per partition
df_txn.write.format("parquet").mode("overwrite") \
    .save("files/txn_many_files")

# Write WITH coalesce — creates fewer, larger files
df_txn.coalesce(1).write.format("parquet").mode("overwrite") \
    .save("files/txn_single_file")

# Count files written — observe the difference
import os

many_files_count = len([
    f for f in os.listdir("files/txn_many_files")
    if f.endswith(".parquet")
])
single_file_count = len([
    f for f in os.listdir("files/txn_single_file")
    if f.endswith(".parquet")
])

print(f"\n=== Small Files Demonstration ===")
print(f"Without coalesce: {many_files_count} parquet files")
print(f"With coalesce(1): {single_file_count} parquet file")
print(f"Same data, dramatically different read performance at scale")

# PRODUCTION NOTE: coalesce(1) means one file — fine for small data.
# For large data, use coalesce(n) where n = total_size / 128MB
# Example: 2GB output → coalesce(16) → sixteen 128MB files


# ── BLOCK 5: cache() and unpersist() ─────────────────────────
# Show how caching prevents re-reading source data for multiple actions.

spark.sparkContext.setJobDescription("Block5: build base DataFrame")

# Build an expensive base DataFrame — join + filter
df_base = df_txn.join(
    broadcast(df_currency),   # broadcast the small currency table
    on="region",
    how="inner"
).filter(col("status") == "completed")

# WITHOUT caching — this base DataFrame is recomputed for every action
# Uncomment the explain to see the full lineage that gets re-executed
# df_base.explain(mode="simple")

# Cache the base DataFrame — materialised on first action, reused after
df_base.cache()
print("\n=== Caching demonstration ===")
print("df_base is registered for caching (not yet materialised)")

# Action 1 — materialises the cache
spark.sparkContext.setJobDescription("Block5a: action1 - materialises cache")
total_completed = df_base.count()
print(f"Action 1 complete: {total_completed} completed transactions")
print("df_base is now in memory — subsequent actions reuse it")

# Action 2 — reads from memory, does NOT re-read source
spark.sparkContext.setJobDescription("Block5b: action2 - reads from cache")
df_summary = df_base.groupBy("dept").agg(
    count("*").alias("txn_count"),
    avg("amount").alias("avg_amount")
)
df_summary.show()

# Action 3 — also reads from memory
spark.sparkContext.setJobDescription("Block5c: action3 - reads from cache")
df_regional = df_base.groupBy("region", "currency").agg(
    sum("amount").alias("total_amount")
)
df_regional.show()

# ALWAYS unpersist when done — release memory for subsequent operations
df_base.unpersist()
print("df_base unpersisted — memory released")

# Verify it's no longer cached
print(f"Is df_base still cached? {df_base.is_cached}")


# ── BLOCK 6: persist() with explicit StorageLevel ────────────
# cache() = persist(MEMORY_AND_DISK) — the default.
# persist() lets you choose the storage level explicitly.

spark.sparkContext.setJobDescription("Block6: explicit persist levels")

# MEMORY_AND_DISK — try RAM first, spill to disk if needed
# This is what cache() uses internally — the most common choice
df_txn.persist(StorageLevel.MEMORY_AND_DISK)
df_txn.count()  # action to materialise
print(f"\n=== Explicit persist with MEMORY_AND_DISK ===")
print(f"Is df_txn cached? {df_txn.is_cached}")
df_txn.unpersist()

# MEMORY_ONLY — RAM only, no disk fallback
# Faster if data fits, throws OOM if it doesn't
df_txn.persist(StorageLevel.MEMORY_ONLY)
df_txn.count()
print(f"\n=== Explicit persist with MEMORY_ONLY ===")
print(f"Is df_txn cached? {df_txn.is_cached}")
df_txn.unpersist()

# DISK_ONLY — disk only, no RAM
# Use when DataFrame is very large and RAM is scarce
# Slower than MEMORY_AND_DISK but avoids OOM on large data
df_txn.persist(StorageLevel.DISK_ONLY)
df_txn.count()
print(f"\n=== Explicit persist with DISK_ONLY ===")
print(f"Is df_txn cached? {df_txn.is_cached}")
df_txn.unpersist()

print("\nAll persisted DataFrames unpersisted — memory clean")


# ── BLOCK 7: Broadcast Variables ─────────────────────────────
# Different from broadcast joins — this is for arbitrary Python objects
# used inside UDFs or map operations across many tasks.

spark.sparkContext.setJobDescription("Block7: broadcast variable")

# A Python dictionary mapping dept to division
# Without broadcast: sent to every task (potentially hundreds of times)
# With broadcast: sent once per executor (typically 4-20 times)
division_map = {
    "Engineering": "Technology",
    "Marketing":   "Commercial",
    "HR":          "Operations",
    "Finance":     "Operations",
    "Sales":       "Commercial",
}

# Broadcast the dictionary — sent once per executor, not once per task
division_broadcast = spark.sparkContext.broadcast(division_map)

# Use it inside a UDF
from pyspark.sql.functions import udf

@udf(returnType=StringType())
def get_division(dept):
    # .value accesses the broadcasted object
    return division_broadcast.value.get(dept, "Unknown")

df_with_division = df_txn.withColumn("division", get_division(col("dept")))

print("\n=== Broadcast Variable: Dept → Division mapping ===")
df_with_division.select("dept", "division").distinct() \
    .orderBy("dept").show()

# Always unpublish broadcast variables when done
division_broadcast.unpublish() if hasattr(division_broadcast, 'unpublish') \
    else division_broadcast.destroy()


# ── BLOCK 8: Accumulators ─────────────────────────────────────
# Write-only from executors, read-only from Driver.
# Use for counting events distributed across tasks.

spark.sparkContext.setJobDescription("Block8: accumulators")

# Create accumulators — Driver owns these
negative_amounts   = spark.sparkContext.accumulator(0)
high_value_txns    = spark.sparkContext.accumulator(0)
cancelled_txns     = spark.sparkContext.accumulator(0)

# Process rows — executors write to accumulators
# IMPORTANT: use foreachPartition (action) not map (transformation)
# Accumulators in transformations may be incremented multiple times
# due to task retries — unreliable. In actions: exactly once.

def count_anomalies(partition):
    for row in partition:
        if row["amount"] < 0:
            negative_amounts.add(1)
        if row["amount"] > 8000:
            high_value_txns.add(1)
        if row["status"] == "cancelled":
            cancelled_txns.add(1)

df_txn.foreachPartition(count_anomalies)

# Driver reads accumulator values
print("\n=== Accumulator Results ===")
print(f"Negative amount transactions: {negative_amounts.value}")
print(f"High value transactions (>8000): {high_value_txns.value}")
print(f"Cancelled transactions: {cancelled_txns.value}")
print(f"Total transactions: {df_txn.count()}")


# ── BLOCK 9: Putting It All Together ─────────────────────────
# Full pipeline using all four concepts:
# broadcast join + cache + accumulator + coalesce before write

spark.sparkContext.setJobDescription("Block9: complete pipeline")

# Step 1 — Accumulator for bad records (before any filtering)
bad_records_count = spark.sparkContext.accumulator(0)

def flag_bad_records(partition):
    for row in partition:
        if row["amount"] <= 0:
            bad_records_count.add(1)

df_txn.foreachPartition(flag_bad_records)

# Step 2 — Build the base DataFrame with broadcast join
df_pipeline_base = df_txn \
    .filter(col("amount") > 0) \
    .join(broadcast(df_currency), on="region", how="inner")

# Step 3 — Cache before multiple actions
df_pipeline_base.cache()

# Step 4 — Three aggregations, all reading from cache
spark.sparkContext.setJobDescription("Block9a: dept summary")
df_dept_summary = df_pipeline_base \
    .groupBy("dept") \
    .agg(count("*").alias("txn_count"),
         avg("amount").alias("avg_amount"))

spark.sparkContext.setJobDescription("Block9b: regional summary")
df_regional_summary = df_pipeline_base \
    .groupBy("region", "currency") \
    .agg(sum("amount").alias("total_amount"))

spark.sparkContext.setJobDescription("Block9c: status breakdown")
df_status_summary = df_pipeline_base \
    .groupBy("status") \
    .agg(count("*").alias("count"))

# Step 5 — Write all three, coalesce before each write
df_dept_summary.coalesce(1).write.format("delta").mode("overwrite") \
    .save("files/pipeline_dept_summary")

df_regional_summary.coalesce(1).write.format("delta").mode("overwrite") \
    .save("files/pipeline_regional_summary")

df_status_summary.coalesce(1).write.format("delta").mode("overwrite") \
    .save("files/pipeline_status_summary")

# Step 6 — Unpersist immediately after all writes complete
df_pipeline_base.unpersist()

# Step 7 — Report bad records
print("\n=== Complete Pipeline Results ===")
print(f"Bad records (amount <= 0): {bad_records_count.value}")
print(f"Department summary written to: files/pipeline_dept_summary")
print(f"Regional summary written to:   files/pipeline_regional_summary")
print(f"Status summary written to:     files/pipeline_status_summary")

# Verify outputs
print("\n=== Department Summary ===")
spark.read.format("delta").load("files/pipeline_dept_summary").show()

print("\n=== Regional Summary ===")
spark.read.format("delta").load("files/pipeline_regional_summary").show()

print("\n=== Status Summary ===")
spark.read.format("delta").load("files/pipeline_status_summary").show()


input("\n>>> All blocks complete. Check Spark UI. Press ENTER to stop.")
spark.stop()