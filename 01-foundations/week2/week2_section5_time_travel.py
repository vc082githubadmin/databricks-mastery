# ============================================================
# Week 2 · Section 5 — Time Travel Deep Dive
# Run in Databricks — spark object pre-created automatically
# ============================================================


# ── BLOCK 1: Setup — build a versioned table ─────────────────
# Create a table with multiple meaningful versions
# Each version represents a realistic business operation

spark.sql("DROP TABLE IF EXISTS default.sales_history")

# Version 0 — table creation
spark.sql("""
    CREATE TABLE default.sales_history (
        sale_id     INT,
        product     STRING,
        amount      DOUBLE,
        region      STRING,
        sale_date   STRING,
        status      STRING
    ) USING DELTA
""")

# Version 1 — Q1 sales loaded
spark.sql("""
    INSERT INTO default.sales_history VALUES
    (1, 'Laptop',  999.99, 'North', '2024-01-15', 'completed'),
    (2, 'Phone',   699.99, 'South', '2024-01-16', 'completed'),
    (3, 'Tablet',  499.99, 'East',  '2024-01-17', 'completed'),
    (4, 'Monitor', 399.99, 'West',  '2024-01-18', 'completed'),
    (5, 'Webcam',   89.99, 'North', '2024-01-19', 'completed')
""")

# Version 2 — Q2 data appended
spark.sql("""
    INSERT INTO default.sales_history VALUES
    (6, 'Laptop',  999.99, 'South', '2024-04-01', 'completed'),
    (7, 'Phone',   699.99, 'East',  '2024-04-02', 'pending'),
    (8, 'Tablet',  499.99, 'North', '2024-04-03', 'completed')
""")

# Version 3 — bad pipeline run: incorrect amounts applied
# Simulates a production error we will need to recover from
spark.sql("""
    UPDATE default.sales_history
    SET amount = amount * 100
    WHERE sale_date >= '2024-01-01'
""")

print("=== After bad pipeline run (amounts multiplied by 100) ===")
spark.sql("""
    SELECT sale_id, product, amount, status
    FROM default.sales_history
    ORDER BY sale_id
""").show()

print("\n=== Transaction history ===")
spark.sql("""
    DESCRIBE HISTORY default.sales_history
""").select("version", "timestamp", "operation").show()


# ── BLOCK 2: Time travel by version ──────────────────────────
# Query the table as it existed at a specific version

print("\n=== VERSION AS OF 1 (Q1 data only — before Q2 append) ===")
spark.sql("""
    SELECT sale_id, product, amount
    FROM default.sales_history VERSION AS OF 1
    ORDER BY sale_id
""").show()

print("\n=== VERSION AS OF 2 (Q1 + Q2 data — before bad update) ===")
spark.sql("""
    SELECT COUNT(*) as total_rows,
           SUM(amount) as total_revenue
    FROM default.sales_history VERSION AS OF 2
""").show()

print("\n=== Current version (corrupted amounts) ===")
spark.sql("""
    SELECT COUNT(*) as total_rows,
           SUM(amount) as total_revenue
    FROM default.sales_history
""").show()

# OBSERVE: version 2 total revenue is correct
#          current version total revenue is 100x too high


# ── BLOCK 3: Time travel by timestamp ────────────────────────
# Find the timestamp of version 2 from DESCRIBE HISTORY
# Then use it to query by timestamp

history = spark.sql("""
    DESCRIBE HISTORY default.sales_history
""").select("version", "timestamp").orderBy("version")

history.show()

# Get timestamp of version 2 programmatically
v2_timestamp = history.filter("version = 2") \
                      .select("timestamp") \
                      .collect()[0][0]

print(f"\n=== Version 2 timestamp: {v2_timestamp} ===")

# Query using that timestamp
print("\n=== TIMESTAMP AS OF version 2 ===")
spark.sql(f"""
    SELECT COUNT(*) as total_rows,
           SUM(amount) as total_revenue
    FROM default.sales_history
    TIMESTAMP AS OF '{v2_timestamp}'
""").show()

# Python API equivalent
v2_df = spark.read.format("delta") \
                  .option("timestampAsOf", str(v2_timestamp)) \
                  .table("default.sales_history")

print(f"\n=== Python API TIMESTAMP AS OF — row count: {v2_df.count()} ===")


# ── BLOCK 4: Data comparison between versions ────────────────
# Compare version 2 (correct) vs current (corrupted)
# Identify exactly which rows changed and by how much

from pyspark.sql.functions import col

current_df = spark.read.table("default.sales_history")

v2_df = spark.read.format("delta") \
                  .option("versionAsOf", 2) \
                  .table("default.sales_history")

print("\n=== Data diff: current vs version 2 (find corrupted rows) ===")
diff = current_df.alias("curr").join(
    v2_df.alias("prev"),
    on="sale_id"
).where("curr.amount != prev.amount") \
 .select(
     col("curr.sale_id"),
     col("curr.product"),
     col("prev.amount").alias("correct_amount"),
     col("curr.amount").alias("corrupted_amount")
 ).orderBy("sale_id")

diff.show()

# OBSERVE: every row shows corrupted_amount = correct_amount * 100
# This diff pattern pinpoints exactly what the bad pipeline did


# ── BLOCK 5: RESTORE — point-in-time recovery ────────────────
# Restore the table to version 2 — before the bad update

print("\n=== Current state before RESTORE (corrupted) ===")
spark.sql("""
    SELECT SUM(amount) as total_revenue FROM default.sales_history
""").show()

spark.sql("""
    RESTORE TABLE default.sales_history TO VERSION AS OF 2
""")

print("\n=== After RESTORE to version 2 (correct amounts) ===")
spark.sql("""
    SELECT SUM(amount) as total_revenue FROM default.sales_history
""").show()

print("\n=== History after RESTORE — new version created ===")
spark.sql("""
    DESCRIBE HISTORY default.sales_history
""").select("version", "timestamp", "operation").show()

# OBSERVE: RESTORE creates a new version (not version 2 again)
# The history is preserved — version 3 (bad update) still exists
# You can still query version 3 via time travel if needed for audit


# ── BLOCK 6: Create new table from time travel ───────────────
# Instead of restoring in place, create a clean copy
# Useful when you want to keep the corrupted table for investigation

spark.sql("DROP TABLE IF EXISTS default.sales_clean_copy")

spark.sql("""
    CREATE TABLE default.sales_clean_copy
    USING DELTA
    AS SELECT * FROM default.sales_history VERSION AS OF 2
""")

print("\n=== Clean copy created from version 2 ===")
spark.sql("""
    SELECT COUNT(*) as rows,
           SUM(amount) as total_revenue
    FROM default.sales_clean_copy
""").show()


# ── BLOCK 7: Retention configuration ─────────────────────────
# View and configure how long history is retained

print("\n=== Current retention settings ===")
spark.sql("""
    DESCRIBE DETAIL default.sales_history
""").select("name", "properties").show(truncate=False)

# Set retention to 90 days for compliance requirements
spark.sql("""
    ALTER TABLE default.sales_history
    SET TBLPROPERTIES (
        'delta.deletedFileRetentionDuration' = 'interval 90 days',
        'delta.logRetentionDuration'         = 'interval 90 days'
    )
""")

print("\n=== Updated retention settings ===")
spark.sql("""
    DESCRIBE DETAIL default.sales_history
""").select("name", "properties").show(truncate=False)

# Verify time travel still works after retention change
print("\n=== Time travel still works after retention update ===")
spark.sql("""
    SELECT COUNT(*) as rows
    FROM default.sales_history VERSION AS OF 1
""").show()


# ── BLOCK 8: Programmatic version tracking ───────────────────
# Best practice: record the version used for reproducibility

# Get the current version number programmatically
current_version = spark.sql("""
    SELECT MAX(version) as current_version
    FROM (DESCRIBE HISTORY default.sales_history)
""").collect()[0][0]

print(f"\n=== Current version for reproducibility tracking: {current_version} ===")

# Simulate reading data for an ML training run
training_df = spark.read.format("delta") \
                        .option("versionAsOf", current_version) \
                        .table("default.sales_history")

print(f"Training dataset row count: {training_df.count()}")
print(f"Version to log with model: {current_version}")
print("Log this version in MLflow or job parameters for exact reproducibility")

# OBSERVE: by recording current_version at training time
# you can always reconstruct the exact dataset used
# regardless of how many updates the table receives later