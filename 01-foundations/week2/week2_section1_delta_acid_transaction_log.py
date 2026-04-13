# ============================================================
# Week 2 · Section 1 — Delta Lake ACID & Transaction Log
# Run this in Databricks Community Edition
# No SparkSession creation needed — spark is pre-created
# ============================================================

# ── BLOCK 1: Verify environment ──────────────────────────────
# In Databricks you never write SparkSession.builder
# spark is automatically available in every notebook cell

print(f"Spark version: {spark.version}")
print(f"Current user: {spark.sql('SELECT current_user()').collect()[0][0]}")

# Confirm Delta is available — no pip install needed in Databricks
spark.sql("SELECT 1 AS delta_check").show()


# ── BLOCK 2: Create a Delta table ────────────────────────────
# We use saveAsTable() to register in Unity Catalog
# This is the Databricks-native pattern — not save("path")

# Drop table if it exists from previous runs
spark.sql("DROP TABLE IF EXISTS default.employees_delta")

# Create sample data
data = [
    (1,  "Alice",   "Engineering", 95000),
    (2,  "Bob",     "Marketing",   72000),
    (3,  "Carol",   "Engineering", 105000),
    (4,  "Dave",    "Marketing",   68000),
    (5,  "Eve",     "Engineering", 88000),
]

df = spark.createDataFrame(
    data,
    ["id", "name", "dept", "salary"]
)

df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("default.employees_delta")

print("Table created — version 0")
spark.sql("SELECT * FROM default.employees_delta").show()


# ── BLOCK 3: Inspect the transaction log ─────────────────────
# DESCRIBE HISTORY shows every transaction as a row
# This is the human-readable view of _delta_log/

print("\n=== Transaction History (version 0) ===")
spark.sql("""
    DESCRIBE HISTORY default.employees_delta
""").select(
    "version",
    "timestamp",
    "operation",
    "operationParameters"
).show(truncate=False)

# DESCRIBE DETAIL shows current table metadata
print("\n=== Table Detail ===")
spark.sql("""
    DESCRIBE DETAIL default.employees_delta
""").select(
    "format",
    "numFiles",
    "sizeInBytes",
    "location"
).show(truncate=False)

# OBSERVE: version = 0, operation = CREATE TABLE or WRITE
# location shows where the Delta files live in cloud storage


# ── BLOCK 4: Make changes — watch versions increment ─────────
# Each write operation creates a new version in the log

# Version 1 — append new employees
new_employees = spark.createDataFrame(
    [(6, "Frank", "HR", 65000),
     (7, "Grace", "Engineering", 112000)],
    ["id", "name", "dept", "salary"]
)

new_employees.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("default.employees_delta")

print("\n=== After append (should be version 1) ===")
spark.sql("SELECT COUNT(*) as total FROM default.employees_delta").show()

# Version 2 — update Alice's salary
spark.sql("""
    UPDATE default.employees_delta
    SET salary = 102000
    WHERE name = 'Alice'
""")

print("\n=== After UPDATE (should be version 2) ===")
spark.sql("""
    SELECT name, salary FROM default.employees_delta
    WHERE name = 'Alice'
""").show()

# Version 3 — delete Dave
spark.sql("""
    DELETE FROM default.employees_delta
    WHERE name = 'Dave'
""")

print("\n=== After DELETE (should be version 3) ===")
spark.sql("""
    SELECT COUNT(*) as total FROM default.employees_delta
""").show()

# Now check the full history — should show 4 versions (0,1,2,3)
print("\n=== Full Transaction History ===")
spark.sql("""
    DESCRIBE HISTORY default.employees_delta
""").select(
    "version",
    "timestamp",
    "operation"
).show()

# OBSERVE: each operation created a new version
# UPDATE and DELETE are tracked — this is what enables time travel


# ── BLOCK 5: Time Travel ─────────────────────────────────────
# Query the table AS IT WAS at any previous version

# What did the table look like at version 0 (original 5 rows)?
print("\n=== Time Travel: VERSION AS OF 0 ===")
spark.sql("""
    SELECT * FROM default.employees_delta VERSION AS OF 0
""").show()

# What did it look like at version 1 (after append, 7 rows)?
print("\n=== Time Travel: VERSION AS OF 1 ===")
spark.sql("""
    SELECT COUNT(*) as total FROM default.employees_delta VERSION AS OF 1
""").show()

# What was Alice's salary at version 1 (before the UPDATE)?
print("\n=== Alice's salary at version 1 (before UPDATE) ===")
spark.sql("""
    SELECT name, salary
    FROM default.employees_delta VERSION AS OF 1
    WHERE name = 'Alice'
""").show()

# What is Alice's salary now (version 2 onwards)?
print("\n=== Alice's salary now (after UPDATE) ===")
spark.sql("""
    SELECT name, salary
    FROM default.employees_delta
    WHERE name = 'Alice'
""").show()

# OBSERVE: version 1 shows salary = 95000
#          current version shows salary = 102000
# The data files for version 1 are still on disk — Delta never deleted them


# ── BLOCK 6: RESTORE — rollback to a previous version ────────
# If a bad write corrupted your table, restore to a known good version

# Current state: version 3 (Dave deleted, Alice updated, Frank+Grace added)
print("\n=== Current state (version 3) ===")
spark.sql("SELECT * FROM default.employees_delta ORDER BY id").show()

# Restore to version 0 — back to original 5 rows, original salaries
spark.sql("""
    RESTORE TABLE default.employees_delta TO VERSION AS OF 0
""")

print("\n=== After RESTORE to version 0 ===")
spark.sql("SELECT * FROM default.employees_delta ORDER BY id").show()

# Check history — RESTORE itself creates a new version
print("\n=== History after RESTORE ===")
spark.sql("""
    DESCRIBE HISTORY default.employees_delta
""").select(
    "version",
    "timestamp",
    "operation"
).show()

# OBSERVE: RESTORE created version 4
# The table is back to original 5 rows
# Versions 1,2,3 still exist in the log — nothing is erased
# You could restore to version 3 again if needed


# ── BLOCK 7: VACUUM — clean up old files ─────────────────────
# VACUUM removes data files no longer referenced by the active log
# Default retention: 7 days
# WARNING: after VACUUM, time travel beyond retention window is impossible

# Check what VACUUM would delete (DRY RUN — does not delete anything)
print("\n=== VACUUM DRY RUN (shows files that would be deleted) ===")
spark.sql("""
    VACUUM default.employees_delta DRY RUN
""").show(truncate=False)

# Run VACUUM with default 7-day retention
# Files newer than 7 days are protected — so nothing is deleted in this lab
spark.sql("""
    VACUUM default.employees_delta
""")

print("\n=== VACUUM complete ===")
print("Files older than 7 days removed (none in this lab — all files are new)")

# IMPORTANT: Never run VACUUM RETAIN 0 HOURS in production
# It bypasses the safety check and destroys time travel capability
# Databricks blocks it by default — requires explicit override to enable
