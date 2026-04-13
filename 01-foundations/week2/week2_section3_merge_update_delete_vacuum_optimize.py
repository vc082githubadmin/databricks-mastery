# ============================================================
# Week 2 · Section 3 — MERGE INTO, UPDATE, DELETE, VACUUM & OPTIMIZE
# Run in Databricks — spark object pre-created automatically
# ============================================================


# ── BLOCK 1: Setup ───────────────────────────────────────────
# Create a customers target table and a CDC source table

spark.sql("DROP TABLE IF EXISTS default.customers_target")
spark.sql("DROP TABLE IF EXISTS default.customers_cdc")

# Target table — current state of customers
customers_data = [
    (1, "Alice",   "alice@email.com",   "active",  "2024-01-01"),
    (2, "Bob",     "bob@email.com",     "active",  "2024-01-02"),
    (3, "Carol",   "carol@email.com",   "active",  "2024-01-03"),
    (4, "Dave",    "dave@email.com",    "active",  "2024-01-04"),
    (5, "Eve",     "eve@email.com",     "inactive","2024-01-05"),
]

df_customers = spark.createDataFrame(
    customers_data,
    ["customer_id", "name", "email", "status", "created_date"]
)

df_customers.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable("default.customers_target")

print("=== Initial customers table ===")
spark.sql("SELECT * FROM default.customers_target ORDER BY customer_id").show()


# ── BLOCK 2: UPDATE ──────────────────────────────────────────
# Update Eve's status from inactive to active
# Update Bob's email address

spark.sql("""
    UPDATE default.customers_target
    SET status = 'active'
    WHERE customer_id = 5
""")

spark.sql("""
    UPDATE default.customers_target
    SET email = 'bob.new@email.com',
        status = 'premium'
    WHERE customer_id = 2
""")

print("\n=== After UPDATE (Eve active, Bob premium) ===")
spark.sql("""
    SELECT customer_id, name, email, status
    FROM default.customers_target
    ORDER BY customer_id
""").show()

# Check how many versions exist now
print("\n=== History after UPDATEs ===")
spark.sql("""
    DESCRIBE HISTORY default.customers_target
""").select("version", "timestamp", "operation").show()


# ── BLOCK 3: DELETE ──────────────────────────────────────────
# Hard delete — permanently remove Dave
# Soft delete pattern — mark a record as deleted without removing it

# Hard delete
spark.sql("""
    DELETE FROM default.customers_target
    WHERE customer_id = 4
""")

print("\n=== After hard DELETE of Dave ===")
spark.sql("""
    SELECT customer_id, name, status
    FROM default.customers_target
    ORDER BY customer_id
""").show()

# Soft delete pattern — add is_deleted column first via schema evolution
# Then mark records as deleted instead of removing them
spark.sql("""
    ALTER TABLE default.customers_target
    ADD COLUMNS (is_deleted BOOLEAN)
""")

spark.sql("""
    UPDATE default.customers_target
    SET is_deleted = false
    WHERE is_deleted IS NULL
""")

spark.sql("""
    UPDATE default.customers_target
    SET is_deleted = true,
        status = 'deleted'
    WHERE customer_id = 5
""")

# Normal query — filters out soft-deleted records
print("\n=== Normal query (excludes soft-deleted) ===")
spark.sql("""
    SELECT customer_id, name, email, status
    FROM default.customers_target
    WHERE is_deleted = false OR is_deleted IS NULL
    ORDER BY customer_id
""").show()

# Audit query — shows everything including soft-deleted
print("\n=== Audit query (includes soft-deleted) ===")
spark.sql("""
    SELECT customer_id, name, status, is_deleted
    FROM default.customers_target
    ORDER BY customer_id
""").show()


# ── BLOCK 4: MERGE INTO — the CDC upsert pattern ─────────────
# Source table: incoming changes with op_type I/U/D

spark.sql("DROP TABLE IF EXISTS default.customers_cdc")

cdc_data = [
    # Update Bob's email again
    (2, "Bob",     "bob.updated@email.com", "premium", "2024-02-01", "U"),
    # Insert new customer Frank
    (6, "Frank",   "frank@email.com",       "active",  "2024-02-01", "I"),
    # Insert new customer Grace
    (7, "Grace",   "grace@email.com",       "active",  "2024-02-01", "I"),
    # Delete Alice (op_type D)
    (1, "Alice",   "alice@email.com",       "active",  "2024-01-01", "D"),
]

df_cdc = spark.createDataFrame(
    cdc_data,
    ["customer_id", "name", "email", "status", "created_date", "op_type"]
)

df_cdc.write.format("delta") \
      .mode("overwrite") \
      .saveAsTable("default.customers_cdc")

print("\n=== CDC source records ===")
spark.sql("SELECT * FROM default.customers_cdc ORDER BY customer_id").show()

# MERGE — handles all three op types in one atomic transaction
spark.sql("""
    MERGE INTO default.customers_target t
    USING default.customers_cdc s
    ON t.customer_id = s.customer_id

    WHEN MATCHED AND s.op_type = 'D'
        THEN DELETE

    WHEN MATCHED AND s.op_type = 'U'
        THEN UPDATE SET
            t.email        = s.email,
            t.status       = s.status,
            t.is_deleted   = false

    WHEN NOT MATCHED AND s.op_type = 'I'
        THEN INSERT (customer_id, name, email, status, created_date, is_deleted)
             VALUES (s.customer_id, s.name, s.email, s.status, s.created_date, false)
""")

print("\n=== After MERGE ===")
print("Expected: Alice deleted, Bob email updated, Frank and Grace inserted")
spark.sql("""
    SELECT customer_id, name, email, status
    FROM default.customers_target
    ORDER BY customer_id
""").show()

# Full history — shows every operation
print("\n=== Full transaction history ===")
spark.sql("""
    DESCRIBE HISTORY default.customers_target
""").select("version", "timestamp", "operation").show()


# ── BLOCK 5: MERGE for deduplication ─────────────────────────
# A common pattern: insert only if the record doesn't already exist
# Prevents duplicate records from being inserted

spark.sql("DROP TABLE IF EXISTS default.events")

spark.sql("""
    CREATE TABLE default.events (
        event_id   STRING,
        event_type STRING,
        user_id    INT,
        event_time TIMESTAMP
    ) USING DELTA
""")

spark.sql("""
    INSERT INTO default.events VALUES
    ('E001', 'click', 101, current_timestamp()),
    ('E002', 'view',  102, current_timestamp())
""")

print("\n=== Initial events ===")
spark.sql("SELECT * FROM default.events").show()

# Simulate a reprocessing scenario — E001 arrives again (duplicate)
# Plus E003 is genuinely new
new_events = spark.createDataFrame(
    [("E001", "click", 101),
     ("E003", "purchase", 101)],
    ["event_id", "event_type", "user_id"]
)

new_events.createOrReplaceTempView("new_events_staging")

# MERGE with only WHEN NOT MATCHED = insert-only deduplication
spark.sql("""
    MERGE INTO default.events t
    USING new_events_staging s
    ON t.event_id = s.event_id

    WHEN NOT MATCHED
        THEN INSERT (event_id, event_type, user_id, event_time)
             VALUES (s.event_id, s.event_type, s.user_id, current_timestamp())
""")

print("\n=== After deduplication MERGE (E001 not duplicated, E003 added) ===")
spark.sql("SELECT event_id, event_type, user_id FROM default.events ORDER BY event_id").show()


# ── BLOCK 6: OPTIMIZE and Z-ORDER ────────────────────────────
# Compact small files and cluster data for faster queries

print("\n=== Before OPTIMIZE ===")
spark.sql("""
    DESCRIBE DETAIL default.customers_target
""").select("numFiles", "sizeInBytes").show()

# OPTIMIZE with Z-ORDER by the most common filter column
spark.sql("""
    OPTIMIZE default.customers_target
    ZORDER BY (customer_id)
""")

print("\n=== After OPTIMIZE ===")
spark.sql("""
    DESCRIBE DETAIL default.customers_target
""").select("numFiles", "sizeInBytes").show()

# OBSERVE: numFiles decreases — small files compacted into fewer larger ones
# sizeInBytes may also change due to better compression in larger files

print("\n=== History shows OPTIMIZE as a version ===")
spark.sql("""
    DESCRIBE HISTORY default.customers_target
""").select("version", "timestamp", "operation").show(5)


# ── BLOCK 7: VACUUM ──────────────────────────────────────────
# Remove files no longer referenced by active versions

# DRY RUN first — see what would be deleted without deleting anything
print("\n=== VACUUM DRY RUN ===")
spark.sql("""
    VACUUM default.customers_target DRY RUN
""").show(truncate=False)

# Run VACUUM with default 7-day retention
spark.sql("""
    VACUUM default.customers_target
""")

print("\n=== VACUUM complete ===")
print("Files outside 7-day retention window removed")
print("(None in this lab — all files are fresh)")

# Verify table still works correctly after VACUUM
print("\n=== Table intact after VACUUM ===")
spark.sql("""
    SELECT customer_id, name, status
    FROM default.customers_target
    ORDER BY customer_id
""").show()