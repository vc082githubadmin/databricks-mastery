# ============================================================
# Week 2 · Section 4 — Delta Change Data Feed (CDF)
# Run in Databricks — spark object pre-created automatically
# ============================================================


# ── BLOCK 1: Create table with CDF enabled ───────────────────
# CDF must be enabled explicitly — it is off by default
# Only captures changes AFTER it is enabled

spark.sql("DROP TABLE IF EXISTS default.products_cdf")

spark.sql("""
    CREATE TABLE default.products_cdf (
        product_id   INT,
        name         STRING,
        price        DOUBLE,
        category     STRING,
        in_stock     BOOLEAN
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Insert initial data — version 1
spark.sql("""
    INSERT INTO default.products_cdf VALUES
    (1, 'Laptop',     999.99, 'Electronics', true),
    (2, 'Phone',      699.99, 'Electronics', true),
    (3, 'Tablet',     499.99, 'Electronics', true),
    (4, 'Headphones', 199.99, 'Electronics', true),
    (5, 'Keyboard',    79.99, 'Accessories', true)
""")

print("=== Initial products table ===")
spark.sql("SELECT * FROM default.products_cdf ORDER BY product_id").show()

# Verify CDF is enabled
print("\n=== Table properties — CDF enabled ===")
spark.sql("""
    DESCRIBE DETAIL default.products_cdf
""").select("name", "properties").show(truncate=False)


# ── BLOCK 2: Make changes — generate CDF records ─────────────
# Each operation type produces different _change_type values
# INSERT  → insert
# UPDATE  → update_preimage + update_postimage
# DELETE  → delete

# Version 2 — price update for Laptop
spark.sql("""
    UPDATE default.products_cdf
    SET price = 1099.99
    WHERE product_id = 1
""")

# Version 3 — mark Phone as out of stock
spark.sql("""
    UPDATE default.products_cdf
    SET in_stock = false
    WHERE product_id = 2
""")

# Version 4 — delete Keyboard (discontinued)
spark.sql("""
    DELETE FROM default.products_cdf
    WHERE product_id = 5
""")

# Version 5 — insert two new products
spark.sql("""
    INSERT INTO default.products_cdf VALUES
    (6, 'Monitor',    399.99, 'Electronics', true),
    (7, 'Webcam',      89.99, 'Accessories', true)
""")

print("\n=== Current table state after all changes ===")
spark.sql("""
    SELECT * FROM default.products_cdf ORDER BY product_id
""").show()

print("\n=== Transaction history ===")
spark.sql("""
    DESCRIBE HISTORY default.products_cdf
""").select("version", "timestamp", "operation").show()


# ── BLOCK 3: Read CDF in batch mode ──────────────────────────
# Read all changes from version 1 onwards
# Each changed row appears with _change_type, _commit_version, _commit_timestamp

print("\n=== CDF batch read — all changes from version 1 ===")
cdf_df = (spark.read
               .format("delta")
               .option("readChangeFeed", "true")
               .option("startingVersion", 1)
               .table("default.products_cdf"))

cdf_df.select(
    "product_id",
    "name",
    "price",
    "in_stock",
    "_change_type",
    "_commit_version"
).orderBy("_commit_version", "product_id").show(20)

# OBSERVE:
# UPDATE on Laptop → 2 rows: update_preimage (old price) + update_postimage (new price)
# UPDATE on Phone  → 2 rows: update_preimage (in_stock=true) + update_postimage (in_stock=false)
# DELETE on Keyboard → 1 row: delete
# INSERT of Monitor + Webcam → 2 rows: insert + insert


# ── BLOCK 4: Filter by change type ───────────────────────────
# Downstream consumers typically filter on _change_type to route changes

print("\n=== Only UPDATE postimages (current values after updates) ===")
cdf_df.filter("_change_type = 'update_postimage'") \
      .select("product_id", "name", "price", "in_stock", "_commit_version") \
      .show()

print("\n=== Only DELETEs (what was removed) ===")
cdf_df.filter("_change_type = 'delete'") \
      .select("product_id", "name", "price", "_commit_version") \
      .show()

print("\n=== Only INSERTs (new rows added) ===")
cdf_df.filter("_change_type = 'insert'") \
      .select("product_id", "name", "price", "_commit_version") \
      .show()

# OBSERVE: update_preimage is used when you need to know what changed FROM
#          update_postimage is used when you need the current/new value
#          delete shows exactly what was in the row before it was removed


# ── BLOCK 5: CDF between specific versions ───────────────────
# Read only changes between version 2 and version 3
# Useful for incremental processing — only process what changed since last run

print("\n=== CDF between version 2 and version 3 only ===")
cdf_v2_v3 = (spark.read
                  .format("delta")
                  .option("readChangeFeed", "true")
                  .option("startingVersion", 2)
                  .option("endingVersion", 3)
                  .table("default.products_cdf"))

cdf_v2_v3.select(
    "product_id",
    "name",
    "price",
    "in_stock",
    "_change_type",
    "_commit_version"
).orderBy("_commit_version").show()

# OBSERVE: only shows changes from versions 2 and 3
# Changes from versions 4 and 5 are excluded


# ── BLOCK 6: CDF streaming read ──────────────────────────────
# Stream changes from products_cdf into a summary table
# CDF solves stream composability — UPDATEs and DELETEs are safe to stream

spark.sql("DROP TABLE IF EXISTS default.products_changes_log")

# Read CDF as a stream — only processes new changes since last checkpoint
stream_query = (spark.readStream
                     .format("delta")
                     .option("readChangeFeed", "true")
                     .option("startingVersion", 1)
                     .table("default.products_cdf")
                     .writeStream
                     .format("delta")
                     .option("checkpointLocation",
                             "dbfs:/tmp/checkpoints/products_cdf")
                     .outputMode("append")
                     .toTable("default.products_changes_log"))

import time
time.sleep(10)
stream_query.stop()

print("\n=== Products changes log (streamed via CDF) ===")
spark.sql("""
    SELECT product_id, name, price, in_stock,
           _change_type, _commit_version
    FROM default.products_changes_log
    ORDER BY _commit_version, product_id
""").show(20)


# ── BLOCK 7: CDF for downstream propagation ──────────────────
# Common pattern: use CDF to propagate only changes to a downstream table
# Instead of reprocessing the entire source table on every run

spark.sql("DROP TABLE IF EXISTS default.electronics_summary")

# Create a summary table — only Electronics category
spark.sql("""
    CREATE TABLE default.electronics_summary
    USING DELTA
    AS SELECT product_id, name, price, in_stock
       FROM default.products_cdf
       WHERE category = 'Electronics'
""")

print("\n=== Initial electronics summary ===")
spark.sql("SELECT * FROM default.electronics_summary ORDER BY product_id").show()

# Now use CDF to get only the changes to Electronics products
# and apply them to the summary table using MERGE

electronics_changes = (spark.read
                            .format("delta")
                            .option("readChangeFeed", "true")
                            .option("startingVersion", 2)
                            .table("default.products_cdf")
                            .filter("category = 'Electronics'"))

electronics_changes.createOrReplaceTempView("electronics_cdf")

# Apply updates and deletes from CDF to the summary table
spark.sql("""
    MERGE INTO default.electronics_summary t
    USING (
        SELECT product_id, name, price, in_stock, _change_type
        FROM electronics_cdf
        WHERE _change_type IN ('update_postimage', 'insert', 'delete')
    ) s
    ON t.product_id = s.product_id

    WHEN MATCHED AND s._change_type = 'delete'
        THEN DELETE

    WHEN MATCHED AND s._change_type = 'update_postimage'
        THEN UPDATE SET
            t.price    = s.price,
            t.in_stock = s.in_stock

    WHEN NOT MATCHED AND s._change_type = 'insert'
        THEN INSERT (product_id, name, price, in_stock)
             VALUES (s.product_id, s.name, s.price, s.in_stock)
""")

print("\n=== Electronics summary after CDF-driven MERGE ===")
print("Expected: Laptop price updated, Phone in_stock=false, Monitor added")
spark.sql("""
    SELECT * FROM default.electronics_summary ORDER BY product_id
""").show()

# OBSERVE: only changed rows were processed — not the full source table
# This is incremental processing — the efficiency gain at scale is enormous