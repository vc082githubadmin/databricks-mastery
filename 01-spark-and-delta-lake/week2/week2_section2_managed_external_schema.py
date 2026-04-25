# Databricks notebook source
# ============================================================
# Week 2 · Section 2 — Managed vs External Tables & Schema Evolution
# Run in Databricks — spark object pre-created automatically
# ============================================================


# ── BLOCK 1: Managed Table ───────────────────────────────────
# Databricks controls both metadata AND data files
# DROP TABLE removes everything — metadata and data files

spark.sql("DROP TABLE IF EXISTS default.orders_managed")

data = [(1, "Alice", 500.0, "completed"),
        (2, "Bob",   750.0, "pending"),
        (3, "Carol", 300.0, "completed")]

df = spark.createDataFrame(
    data, ["order_id", "customer_id", "amount", "status"]
)

df.write.format("delta") \
       .mode("overwrite") \
       .saveAsTable("default.orders_managed")

# Check where Databricks stored the data files
print("=== Managed Table Location ===")
spark.sql("""
    DESCRIBE DETAIL default.orders_managed
""").select("name", "location", "format").show(truncate=False)

# OBSERVE: location is controlled by Databricks
# tableType = MANAGED
# You did NOT choose this path — Databricks did

# COMMAND ----------

# ── BLOCK 2: External Table ──────────────────────────────────
# YOU control the data file location
# DROP TABLE removes only metadata — data files survive

spark.sql("DROP TABLE IF EXISTS default.orders_external")

# In enterprise Databricks, use your org's storage path
# Format: abfss://container@account.dfs.core.windows.net/path (Azure)
#         s3://bucket/path (AWS)
# For this lab we use a DBFS path to demonstrate the concept
# In production: always use cloud storage paths, not DBFS

EXTERNAL_PATH = "dbfs:/tmp/learning/orders_external"

df.write.format("delta") \
       .mode("overwrite") \
       .save(EXTERNAL_PATH)

# Register as external table pointing to our chosen path
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS default.orders_external
    USING DELTA
    LOCATION '{EXTERNAL_PATH}'
""")

print("\n=== External Table Location ===")
spark.sql("""
    DESCRIBE DETAIL default.orders_external
""").select("name", "location", "format").show(truncate=False)

# OBSERVE: location = the path YOU specified
# tableType = EXTERNAL
# You own this path — dropping the table leaves data intact

# COMMAND ----------

# ── BLOCK 3: DROP behaviour difference ───────────────────────
# This is the critical difference between managed and external

# Count rows before DROP
print("\n=== Before DROP — both tables have data ===")
print(f"Managed rows:  {spark.sql('SELECT COUNT(*) FROM default.orders_managed').collect()[0][0]}")
print(f"External rows: {spark.sql('SELECT COUNT(*) FROM default.orders_external').collect()[0][0]}")

# DROP the managed table — deletes metadata AND data files
spark.sql("DROP TABLE IF EXISTS default.orders_managed")

# DROP the external table — deletes metadata ONLY
spark.sql("DROP TABLE IF EXISTS default.orders_external")

print("\n=== After DROP ===")

# Try to read managed table — should fail (data files deleted)
try:
    spark.sql("SELECT COUNT(*) FROM default.orders_managed").show()
except Exception as e:
    print(f"Managed table query failed (expected): {type(e).__name__}")

# Try to read external data directly from path — should succeed
# Data files still exist even though the table was dropped
print("\n=== External data files still exist at path ===")
spark.read.format("delta").load(EXTERNAL_PATH).show()

# OBSERVE: managed table data is GONE after DROP
#          external data files SURVIVE the DROP
# This is the critical production difference

# Recreate external table pointing to the same path
# Data is immediately available again — no migration needed
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS default.orders_external
    USING DELTA
    LOCATION '{EXTERNAL_PATH}'
""")

print("\n=== External table recreated — data immediately available ===")
spark.sql("SELECT * FROM default.orders_external").show()


# COMMAND ----------

# ── BLOCK 4: Schema Enforcement ──────────────────────────────
# Delta rejects writes that don't match the table schema by default

spark.sql("DROP TABLE IF EXISTS default.products")

products_data = [(1, "Laptop", 999.99),
                 (2, "Phone",  699.99),
                 (3, "Tablet", 499.99)]

df_products = spark.createDataFrame(
    products_data, ["product_id", "name", "price"]
)

df_products.write.format("delta") \
           .mode("overwrite") \
           .saveAsTable("default.products")

print("\n=== Products table schema ===")
spark.sql("DESCRIBE default.products").show()

# Now try to write a DataFrame with an EXTRA column
df_with_extra = spark.createDataFrame(
    [(4, "Headphones", 199.99, "Electronics")],
    ["product_id", "name", "price", "category"]  # extra column!
)

print("\n=== Attempting write with extra column (should fail) ===")
try:
    df_with_extra.write.format("delta") \
                .mode("append") \
                .saveAsTable("default.products")
    print("Write succeeded (unexpected)")
except Exception as e:
    print(f"Write rejected by schema enforcement (expected)")
    print(f"Error type: {type(e).__name__}")

# OBSERVE: Delta rejected the write
# The table is unchanged — check the count
print(f"\nTable still has original 3 rows: {spark.sql('SELECT COUNT(*) FROM default.products').collect()[0][0]}")

# COMMAND ----------

# ── BLOCK 5: Schema Evolution with mergeSchema ───────────────
# Deliberately allow the schema to change by setting mergeSchema=true

print("\n=== Schema Evolution with mergeSchema=true ===")
df_with_extra.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("default.products")

print("Write succeeded with mergeSchema=true")

# Check the new schema — category column added
print("\n=== Updated schema (category column added) ===")
spark.sql("DESCRIBE default.products").show()

# Query all rows — historical rows show null for category
print("\n=== All rows — historical rows have null category ===")
spark.sql("SELECT * FROM default.products ORDER BY product_id").show()

# OBSERVE: product_id 1,2,3 have null category (written before evolution)
#          product_id 4 has category = "Electronics" (written after evolution)
# Old Parquet files NOT rewritten — null returned dynamically from metadata

# COMMAND ----------

# ── BLOCK 6: Schema Evolution with ALTER TABLE ───────────────
# Add columns explicitly with full control over name and type

spark.sql("""
    ALTER TABLE default.products 
    ADD COLUMNS(
        stock_count INT,
        supplier    STRING
    )
""")

print("\n=== Schema after ALTER TABLE ADD COLUMNS ===")
spark.sql("DESCRIBE default.products").show()

# All existing rows show null for new columns
print("\n=== All rows after ALTER TABLE — new columns are null ===")
spark.sql("SELECT product_id, name, price, stock_count, supplier FROM default.products ORDER BY product_id").show()

# OBSERVE: stock_count and supplier are null for all existing rows
# New writes will include these columns with actual values

# COMMAND ----------

# ── BLOCK 7: Schema Enforcement with Constraints ─────────────
# ADD CONSTRAINT enforces business rules — throws error on violation
# From B3: "Check NOT NULL or arbitrary boolean condition"

spark.sql("DROP TABLE IF EXISTS default.transactions")

spark.sql("""
    CREATE TABLE default.transactions (
        txn_id    INT,
        amount    DOUBLE,
        status    STRING
    ) USING DELTA
""")

# Add a constraint: amount must be positive
spark.sql("""
    ALTER TABLE default.transactions
    ADD CONSTRAINT positive_amount CHECK (amount > 0)
""")

# Insert valid data
spark.sql("""
    INSERT INTO default.transactions VALUES
    (1, 100.0, 'completed'),
    (2, 250.0, 'pending')
""")

print("\n=== Valid data inserted ===")
spark.sql("SELECT * FROM default.transactions").show()

# Try to insert invalid data — negative amount
print("\n=== Attempting insert with negative amount (should fail) ===")
try:
    spark.sql("""
        INSERT INTO default.transactions VALUES
        (3, -50.0, 'completed')
    """)
    print("Insert succeeded (unexpected)")
except Exception as e:
    print(f"Constraint violation caught (expected): {type(e).__name__}")

print(f"\nTable still has 2 valid rows: {spark.sql('SELECT COUNT(*) FROM default.transactions').collect()[0][0]}")

# Check what constraints exist on the table
print("\n=== Table constraints ===")
spark.sql("DESCRIBE DETAIL default.transactions") \
     .select("name", "properties") \
     .show(truncate=False)
