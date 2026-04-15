# Databricks notebook source
# ============================================================
# Week 2 · Section 6 — Medallion Architecture Bronze/Silver/Gold
# End-to-end mini pipeline: raw orders → Bronze → Silver → Gold
# Run in Databricks — spark object pre-created automatically
# ============================================================


# ── BLOCK 1: Setup — create the schema ───────────────────────
# In production each layer lives in a separate schema or catalog
# Here we use a naming convention to keep the lab self-contained

spark.sql("DROP TABLE IF EXISTS default.bronze_orders")
spark.sql("DROP TABLE IF EXISTS default.silver_orders")
spark.sql("DROP TABLE IF EXISTS default.gold_revenue_by_region")
spark.sql("DROP TABLE IF EXISTS default.silver_orders_quarantine")

print("=== Setup complete — all tables dropped ===")




# COMMAND ----------

# ── BLOCK 2: Bronze — raw ingestion ──────────────────────────

from pyspark.sql.functions import to_timestamp
# Bronze captures exactly what arrived — raw payload + metadata
# No transformation, no parsing, no cleaning
# Append-only

spark.sql("""
    CREATE TABLE default.bronze_orders (
        ingestion_timestamp TIMESTAMP,
        source_file         STRING,
        raw_payload         STRING
    ) USING DELTA
""")

# Simulate three batches of raw JSON arriving at different times
# Batch 1 — clean records
batch1 = [
    ("2024-01-15T08:00:00", "orders_20240115.json",
     '{"order_id":1,"customer_id":"C001","amount":999.99,"region":"North","status":"completed","order_date":"2024-01-15"}'),
    ("2024-01-15T08:00:00", "orders_20240115.json",
     '{"order_id":2,"customer_id":"C002","amount":699.99,"region":"South","status":"completed","order_date":"2024-01-15"}'),
    ("2024-01-15T08:00:00", "orders_20240115.json",
     '{"order_id":3,"customer_id":"C001","amount":499.99,"region":"North","status":"pending","order_date":"2024-01-15"}'),
]
# Batch 2 — includes a duplicate of order 1 (at-least-once delivery)
batch2 = [
    ("2024-01-16T08:00:00", "orders_20240116.json",
     '{"order_id":1,"customer_id":"C001","amount":999.99,"region":"North","status":"completed","order_date":"2024-01-15"}'),
    ("2024-01-16T08:00:00", "orders_20240116.json",
     '{"order_id":4,"customer_id":"C003","amount":299.99,"region":"East","status":"completed","order_date":"2024-01-16"}'),
    ("2024-01-16T08:00:00", "orders_20240116.json",
     '{"order_id":5,"customer_id":"C002","amount":-50.00,"region":"South","status":"refund","order_date":"2024-01-16"}'),
]

# Batch 3 — includes a bad record with null customer_id
batch3 = [
    ("2024-01-17T08:00:00", "orders_20240117.json",
     '{"order_id":6,"customer_id":"C003","amount":149.99,"region":"East","status":"completed","order_date":"2024-01-17"}'),
    ("2024-01-17T08:00:00", "orders_20240117.json",
     '{"order_id":7,"customer_id":null,"amount":399.99,"region":"West","status":"completed","order_date":"2024-01-17"}'),
]

from pyspark.sql.functions import col, from_json, schema_of_json, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Load all batches into Bronze — append only
for batch in [batch1, batch2, batch3]:
    df = spark.createDataFrame(
        batch,
        ["ingestion_timestamp", "source_file", "raw_payload"]
    ).withColumn("ingestion_timestamp",to_timestamp(col("ingestion_timestamp")))
    df.write.format("delta").mode("append").saveAsTable("default.bronze_orders")

print("\n=== Bronze table — raw payloads preserved ===")
spark.sql("""
    SELECT ingestion_timestamp, source_file, raw_payload
    FROM default.bronze_orders
    ORDER BY ingestion_timestamp, raw_payload
""").show(truncate=False)

print(f"\nBronze row count: {spark.sql('SELECT COUNT(*) FROM default.bronze_orders').collect()[0][0]}")
print("Note: order_id 1 appears TWICE — duplicate from at-least-once delivery")

# COMMAND ----------

# ── BLOCK 3: Bronze → Silver promotion ───────────────────────
# Parse raw JSON, enforce schema, deduplicate, quarantine bad records

from pyspark.sql.functions import get_json_object, to_timestamp, when

# Parse raw_payload JSON into typed columns
bronze_parsed = spark.sql("""
    SELECT
        CAST(get_json_object(raw_payload, '$.order_id')     AS INT)       AS order_id,
        get_json_object(raw_payload, '$.customer_id')                      AS customer_id,
        CAST(get_json_object(raw_payload, '$.amount')       AS DOUBLE)    AS amount,
        get_json_object(raw_payload, '$.region')                           AS region,
        get_json_object(raw_payload, '$.status')                           AS status,
        CAST(get_json_object(raw_payload, '$.order_date')   AS DATE)      AS order_date,
        ingestion_timestamp,
        source_file
    FROM default.bronze_orders
""")

print("\n=== Parsed Bronze records (before quality checks) ===")
bronze_parsed.show()

# Split into valid and invalid records
valid_records = bronze_parsed.filter(
    (col("customer_id").isNotNull()) &
    (col("amount") > 0) &
    (col("order_id").isNotNull())
)

invalid_records = bronze_parsed.filter(
    (col("customer_id").isNull()) |
    (col("amount") <= 0) |
    (col("order_id").isNull())
)
valid_records = valid_records.drop_duplicates(["order_id"])
print(f"\nValid records: {valid_records.count()}")
print(f"Invalid/quarantined records: {invalid_records.count()}")

# Write invalid records to quarantine table for investigation
invalid_records.write.format("delta") \
               .mode("overwrite") \
               .saveAsTable("default.silver_orders_quarantine")

print("\n=== Quarantine table (bad records separated) ===")
spark.sql("""
    SELECT order_id, customer_id, amount, region, status
    FROM default.silver_orders_quarantine
""").show()

# Create Silver table
spark.sql("""
    CREATE TABLE default.silver_orders (
        order_id            INT,
        customer_id         STRING,
        amount              DOUBLE,
        region              STRING,
        status              STRING,
        order_date          DATE,
        ingestion_timestamp TIMESTAMP,
        source_file         STRING
    ) USING DELTA
""")

# MERGE valid records into Silver — deduplication on order_id
valid_records.createOrReplaceTempView("valid_orders_staging")

spark.sql("""
    MERGE INTO default.silver_orders t
    USING valid_orders_staging s
    ON t.order_id = s.order_id

    WHEN NOT MATCHED
        THEN INSERT *
""")

print("\n=== Silver table (deduplicated, validated) ===")
spark.sql("""
    SELECT order_id, customer_id, amount, region, status, order_date
    FROM default.silver_orders
    ORDER BY order_id
""").show()

print(f"\nSilver row count: {spark.sql('SELECT COUNT(*) FROM default.silver_orders').collect()[0][0]}")
print("Note: order_id 1 appears ONCE — duplicate removed by MERGE")
print("Note: order_id 7 (null customer) is in quarantine — not in Silver")
print("Note: order_id 5 (negative amount) is in quarantine — not in Silver")

# COMMAND ----------

# ── BLOCK 4: Silver → Gold aggregation ───────────────────────
# Gold is purpose-built for a specific business question
# Here: daily revenue by region — for BI dashboards

spark.sql("""
    CREATE TABLE default.gold_revenue_by_region
    USING DELTA
    AS
    SELECT
        region,
        order_date,
        COUNT(order_id)  AS order_count,
        SUM(amount)      AS total_revenue,
        AVG(amount)      AS avg_order_value,
        MAX(amount)      AS max_order_value
    FROM default.silver_orders
    WHERE status = 'completed'
    GROUP BY region, order_date
    ORDER BY order_date, region
""")

print("\n=== Gold — daily revenue by region ===")
spark.sql("""
    SELECT region, order_date, order_count,
           ROUND(total_revenue, 2)   AS total_revenue,
           ROUND(avg_order_value, 2) AS avg_order_value
    FROM default.gold_revenue_by_region
    ORDER BY order_date, region
""").show()


# COMMAND ----------

# ── BLOCK 5: Layer verification ───────────────────────────────
# Confirm each layer has the right row counts and properties

print("\n=== Layer summary ===")
bronze_count = spark.sql("SELECT COUNT(*) FROM default.bronze_orders").collect()[0][0]
silver_count = spark.sql("SELECT COUNT(*) FROM default.silver_orders").collect()[0][0]
gold_count   = spark.sql("SELECT COUNT(*) FROM default.gold_revenue_by_region").collect()[0][0]
quarantine_count = spark.sql("SELECT COUNT(*) FROM default.silver_orders_quarantine").collect()[0][0]

print(f"Bronze rows:     {bronze_count}  (raw, includes duplicates and bad records)")
print(f"Silver rows:     {silver_count}  (deduplicated, validated — 8 raw - 1 dup - 2 bad = 5)")
print(f"Gold rows:       {gold_count}   (aggregated by region and date)")
print(f"Quarantine rows: {quarantine_count}  (2 bad records: null customer + negative amount)")

# Verify Silver has no duplicates
print("\n=== Silver deduplication check (should show no duplicate order_ids) ===")
spark.sql("""
    SELECT order_id, COUNT(*) as count
    FROM default.silver_orders
    GROUP BY order_id
    HAVING count > 1
""").show()

# Verify quarantine has the expected bad records
print("\n=== Quarantine contents (null customer and negative amount) ===")
spark.sql("""
    SELECT order_id, customer_id, amount, status
    FROM default.silver_orders_quarantine
    ORDER BY order_id
""").show()

# COMMAND ----------

# ── BLOCK 6: SCD Type 2 — customer dimension ─────────────────
# Demonstrates how slowly changing dimensions work in the Lakehouse

spark.sql("DROP TABLE IF EXISTS default.dim_customers")

spark.sql("""
    CREATE TABLE default.dim_customers (
        customer_id STRING,
        name        STRING,
        address     STRING,
        city        STRING,
        valid_from  DATE,
        valid_to    DATE,
        is_current  BOOLEAN
    ) USING DELTA
""")

# Initial customer records
spark.sql("""
    INSERT INTO default.dim_customers VALUES
    ('C001', 'Alice Smith', '123 Oak Ave',   'Boston',   '2020-01-01', NULL, true),
    ('C002', 'Bob Jones',   '99 Jump St',    'New York',  '2020-01-01', NULL, true),
    ('C003', 'Carol White', '1000 Rodeo Dr', 'Austin',   '2020-01-01', NULL, true)
""")

print("\n=== Initial customer dimension (all current) ===")
spark.sql("SELECT * FROM default.dim_customers ORDER BY customer_id").show()

# Alice moves — SCD Type 2 update
# Step 1: expire the current record
spark.sql("""
    UPDATE default.dim_customers
    SET valid_to   = '2024-01-20',
        is_current = false
    WHERE customer_id = 'C001'
    AND   is_current  = true
""")

# Step 2: insert the new current record
spark.sql("""
    INSERT INTO default.dim_customers VALUES
    ('C001', 'Alice Smith', '430 River Rd', 'Cambridge', '2024-01-20', NULL, true)
""")

print("\n=== Customer dimension after Alice moves (SCD Type 2) ===")
spark.sql("""
    SELECT customer_id, name, address, city, valid_from, valid_to, is_current
    FROM default.dim_customers
    ORDER BY customer_id, valid_from
""").show()

# Query: what was Alice's address when order 1 was placed (2024-01-15)?
print("\n=== Alice's address at time of order 1 (2024-01-15) ===")
spark.sql("""
    SELECT o.order_id, o.order_date, c.name, c.address, c.city
    FROM default.silver_orders o
    JOIN default.dim_customers c
      ON o.customer_id = c.customer_id
     AND o.order_date BETWEEN c.valid_from AND COALESCE(c.valid_to, '9999-12-31')
    WHERE o.order_id = 1
""").show()

print("\n=== Alice's CURRENT address ===")
spark.sql("""
    SELECT customer_id, name, address, city
    FROM default.dim_customers
    WHERE customer_id = 'C001'
    AND   is_current  = true
""").show()

# OBSERVE: order 1 (placed on 2024-01-15) shows the OLD address (123 Oak Ave)
#          the current dimension shows the NEW address (430 River Rd)
#          SCD Type 2 preserves history — both are correct for their context
