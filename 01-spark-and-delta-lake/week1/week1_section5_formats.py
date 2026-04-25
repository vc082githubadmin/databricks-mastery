# ============================================================
# Week 1 · Section 5 — Reading & Writing Data Formats
# Reference code — understand each block, then type it yourself
# ============================================================

# ── BLOCK 1: SparkSession Setup ──────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum, to_date,
    regexp_replace, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, LongType, DoubleType, DateType
)
from delta import configure_spark_with_delta_pip
import os

builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("Week1-S5: Reading & Writing Formats")
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
print(f"Spark version: {spark.version}\n")

# ── BLOCK 2: Create Sample Data Files ────────────────────────
# We generate sample CSV and JSON files on disk first
# so we can demonstrate reading them in subsequent blocks.
# In production these files would come from source systems.

os.makedirs("files/raw", exist_ok=True)

# Write a pipe-delimited CSV with real-world formatting issues:
# - Pipe delimiter instead of comma
# - N/A for null values
# - Dates in dd/MM/yyyy format
# - Amount with comma formatting (the tricky one)
csv_content = """emp_id|name|dept|salary|hire_date|bonus
EMP-001|Alice Smith|Engineering|95000|15/03/2019|12,500.00
EMP-002|Bob Jones|Marketing|72000|22/07/2020|N/A
EMP-003|Carol White|Engineering|105000|01/01/2018|18,750.00
EMP-004|Dave Brown|Marketing|68000|30/11/2021|N/A
EMP-005|Eve Davis|Engineering|88000|14/06/2019|11,000.00
EMP-006|Frank Miller|HR|65000|05/09/2022|N/A"""

with open("files/raw/employees.csv", "w") as f:
    f.write(csv_content)

# Write a JSON file — one JSON object per line (standard)
json_content = """{"order_id": 1001, "customer_id": "C-001", "product": "Laptop", "amount": 999.99, "order_date": "2024-01-15", "status": "completed"}
{"order_id": 1002, "customer_id": "C-002", "product": "Phone", "amount": 699.99, "order_date": "2024-01-16", "status": "pending"}
{"order_id": 1003, "customer_id": "C-001", "product": "Tablet", "amount": 499.99, "order_date": "2024-01-17", "status": "completed"}
{"order_id": 1004, "customer_id": "C-003", "product": "Laptop", "amount": 999.99, "order_date": "2024-01-18", "status": "cancelled"}
{"order_id": 1005, "customer_id": "C-002", "product": "Headphones", "amount": 199.99, "order_date": "2024-01-19", "status": "completed"}"""

with open("files/raw/orders.json", "w") as f:
    f.write(json_content)

print("=== Sample files created ===")
print("files/raw/employees.csv")
print("files/raw/orders.json\n")


# ── BLOCK 3: Reading CSV — With All Options ───────────────────
# Demonstrate reading the tricky pipe-delimited CSV
# with all the options needed for production use.

spark.sparkContext.setJobDescription("Block3: read CSV")

# Step 1 — First, read with inferSchema to see what Spark guesses
# This is for illustration only — we'll replace with explicit schema
print("=== CSV read with inferSchema (what Spark guesses) ===")
df_csv_inferred = (
    spark.read
    .format("csv")
    .option("header",      "true")
    .option("sep",         "|")
    .option("nullValue",   "N/A")
    .option("inferSchema", "true")   # guessing — never in production
    .load("files/raw/employees.csv")
)
df_csv_inferred.printSchema()
df_csv_inferred.show()

# OBSERVE: bonus column is read as String because of "12,500.00" formatting
# hire_date is read as String because format doesn't match default
# salary is read correctly as integer — simple numeric value

# Step 2 — Explicit schema + handle the bonus column properly
# bonus is read as String first, then cleaned and cast
csv_schema = StructType([
    StructField("emp_id",    StringType(),  nullable=False),
    StructField("name",      StringType(),  nullable=False),
    StructField("dept",      StringType(),  nullable=False),
    StructField("salary",    LongType(),    nullable=False),
    StructField("hire_date", StringType(),  nullable=True),  # read as string first
    StructField("bonus",     StringType(),  nullable=True),  # read as string, clean later
])

df_csv = (
    spark.read
    .format("csv")
    .option("header",      "true")
    .option("sep",         "|")
    .option("nullValue",   "N/A")
    .option("inferSchema", "false")  # explicit schema — production standard
    .schema(csv_schema)
    .load("files/raw/employees.csv")
)

# Step 3 — Clean the bonus column: strip commas, cast to Double
# This is the standard pattern for financial data with formatted numbers
df_csv_clean = df_csv.withColumn(
    "bonus_amount",
    regexp_replace(col("bonus"), ",", "").cast(DoubleType())
).withColumn(
    "hire_date_parsed",
    to_date(col("hire_date"), "dd/MM/yyyy")   # parse with correct format
).drop("bonus", "hire_date")  # drop raw string versions

print("\n=== CSV read with explicit schema + cleaned columns ===")
df_csv_clean.printSchema()
df_csv_clean.show()

# OBSERVE:
# bonus_amount is now DoubleType — null where original was N/A
# hire_date_parsed is DateType — correctly parsed from dd/MM/yyyy format


# ── BLOCK 4: Reading JSON ─────────────────────────────────────

spark.sparkContext.setJobDescription("Block4: read JSON")

# JSON schema — explicit as always
orders_schema = StructType([
    StructField("order_id",    IntegerType(), nullable=False),
    StructField("customer_id", StringType(),  nullable=False),
    StructField("product",     StringType(),  nullable=False),
    StructField("amount",      DoubleType(),  nullable=False),
    StructField("order_date",  StringType(),  nullable=False),
    StructField("status",      StringType(),  nullable=False),
])

df_json = (
    spark.read
    .format("json")
    .schema(orders_schema)
    .load("files/raw/orders.json")
)

print("\n=== JSON read with explicit schema ===")
df_json.printSchema()
df_json.show()

# Convert order_date string to DateType
df_json_clean = df_json.withColumn(
    "order_date",
    to_date(col("order_date"), "yyyy-MM-dd")
)

print("\n=== JSON with parsed date ===")
df_json_clean.show()


# ── BLOCK 5: Writing and Reading Parquet ──────────────────────

spark.sparkContext.setJobDescription("Block5: write and read Parquet")

# Write to Parquet
df_csv_clean.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("files/employees_parquet")

# Read back — schema is embedded in the file, no schema needed
df_parquet = spark.read.format("parquet").load("files/employees_parquet")

print("\n=== Parquet read-back ===")
df_parquet.printSchema()  # OBSERVE: schema preserved automatically
df_parquet.show()

# Count files written — no coalesce so one file per partition
parquet_files = [
    f for f in os.listdir("files/employees_parquet")
    if f.endswith(".parquet")
]
print(f"Parquet files written: {len(parquet_files)}")
# Small dataset — likely 1-2 files given partition count


# ── BLOCK 6: Writing and Reading Delta ───────────────────────

spark.sparkContext.setJobDescription("Block6: write and read Delta")

# Write to Delta
df_json_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .save("files/orders_delta")

# Read back
df_delta = spark.read.format("delta").load("files/orders_delta")

print("\n=== Delta read-back ===")
df_delta.printSchema()
df_delta.show()

# Inspect the Delta transaction log
print("\n=== Delta transaction log files ===")
log_files = os.listdir("files/orders_delta/_delta_log")
for f in sorted(log_files):
    print(f"  {f}")
# OBSERVE: _delta_log folder created automatically
# Contains JSON files recording every write operation
# This is what enables time travel and ACID transactions


# ── BLOCK 7: partitionBy() on Write ──────────────────────────
# Demonstrate how partitionBy creates folder structure on disk
# enabling partition pruning on reads.

spark.sparkContext.setJobDescription("Block7: partitionBy on write")

# Add a region column to orders for partitioning demonstration
from pyspark.sql.functions import when

df_orders_with_region = df_json_clean.withColumn(
    "region",
    when(col("customer_id") == "C-001", "North")
    .when(col("customer_id") == "C-002", "South")
    .otherwise("East")
)

# Write partitioned by region
df_orders_with_region.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("region") \
    .save("files/orders_partitioned")

print("\n=== partitionBy('region') folder structure ===")
for item in sorted(os.listdir("files/orders_partitioned")):
    if not item.startswith("_"):
        print(f"  {item}/")
        region_path = f"files/orders_partitioned/{item}"
        if os.path.isdir(region_path):
            for f in os.listdir(region_path):
                if f.endswith(".parquet"):
                    print(f"    {f}")

# Read with partition filter — Spark only reads matching folder
df_north = spark.read.format("delta") \
    .load("files/orders_partitioned") \
    .filter(col("region") == "North")

print("\n=== Read with partition filter (region = North) ===")
df_north.show()
# OBSERVE: Spark reads ONLY the region=North folder
# All other region folders are completely skipped — partition pruning


# ── BLOCK 8: Format Comparison ───────────────────────────────
# Write the same dataset in three formats and compare file sizes.

spark.sparkContext.setJobDescription("Block8: format comparison")

import random
random.seed(42)

# Generate a larger dataset for meaningful size comparison
large_data = [
    (i,
     f"EMP-{random.randint(1,100)}",
     random.choice(["Engineering","Marketing","HR","Finance","Sales"]),
     random.randint(50000, 150000),
     random.choice(["North","South","East","West"]))
    for i in range(10000)
]

large_schema = StructType([
    StructField("id",     IntegerType(), nullable=False),
    StructField("emp_id", StringType(),  nullable=False),
    StructField("dept",   StringType(),  nullable=False),
    StructField("salary", LongType(),    nullable=False),
    StructField("region", StringType(),  nullable=False),
])

df_large = spark.createDataFrame(large_data, large_schema)

# Write in all three formats
df_large.coalesce(1).write.format("csv") \
    .option("header", "true").mode("overwrite") \
    .save("files/comparison_csv")

df_large.coalesce(1).write.format("parquet") \
    .mode("overwrite").save("files/comparison_parquet")

df_large.coalesce(1).write.format("delta") \
    .mode("overwrite").save("files/comparison_delta")

# Calculate sizes
def get_dir_size(path):
    total = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            total += os.path.getsize(fp)
    return total

csv_size     = get_dir_size("files/comparison_csv")
parquet_size = get_dir_size("files/comparison_parquet")
delta_size   = get_dir_size("files/comparison_delta")

print("\n=== Format Size Comparison (10,000 rows) ===")
print(f"CSV:     {csv_size:,} bytes  ({csv_size/1024:.1f} KB)")
print(f"Parquet: {parquet_size:,} bytes  ({parquet_size/1024:.1f} KB)")
print(f"Delta:   {delta_size:,} bytes  ({delta_size/1024:.1f} KB)")
print(f"\nParquet compression vs CSV: "
      f"{(1 - parquet_size/csv_size)*100:.1f}% smaller")
# OBSERVE: Parquet is significantly smaller than CSV
# Delta is slightly larger than Parquet due to transaction log overhead
# At scale (TBs) the compression saving is enormous


# ── BLOCK 9: Reading Multiple Files ──────────────────────────
# Production pipelines read entire directories, not single files.
# Spark reads all matching files in a directory as one DataFrame.

spark.sparkContext.setJobDescription("Block9: reading directories")

# Create multiple JSON files simulating daily drops
os.makedirs("files/raw/daily_orders", exist_ok=True)

days = {
    "2024-01-15": [
        '{"order_id": 2001, "amount": 150.0, "region": "North"}',
        '{"order_id": 2002, "amount": 200.0, "region": "South"}'
    ],
    "2024-01-16": [
        '{"order_id": 2003, "amount": 300.0, "region": "East"}',
        '{"order_id": 2004, "amount": 175.0, "region": "North"}'
    ],
    "2024-01-17": [
        '{"order_id": 2005, "amount": 450.0, "region": "West"}'
    ]
}

for date, records in days.items():
    with open(f"files/raw/daily_orders/orders_{date}.json", "w") as f:
        f.write("\n".join(records))

# Read the ENTIRE directory in one call — Spark reads all files
df_all_days = spark.read.format("json") \
    .load("files/raw/daily_orders/")  # note: directory path, not filename

print("\n=== Reading entire directory (all daily files) ===")
df_all_days.show()
print(f"Total records across all files: {df_all_days.count()}")

# Read with wildcard — select specific files matching a pattern
df_jan15_16 = spark.read.format("json") \
    .load("files/raw/daily_orders/orders_2024-01-1[56].json")

print("\n=== Reading with wildcard (Jan 15 and 16 only) ===")
df_jan15_16.show()


input("\n>>> All blocks complete. Check Spark UI. Press ENTER to stop.")
spark.stop()