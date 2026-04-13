# Databricks notebook source
print(spark.version)
print(spark.sparkContext.master)
print(spark.sparkContext.appName)

# COMMAND ----------

# spark.version works on serverless
print(f"Spark version: {spark.version}")

# Use spark.conf instead of sparkContext for config values
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions', 'default')}")
print(f"Adaptive enabled: {spark.conf.get('spark.sql.adaptive.enabled', 'default')}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user(), current_timestamp()

# COMMAND ----------

# Create a simple Delta table directly in Databricks
data = [(1, "Alice", "Engineering", 95000),
        (2, "Bob", "Marketing", 72000),
        (3, "Carol", "Engineering", 105000)]

df = spark.createDataFrame(data, ["id", "name", "dept", "salary"])

# Write as Delta table to Unity Catalog
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.employees_demo")

print("Table created successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.employees_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL default.employees_demo

# COMMAND ----------

# This is a Python cell
employees = spark.table("default.employees_demo")
employees.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## My First Databricks Notebook
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC - The `spark` object is pre-created
# MAGIC - Magic commands switch languages per cell
# MAGIC - `saveAsTable()` registers tables in Unity Catalog
# MAGIC - `DESCRIBE DETAIL` shows Delta table metadata