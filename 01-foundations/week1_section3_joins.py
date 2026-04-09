from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType
)
from delta import configure_spark_with_delta_pip

# Build the spark session
budiler = (
    SparkSession.builder
    .master("local[*]")
    .appName("Week1 - Section 3 - Joins")
    
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable auto-broadcast
                                                            # so we can see both
                                                            # join strategies clearly
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.ui.port", "4040")
)

spark = configure_spark_with_delta_pip(budiler).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print(f"\nSpark UI → {spark.sparkContext.uiWebUrl}")
print(f"Spark version: {spark.version}\n")

# WHY autoBroadcastJoinThreshold = -1?
# We disabled auto-broadcast so Spark is forced to use Sort-Merge Join
# for all joins. This lets us see the Exchange nodes in the query plan.
# Later we'll manually hint broadcast() to force Broadcast Hash Join
# and compare the two plans side by side.

# ── Employees DataFrame ──────────────────────────────────────
employees_schema = StructType([
    StructField("id",     IntegerType(), nullable=False),
    StructField("name",   StringType(),  nullable=False),
    StructField("dept",   StringType(),  nullable=False),
    StructField("salary", LongType(),    nullable=False),
    StructField("city",   StringType(),  nullable=False),
])

employees_data = [
    (1,  "Alice",   "Engineering", 95000,  "New York"),
    (2,  "Bob",     "Marketing",   72000,  "London"),
    (3,  "Carol",   "Engineering", 105000, "New York"),
    (4,  "Dave",    "Marketing",   68000,  "London"),
    (5,  "Eve",     "Engineering", 88000,  "Berlin"),
    (6,  "Frank",   "HR",          65000,  "New York"),
    (7,  "Grace",   "Engineering", 112000, "Berlin"),
    (8,  "Henry",   "HR",          61000,  "London"),
    (9,  "Ivy",     "Marketing",   75000,  "Berlin"),
    (10, "Jack",    "Engineering", 99000,  "New York"),
]

df_employees = spark.createDataFrame(employees_data, employees_schema)

# ── Departments DataFrame ────────────────────────────────────
# NOTICE: Finance has no employees — designed to test LEFT JOIN
# NOTICE: manager is nullable=True — real world has missing data
departments_schema = StructType([
    StructField("dept_id",   IntegerType(), nullable=False),
    StructField("dept_name", StringType(),  nullable=False),
    StructField("budget",    LongType(),    nullable=False),
    StructField("manager",   StringType(),  nullable=True),   # nullable!
])

departments_data = [
    (1, "Engineering", 500000, "Grace"),
    (2, "Marketing",   300000, "Ivy"),
    (3, "HR",          200000, "Henry"),
    (4, "Finance",     400000, None),   # No manager yet AND no employees
]

df_departments = spark.createDataFrame(departments_data, departments_schema)

# ── Projects DataFrame ───────────────────────────────────────
# NOTICE: "Research" dept in projects has no match in departments
# This tests what happens to unmatched rows in different join types
projects_schema = StructType([
    StructField("project_id",   IntegerType(), nullable=False),
    StructField("project_name", StringType(),  nullable=False),
    StructField("dept_name",    StringType(),  nullable=False),
    StructField("budget",       LongType(),    nullable=False),
])

projects_data = [
    (101, "Platform Rebuild", "Engineering", 150000),
    (102, "Brand Campaign",   "Marketing",   80000),
    (103, "Talent Pipeline",  "HR",          50000),
    (104, "Data Lake",        "Engineering", 200000),
    (105, "AI Initiative",    "Research",    300000),  # No matching dept!
]

df_projects = spark.createDataFrame(projects_data, projects_schema)

print("=== Employees ===")
df_employees.show()
print("=== Departments ===")
df_departments.show()
print("=== Projects ===")
df_projects.show()

# ── BLOCK 2: Join Syntax — String vs Column Expression ───────
# TWO ways to write a join condition in PySpark:
 
# Option A — String shorthand (only works when both tables have same column name)
# df_employees.join(df_departments, "dept_name")
# Problem: employees has "dept", departments has "dept_name" — different names
# String shorthand ONLY works when the column name is IDENTICAL in both tables
 
# Option B — Column expression (works always, explicit, production standard)
# df_employees.join(df_departments, col("dept") == col("dept_name"))
# Explicit about which column from which table. No ambiguity. Always use this.
 
# WHY does this matter?
# With string shorthand on matching column names, Spark drops one of the
# duplicate columns automatically. With column expression, BOTH columns
# are kept — you may need to drop the duplicate manually after the join.
# Column expression is safer because it makes the intent explicit.

# ── BLOCK 3: INNER JOIN ──────────────────────────────────────
# Returns ONLY rows where the join condition matches on BOTH sides.
# Finance dept (no employees) and any employee not in departments → excluded
spark.sparkContext.setJobDescription("Block3: inner join employees + departments")
df_inner = df_employees.join(df_departments, col("dept") == col("dept_name"), "inner")
print("\n=== INNER JOIN: Employees with Department Info ===")
print("Expected: 10 rows (all employees match a dept), Finance dept excluded")
df_inner.select(
    "name", "dept", "salary", "budget", "manager"
).show()

# Check the query plan — look for Exchange nodes (shuffles)
print("\n=== INNER JOIN Query Plan ===")
df_inner.explain(mode="formatted")
# With autoBroadcastJoinThreshold=-1 you should see:
# SortMergeJoin → two Exchange nodes → two Sort nodes

# ── BLOCK 4: LEFT JOIN ───────────────────────────────────────
# Returns ALL rows from the LEFT table.
# Right side gets nulls where no match exists.
# Use case: "show all departments, including those with no employees"

spark.sparkContext.setJobDescription("Block4: left join departments + employees")
df_left = df_departments.join(df_employees, col("dept_name") == col("dept"), "left")
print("\n=== LEFT JOIN: Departments with Employee Info ===")
print("Expected: 14 rows (all departments, including Finance with nulls)")
df_left.select(
    "dept_name", "budget", "manager", "name", "salary"
).show()

# Check the query plan — look for Exchange nodes (shuffles)
print("\n=== LEFT JOIN Query Plan ===")
df_left.explain(mode="formatted")
# With autoBroadcastJoinThreshold=-1 you should see:
# SortMergeJoin → two Exchange nodes → two Sort nodes  

spark.sparkContext.setJobDescription("Block5: full outer join departments + projects")
 
""" Below code will error due to ambiguous column names after the join — both tables have "dept_name"
df_full = df_departments.join(
    df_projects,
    col("dept_name") == col("dept_name"),  # Same column name — ambiguous!
    "full"
)
"""


# PROBLEM: both tables have a column called "dept_name"
# After the join you now have TWO dept_name columns — Spark will error or
# produce ambiguous results if you reference "dept_name" downstream
# FIX: use aliases to disambiguate before the join
 
df_dept_aliased = df_departments.alias("d")
df_proj_aliased = df_projects.alias("p")
 
df_full_clean = df_dept_aliased.join(
    df_proj_aliased,
    col("d.dept_name") == col("p.dept_name"),
    "full"
)
 
print("\n=== FULL OUTER JOIN: Departments vs Projects ===")
print("Expected: Finance (no projects) and Research (no dept) both appear with nulls")
df_full_clean.select(
    col("d.dept_name").alias("department"),
    col("d.budget").alias("dept_budget"),
    col("p.project_name"),
    col("p.budget").alias("project_budget")
).show()

# Finance → appears with null project columns (dept with no projects)
# Research → appears with null dept columns (project with no matching dept)

# ── BLOCK 6: BROADCAST HASH JOIN ─────────────────────────────
# Force Spark to use Broadcast Hash Join by hinting broadcast()
# Compare the query plan to Block 3's Sort-Merge Join plan
 
spark.sparkContext.setJobDescription("Block6: broadcast hash join")
 
df_broadcast = df_employees.join(
    broadcast(df_departments),        # Hint: broadcast the small table
    col("dept") == col("dept_name"),
    "inner"
)
 
print("\n=== BROADCAST HASH JOIN Query Plan ===")
print("Compare to Block 3 — look for BroadcastHashJoin, no Exchange nodes")
df_broadcast.explain(mode="formatted")

# With broadcast() hint you should see:
# BroadcastHashJoin → BroadcastExchange (only ONE, for the small table)
# No Exchange on the large table → no shuffle on employees
# This is dramatically faster at scale
 
 # ── BLOCK 7: HANDLING DUPLICATE COLUMN NAMES ────────────────
# When both tables have a column with the same name, the join
# produces two columns with the same name. This causes errors
# downstream. Always handle this explicitly.
 
# THREE approaches:
 
# Approach 1 — Drop the duplicate after join
df_no_dup = df_employees.join(
    df_departments,
    col("dept") == col("dept_name"),
    "inner"
).drop("dept_name")   # Drop the right side's dept_name, keep employees' dept
 
print("\n=== No Duplicate Columns (dropped dept_name) ===")
df_no_dup.select("name", "dept", "salary", "budget").show(3)

# Approach 2 — Use aliases before the join (cleanest for complex joins)
df_emp = df_employees.alias("e")
df_dep = df_departments.alias("d")
 
df_aliased = df_emp.join(
    df_dep,
    col("e.dept") == col("d.dept_name"),
    "inner"
).select(
    col("e.name"),
    col("e.dept"),
    col("e.salary"),
    col("d.budget"),
    col("d.manager")
)
 
print("\n=== Clean Join with Aliases ===")
df_aliased.show(3)

# Approach 3 — Rename before join
df_dept_renamed = df_departments.withColumnRenamed("dept_name", "department")
df_renamed_join = df_employees.join(
    df_dept_renamed,
    col("dept") == col("department"),
    "inner"
)

print("\n=== Clean Join with Rename ===")
df_renamed_join.select("name", "dept", "salary", "budget").show(3)

# ── BLOCK 8: CROSS JOIN — See why it's dangerous ─────────────
# Every row from left × every row from right. No join key.
# 10 employees × 4 departments = 40 rows
# At scale: 2TB × 500GB = catastrophic
 
spark.sparkContext.setJobDescription("Block8: cross join demonstration")
 
df_cross = df_employees.crossJoin(df_departments)
 
print(f"\n=== CROSS JOIN ===")
print(f"Employees: {df_employees.count()} rows")
print(f"Departments: {df_departments.count()} rows")
print(f"Cross join result: {df_cross.count()} rows")
print(f"Expected: {df_employees.count() * df_departments.count()} rows")
df_cross.select("name", "dept", "dept_name", "budget").show(5)

# ── BLOCK 9: FINDING UNMATCHED ROWS (Anti-Join pattern) ──────
# "Which departments have NO employees?"
# "Which projects have NO matching department?"
# This is one of the most common real-world data quality checks
 
# Left Anti Join — returns rows from LEFT that have NO match on RIGHT
df_depts_no_employees = df_departments.join(
    df_employees,
    col("dept_name") == col("dept"),
    "left_anti"   # Keep left rows that have NO match on right
)
 
print("\n=== LEFT ANTI JOIN: Departments with NO employees ===")
df_depts_no_employees.show()

# df_projects_no_dept = df_projects.join(
#     df_departments,
#     col("dept_name") == col("dept_name"),
#     "left_anti"
# )

# This has duplicate column name issue — use aliases
df_proj_a = df_projects.alias("p")
df_dept_a = df_departments.alias("d")
 
df_projects_no_dept = df_proj_a.join(
    df_dept_a,
    col("p.dept_name") == col("d.dept_name"),
    "left_anti"
)
 
print("\n=== LEFT ANTI JOIN: Projects with NO matching department ===")
df_projects_no_dept.show()
# Expected: AI Initiative (Research dept doesn't exist)

input("\n>>> All blocks done. Spark UI open at the URL above. Press ENTER to stop.")
spark.stop()