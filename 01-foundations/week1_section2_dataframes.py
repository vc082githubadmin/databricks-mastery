from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, avg, count, when, round, upper, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from delta import configure_spark_with_delta_pip

# Build the spark session
budiler = (
    SparkSession.builder
    .master("local[*]")
    .appName("Week1 - Section 2 - DataFrames")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.port", "4040")
)

spark = configure_spark_with_delta_pip(budiler).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print(f"Spark Version : {spark.version}")
print(f"Master : {spark.sparkContext.master}")
print(f"App Name : {spark.sparkContext.appName}")
print(f"Default Parallelism : {spark.sparkContext.defaultParallelism}")
print(f"Spark UI  : {spark.sparkContext.uiWebUrl}")

EmployeeSchema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("dept", StringType(), False),
    StructField("salary", LongType(), False),
    StructField("city", StringType(), False)
])

employees = [
    (1,  "Alice",   "Engineering", 95000, "New York"),
    (2,  "Bob",     "Marketing",   72000, "London"),
    (3,  "Carol",   "Engineering", 105000,"New York"),
    (4,  "Dave",    "Marketing",   68000, "London"),
    (5,  "Eve",     "Engineering", 88000, "Berlin"),
    (6,  "Frank",   "HR",          65000, "New York"),
    (7,  "Grace",   "Engineering", 112000,"Berlin"),
    (8,  "Henry",   "HR",          61000, "London"),
    (9,  "Ivy",     "Marketing",   75000, "Berlin"),
    (10, "Jack",    "Engineering", 99000, "New York"),
]

df = spark.createDataFrame(employees, EmployeeSchema)
print("\n=== Schema ===")
df.printSchema()

spark.sparkContext.setJobDescription("Block2: show raw employee data")
print("\n=== Raw Data ===")
df.show()

spark.sparkContext.setJobDescription("Block3a: select with alias")
df_selected = df.select(
    col("name"),
    col("dept"),
    col("salary").alias("salary_usd"))
print("\n=== Selected Columns with Alias ===")
df_selected.show()

spark.sparkContext.setJobDescription("Block3b: withColumn derived column")
df_with_gbp = df.withColumn("salary_gbp", round(col("salary") * 0.79,2))
print("\n=== With Derived Column (GBP) ===")
df_with_gbp.select("name", "salary", "salary_gbp").show()

spark.sparkContext.setJobDescription("Block3c: conditional column")
df_banded = df.withColumn("salary_band", 
    when(col("salary") >= 100000, "Senior")
    .when(col("salary") >= 80000, "Mid")
    .otherwise("Junior")
)
print("\n=== With Conditional Column (Salary Band) ===")
df_banded.select("name", "salary", "salary_band").show()

spark.sparkContext.setJobDescription("Block4: Aggregation 1 — Department summary")
df_dept_summary = df.groupBy("dept").agg(
    count("*").alias("employee_count"),
    round(avg("salary"), 2).alias("avg_salary"),
    sum("salary").alias("total_salary_bill")
).filter(col("avg_salary") > 80000)

print("\n=== Department Summary ===")
df_dept_summary.show()

spark.sparkContext.setJobDescription("Block5: Aggregation 2 — City summary")
df_city_summary = df.groupBy("city").agg(
    count("*").alias("employee_count").alias("employee_count")
)
print("\n=== City Summary ===")
df_city_summary.show()

spark.sparkContext.setJobDescription("Block6: Aggregation 3 — Department + City breakdown")
df_dept_city_summary = df.groupBy("dept", "city").agg(
    count("*").alias("employee_count")
).orderBy(col("dept"), col("city"))
print("\n=== Department + City Breakdown ===")
df_dept_city_summary.show()

# Registering a temp view for SQL queries
df.createOrReplaceTempView("employees")

# ── Query 1 — Department summary (replicates Block 4 Aggregation 1) ──
spark.sparkContext.setJobDescription("Block7: SQL Query 1 — Department summary")
sql_dept_summary = spark.sql("""
SELECT
    dept,
    COUNT(*) AS employee_count,
    ROUND(AVG(salary), 2) AS avg_salary,
    SUM(salary) AS total_salary_bill
FROM employees
GROUP BY dept
HAVING avg_salary > 80000
""")
print("\n=== SQL Department Summary ===")
sql_dept_summary.show()

# ── Query 2 — Top earner per department ──────────────────────────────
# Subquery finds MAX(salary) per dept

spark.sparkContext.setJobDescription("Block7: SQL Query 2 — Top earner per department")
sql_top_earners = spark.sql("""
SELECT e.dept, e.name, e.salary
FROM employees e
    where (e.dept, e.salary) IN (
        SELECT dept, MAX(salary)
        FROM employees
        GROUP BY dept
    )
ORDER BY e.dept
""")
print("\n=== SQL Top Earners per Department ===")
sql_top_earners.show()

# ── Query 3 — Employees above their department average ───────────────
spark.sparkContext.setJobDescription("Block7: SQL Query 3 — Employees above dept average")
sql_above_avg = spark.sql("""
select e.name, 
    e.dept, e.salary, 
    round(avg(e.salary) over (partition by e.dept), 2) as dept_avg_salary
from employees e
""").filter(col("salary") > col("dept_avg_salary"))
print("\n=== SQL Employees Above Department Average ===")
sql_above_avg.show()

# Save the department summary as Parquet and the banded DataFrame as Delta
df_dept_summary.write.format("parquet").mode("overwrite").save("files/employee_dept_summary_parquet")
df_banded.write.format("delta").mode("overwrite").save("files/employee_banded_delta")

# Read back the Delta files to verify
spark.sparkContext.setJobDescription("Block8: Read back Delta files")
df_banded_delta = spark.read.format("delta").load("files/employee_banded_delta")
print("\n=== Banded DataFrame read back from Delta ===")
df_banded_delta.select("name", "dept", "salary", "salary_band").show()


input("\n>>> Spark is running. Open http://localhost:4041 — press ENTER to stop...")
spark.stop()