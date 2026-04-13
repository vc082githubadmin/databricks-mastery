from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, avg
from delta import configure_spark_with_delta_pip

# Build the spark session
budiler = (
    SparkSession.builder
    .master("local[*]")
    .appName("Week1 - Fundamentals")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.port", "4040")
)

spark = configure_spark_with_delta_pip(budiler).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


print(f"Spark Version : {spark.version}")
print(f"Master : {spark.sparkContext.master}")
print(f"App Name : {spark.sparkContext.appName}")
print(f"Default Parallelism : {spark.sparkContext.defaultParallelism}")


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


schema = ["id", "name", "dept", "salary", "city"]
df = spark.createDataFrame(employees, schema)

print("\n=== Schema ===")
df.printSchema()

spark.sparkContext.setJobDescription("Block1: show raw employee data")
print("\n=== Raw Data ===")
df.show()


# Block 2 - add this now
print("\n── Transformations (lazy) ──────────────")
filtered  = df.filter(col("salary") > 80000)
dept_avg  = filtered.groupBy("dept").agg(
    avg("salary").alias("avg_salary"),
    count("*").alias("headcount")
)
highest   = dept_avg.orderBy(desc("avg_salary"))

print("DAG built — no jobs fired yet")
print("Check Jobs tab now — still same job count\n")

spark.sparkContext.setJobDescription("Block2: dept salary aggregation")
highest.show()

print("\n── EXPLAIN — see the plan in code ─────")
spark.sparkContext.setJobDescription("Block3: explain plan")
highest.explain(mode="formatted")

input("\n>>> Spark is running. Open http://localhost:4041 — press ENTER to stop...")
spark.stop()
