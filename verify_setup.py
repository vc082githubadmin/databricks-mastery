from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("VerifySetup")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.port", "4040")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"\n✅ Spark version  : {spark.version}")
print(f"✅ Python version : {spark.sparkContext.pythonVer}")
print(f"✅ Spark UI live  : http://localhost:4040\n")

# DataFrame test
df = spark.range(1000).selectExpr("id", "id * 2 as doubled")
print(f"✅ DataFrame rows : {df.count()}")

# Delta test
df.write.format("delta").mode("overwrite").save("/tmp/verify_delta")
rows = spark.read.format("delta").load("/tmp/verify_delta").count()
print(f"✅ Delta rows     : {rows}")

print("\n🎉 All checks passed — local PySpark + Delta Lake is ready!")
input("\nSpark UI is live at http://localhost:4040 — open it in browser\nPress ENTER to stop...")
spark.stop()