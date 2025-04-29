from pyspark.sql import SparkSession

# Initialize Spark with optimized configurations
spark = SparkSession.builder \
    .appName("HealthDataIngestion") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Read CSV from HDFS
df = spark.read.csv(
    "hdfs:///sberi/health_dataset/raw/synthetic_health_data_10gb.csv",
    header=True,
    inferSchema=True
)

# Clean data: drop nulls and split BP column (if needed)
df = df.na.drop()  # Remove rows with null values

# Save as Parquet (partitioned for efficiency)
df.write.parquet(
    "hdfs:///sberi/health_dataset/processed/health_data_parquet/",
    mode="overwrite"
)

print("Data converted to Parquet and saved to HDFS.")
