from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark
spark = SparkSession.builder \
    .appName("HealthFeatureEngineering") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Read Parquet data
df = spark.read.parquet("hdfs:///sberi/health_dataset/processed/health_data_parquet/")

# --- Feature Engineering ---
# 1. Temporal Features
df = df.withColumn("day_of_week", dayofweek("datestamp")) \
       .withColumn("month", month("datestamp")) \
       .withColumn("year", year("datestamp"))

# 2. Derived Health Metrics
df = df.withColumn("pulse_pressure", col("systolic_bp") - col("diastolic_bp")) \
       .withColumn("activity_level",
           when(col("step_count") < 5000, "Sedentary")
          .when(col("step_count").between(5000, 10000), "Moderate")
          .otherwise("Active"))

# 3. Composite Risk Score
df = df.withColumn("health_risk_score",
    (when(col("systolic_bp") > 140, 3)
     .when(col("systolic_bp") > 130, 2)
     .otherwise(1)) +
    (when(col("bmi") > 30, 2)
     .when(col("bmi") > 25, 1)
     .otherwise(0))
)

# 4. Seasonal Features
df = df.withColumn("season",
    when(col("month").isin(12, 1, 2), "Winter")
   .when(col("month").isin(3, 4, 5), "Spring")
   .when(col("month").isin(6, 7, 8), "Summer")
   .otherwise("Fall"))

# --- Save Results ---
(df.orderBy("user_id", "datestamp")
   .repartition(1)
   .write
   .option("header", "true")
   .option("delimiter", "|")
   .mode("overwrite")
   .csv("hdfs:///sberi/health_dataset/final_processed_data")
)

print("Feature engineering complete. Output saved to HDFS.")
