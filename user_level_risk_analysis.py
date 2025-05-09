from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when

# Start Spark session
spark = SparkSession.builder \
    .appName("UserLevelRiskAnalysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

# Load the previously clustered output
input_path = "file:///Users/kaushikparthasarathy/Downloads/heart_risk_clusters_output"
df_clustered = spark.read.csv(input_path, header=False, inferSchema=True)

# Rename columns
df_clustered = df_clustered.withColumnRenamed("_c0", "user_id").withColumnRenamed("_c1", "risk_level")
df_clustered = df_clustered.withColumn("risk_level", col("risk_level").cast("integer"))

# Risk threshold
threshold = 0.6

# Compute high-risk day ratio
user_risk_summary = df_clustered.groupBy("user_id") \
    .agg(avg(col("risk_level")).alias("high_risk_day_ratio"))

# Add label
user_risk_labeled = user_risk_summary.withColumn(
    "user_risk_label",
    when(col("high_risk_day_ratio") >= threshold, "High Risk").otherwise("Low Risk")
)

# Sort by user_id before saving
user_risk_sorted = user_risk_labeled.orderBy("user_id")

# Show output
user_risk_sorted.show(truncate=False)

# Save output as CSV
output_path = "file:///Users/kaushikparthasarathy/Downloads/user_level_risk_summary_sorted"
user_risk_sorted.write.mode("overwrite").option("header", True).csv(output_path)

# Stop Spark session
spark.stop()
