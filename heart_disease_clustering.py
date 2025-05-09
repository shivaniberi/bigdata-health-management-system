from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HeartDiseaseClustering") \
    .master("local[*]") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

# Load dataset (update the file path if needed)
csv_path = "file:///Users/kaushikparthasarathy/Downloads/health_data_20250429.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True, sep="|")

# Drop rows with missing values in relevant columns
features_to_use = [
    "bmi", "systolic_bp", "diastolic_bp", "pulse_pressure",
    "heart_rate", "spo2", "sleep_quality_score"
]
df_clean = df.dropna(subset=features_to_use)

# Assemble feature vector
assembler = VectorAssembler(inputCols=features_to_use, outputCol="features")
df_model = assembler.transform(df_clean)

# Train KMeans model with 2 clusters (e.g., Low risk = 0, High risk = 1)
kmeans = KMeans(k=2, seed=42, featuresCol="features", predictionCol="risk_level")
model = kmeans.fit(df_model)

# Assign cluster predictions
df_clustered = model.transform(df_model)

# Show sample results
df_clustered.select("user_id", "bmi", "systolic_bp", "pulse_pressure", "risk_level").show(20)

#Save clustered output as CSV to local filesystem
output_path = "file:///Users/kaushikparthasarathy/Downloads/heart_risk_clusters_output"
df_clustered.select("user_id", "risk_level").write.mode("overwrite").csv(output_path)

# Stop Spark session
spark.stop()
