#!/bin/bash
# Download and upload 10GB health dataset to HDFS

# Install gdown if not present
pip install gdown

# Download dataset from Google Drive
gdown "https://drive.google.com/uc?id=1HztY2rqzY1ZPTXsHTkmzT8S0ABYN1vLQ" -O synthetic_health_data_10gb.csv

# Verify file
echo "File size: $(du -h synthetic_health_data_10gb.csv)"
echo "First 2 lines:"
head -n 2 synthetic_health_data_10gb.csv

# Upload to HDFS
hdfs dfs -mkdir -p /sberi/health_dataset/raw
hdfs dfs -put synthetic_health_data_10gb.csv /sberi/health_dataset/raw/

echo "Data uploaded to HDFS at: hdfs:///sberi/health_dataset/raw/synthetic_health_data_10gb.csv"
