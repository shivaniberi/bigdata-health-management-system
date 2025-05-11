# bigdata-health-management-system
**Features**
Data Ingestion: Process large-scale health datasets (10GB+)

Feature Engineering: Automated feature transformation

Risk Prediction: Linear Regression model (linear_model.pkl)

Clustering: K-Means implementation for patient segmentation

Interactive Dashboard: Streamlit-based visualization

### How to Run
1. Upload data: `bash scripts/data_upload.sh`  
2. Run ingestion: `spark-submit scripts/data_ingestion.py`  
3. Feature engineering: `spark-submit scripts/feature_engineering.py`  


4. **Synthesized data-**
https://drive.google.com/drive/folders/1ST9vHckZWEk4bmQYSXxBfGQoHbdK4Q-y?usp=sharing

5. **Processed Data(final_processed_data-17GB)**
[https://drive.google.com/drive/folders/1ST9vHckZWEk4bmQYSXxBfGQoHbdK4Q-y?usp=sharing](https://drive.google.com/file/d/1XSwNbc-LKT_5F0me1THMt34FbZNavoDe/view?usp=drive_link)

6. **EDA**- Run the script: 2_EDA.ipynb
7. **K- means clustering on the processed data** - Run below script := heart_disease_clustering.py
8. **Run Linear Regression model for the prediction of high/low risk of diseases** -
    Risk Prediction:
    -Pre-trained model: linear_model.pkl

  **Streamlit Dashboard:**
  Launch the interactive dashboard:streamlit run app.py
  
**Dashboard Preview:**
  
  ![Health_Prediction](https://github.com/user-attachments/assets/177ff1aa-de04-4ee5-bb19-72dff068657d)




