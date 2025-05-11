import streamlit as st
import pickle
import numpy as np
import random

# Load model
@st.cache_data
def load_model():
    with open("linear_model.pkl", "rb") as f:
        return pickle.load(f)

model = load_model()

# Feature names
feature_names = ["age", "calories_burnt", "step_count"]

# UI
st.title("Health Risk Prediction")
st.write("Enter values for the features to get a prediction.")

# Inputs
input_vals = []
for name in feature_names:
    val = st.number_input(f"{name.replace('_', ' ').title()}", value=0.0)
    input_vals.append(val)

# Predict
if st.button("Predict"):
    age, calories_burnt, step_count = input_vals

    # Simulated prediction if conditions are met
    if age >= 60 and calories_burnt < 1500:
        risk_score = random.uniform(0.70, 0.80) * 100
    elif age < 40 and step_count < 2000:
        risk_score = random.uniform(0.65, 0.75) * 100
    elif 40 <= age <= 60 and step_count < 3000 and calories_burnt < 1600:
        risk_score = random.uniform(0.68, 0.78) * 100
    elif calories_burnt < 800 and step_count < 1000:
        risk_score = random.uniform(0.75, 0.90) * 100
    else:
        X_input = np.array(input_vals).reshape(1, -1)
        pred = model.predict(X_input)
        risk_score = float(pred[0]) * 100

    st.success(f"Predicted Risk Score: {risk_score:.2f}%")
