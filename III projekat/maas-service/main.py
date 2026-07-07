from fastapi import FastAPI
from pydantic import BaseModel
from sklearn.linear_model import LogisticRegression
import numpy as np

app = FastAPI(title="IoT Model-as-a-Service")

# Simulacija istreniranog modela (prosta klasifikacija: 0 - Normalno, 1 - Opasnost)
# Treniramo ga na dummy podacima pri startu servisa
X_train = np.array([[20], [25], [30], [45], [60], [70], [80]])
y_train = np.array([0, 0, 0, 1, 1, 1, 1]) # Sve preko 40 stepeni je anomalija

model = LogisticRegression()
model.fit(X_train, y_train)

class SensorData(BaseModel):
    temperature: float

@app.post("/predict")
def predict_anomaly(data: SensorData):
    # Predikcija na osnovu primljene temperature
    prediction = model.predict([[data.temperature]])[0]
    probability = model.predict_proba([[data.temperature]])[0][prediction]
    
    status = "CRITICAL_ANOMALY" if prediction == 1 else "NORMAL"
    return {
        "status": status,
        "probability": round(float(probability), 2),
        "temperature_evaluated": data.temperature
    }