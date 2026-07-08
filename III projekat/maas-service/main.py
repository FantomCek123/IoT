from fastapi import FastAPI
from pydantic import BaseModel
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split  # ZA VALIDACIJU
from sklearn.metrics import accuracy_score            # ZA TESTIRANJE
import numpy as np

app = FastAPI(title="IoT Model-as-a-Service")

X = np.array([[15], [18], [22], [24], [28], [32], [35], [42], [47], [55], [62], [70], [75], [83]])
y = np.array([0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1])

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = LogisticRegression()
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print(f"✔ Model uspešno istreniran i testiran. Tačnost (Accuracy) na test setu: {accuracy * 100}%")

class SensorData(BaseModel):
    temperature: float

@app.post("/predict")
def predict_anomaly(data: SensorData):
    prediction = model.predict([[data.temperature]])[0]
    probability = model.predict_proba([[data.temperature]])[0][prediction]
    
    status = "CRITICAL_ANOMALY" if prediction == 1 else "NORMAL"
    return {
        "status": status,
        "probability": round(float(probability), 2),
        "temperature_evaluated": data.temperature
    }