from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
import models, schemas, database 

models.Base.metadata.create_all(bind=database.engine)

app = FastAPI(title="IoT REST API - Unificirani camelCase")

def get_db():
    db = database.SessionLocal()
    try:
         yield db
    finally:
         db.close()

@app.get("/")
def root():
    return {"message": "REST API radi i povezan je sa bazom"}

# --- SCENARIO C: Teški istorijski upiti (Agregacija) ---
@app.get("/measurements/analytics")
def get_analytics(db: Session = Depends(get_db)):
    analytics = db.query(
        func.avg(models.IoTMeasurement.temperature).label("avgTemperature"),
        func.max(models.IoTMeasurement.humidity).label("maxHumidity")
    ).first()
    
    return {
        "avgTemperature": round(analytics.avgTemperature or 0.0, 2),
        "maxHumidity": round(analytics.maxHumidity or 0.0, 2)
    }

# --- SCENARIO B: Povlačenje svih merenja ---
@app.get("/measurements/", response_model=list[schemas.IoTMeasurement])
def read_all_measurements(limit: int = 100, db: Session = Depends(get_db)):
    return db.query(models.IoTMeasurement).order_by(models.IoTMeasurement.timestamp.desc()).limit(limit).all()

# --- SCENARIO A: High-Frequency Ingestion ---
@app.post("/measurements/", response_model=schemas.IoTMeasurement, status_code=201)
def create_measurement(data: schemas.IoTMeasurementCreate, db: Session = Depends(get_db)):
    db_measurement = models.IoTMeasurement(**data.model_dump())
    db.add(db_measurement)
    db.commit()
    db.refresh(db_measurement)
    return db_measurement