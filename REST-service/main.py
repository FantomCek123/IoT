from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
import models, schemas, database 

models.Base.metadata.create_all(bind=database.engine)

app = FastAPI(title="IoT REST API - Scenario A & C")

def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def root():
    return {"message": "REST API radi i povezan je sa bazom"}


@app.get("/measurements/", response_model=list[schemas.IoTMeasurement])
def read_all_measurements(limit: int = 100, db: Session = Depends(get_db)):
    """Vraća sva merenja iz baze, bez obzira na uređaj."""
    return db.query(models.IoTMeasurement).order_by(models.IoTMeasurement.timestamp.desc()).limit(limit).all()


@app.get("/measurements/{device_id}", response_model=list[schemas.IoTMeasurement])
def get_history(device_id: str, limit: int = 100, db: Session = Depends(get_db)):
    return db.query(models.IoTMeasurement).filter(
        models.IoTMeasurement.device_id == device_id
    ).order_by(models.IoTMeasurement.timestamp.desc()).limit(limit).all()

@app.post("/measurements/", response_model=schemas.IoTMeasurement, status_code=201)
def create_measurement(data: schemas.IoTMeasurementCreate, db: Session = Depends(get_db)):
    db_measurement = models.IoTMeasurement(**data.model_dump())
    db.add(db_measurement)
    db.commit()
    db.refresh(db_measurement)
    return db_measurement