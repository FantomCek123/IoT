import strawberry
from typing import List, Optional
from datetime import datetime
from sqlalchemy import func
import models, database 

@strawberry.type
class Measurement:
    id: int
    deviceId: str = strawberry.field(name="deviceId")
    temperature: float
    humidity: float
    voltage: Optional[float]
    lightIntensity: Optional[float] = strawberry.field(name="lightIntensity")
    timestamp: datetime

@strawberry.type
class MeasurementAnalytics:
    avgTemperature: float
    maxHumidity: float

@strawberry.type
class Query:
    # --- SCENARIO B: Selektivno praćenje ---
    @strawberry.field
    def all_measurements(self, limit: int = 100) -> List[Measurement]:
        db = database.SessionLocal()
        try:
            return db.query(models.IoTMeasurement).order_by(
                models.IoTMeasurement.timestamp.desc()
            ).limit(limit).all()
        finally:
            db.close()

    @strawberry.field
    def device_history(self, device_id: str) -> List[Measurement]:
        db = database.SessionLocal()
        try:
            return db.query(models.IoTMeasurement).filter(
                models.IoTMeasurement.deviceId == device_id
            ).all()
        finally:
            db.close()

    # --- SCENARIO C: Teški istorijski upiti (Agregacija) ---
    @strawberry.field
    def measurement_analytics(self) -> MeasurementAnalytics:
        db = database.SessionLocal()
        try:
            analytics = db.query(
                func.avg(models.IoTMeasurement.temperature).label("avg_temp"),
                func.max(models.IoTMeasurement.humidity).label("max_hum")
            ).first()
            
            return MeasurementAnalytics(
                avgTemperature=round(analytics.avg_temp or 0.0, 2),
                maxHumidity=round(analytics.max_hum or 0.0, 2)
            )
        finally:
            db.close()

schema = strawberry.Schema(query=Query)