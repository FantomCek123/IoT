import strawberry
from typing import List, Optional
from datetime import datetime
import models, database # Uvozi tvoje postojeće modele

@strawberry.type
class Measurement:
    id: int
    device_id: str
    temperature: float
    humidity: float
    co2: float
    voltage: Optional[float]
    light_intensity: Optional[float]
    timestamp: datetime

@strawberry.type
class Query:
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
                models.IoTMeasurement.device_id == device_id
            ).all()
        finally:
            db.close()

schema = strawberry.Schema(query=Query)