from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class IoTMeasurementCreate(BaseModel):
    device_id: str
    temperature: float
    humidity: float
    co2: float
    voltage: Optional[float] = None
    light_intensity: Optional[float] = None
    timestamp: datetime

class IoTMeasurement(IoTMeasurementCreate):
    id: int

    class Config:
        from_attributes = True