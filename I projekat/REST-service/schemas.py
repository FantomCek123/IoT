from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class IoTMeasurementCreate(BaseModel):
    deviceId: str
    temperature: float
    humidity: float
    voltage: Optional[float] = None
    lightIntensity: Optional[float] = None
    timestamp: datetime

class IoTMeasurement(IoTMeasurementCreate):
    id: int

    class Config:
        from_attributes = True