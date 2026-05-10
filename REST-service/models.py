from sqlalchemy import Column, Integer, Float, String, DateTime, Index
from database import Base
from datetime import datetime

class IoTMeasurement(Base):
    __tablename__ = "measurements"

    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String, index=True) 
    timestamp = Column(DateTime, default=datetime.utcnow, index=True) 
    

    temperature = Column(Float)
    humidity = Column(Float)
    co2 = Column(Float)
    voltage = Column(Float)
    light_intensity = Column(Float)


    __table_args__ = (Index('ix_device_timestamp', 'device_id', 'timestamp'),)