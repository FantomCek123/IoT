from sqlalchemy import Column, Integer, Float, String, DateTime, Index
from database import Base

class IoTMeasurement(Base):
    __tablename__ = "measurements"

    id = Column(Integer, primary_key=True, index=True)
    deviceId = Column("deviceId", String, index=True) 
    timestamp = Column(DateTime, index=True) 
    
    temperature = Column(Float)
    humidity = Column(Float)
    voltage = Column(Float)    
    lightIntensity = Column("lightIntensity", Float)

    __table_args__ = (Index('ix_device_timestamp', 'deviceId', 'timestamp'),)