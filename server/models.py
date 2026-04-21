from datetime import datetime
from sqlalchemy import Column, String, DateTime, Enum
from sqlalchemy.ext.declarative import declarative_base
import enum

Base = declarative_base()

class DeviceStatus(enum.Enum):
    ONLINE = "online"
    OFFLINE = "offline"

class Device(Base):
    __tablename__ = "devices"

    device_id = Column(String(64), primary_key=True, index=True)
    model = Column(String(64), nullable=False)
    status = Column(Enum(DeviceStatus), default=DeviceStatus.OFFLINE)
    last_heartbeat = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
