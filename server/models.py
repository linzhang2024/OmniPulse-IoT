from datetime import datetime
from sqlalchemy import Column, String, DateTime, Enum, ForeignKey, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import enum

Base = declarative_base()

class DeviceStatus(enum.Enum):
    ONLINE = "online"
    OFFLINE = "offline"

class CommandStatus(enum.Enum):
    PENDING = "pending"
    DELIVERED = "delivered"
    SUCCESS = "success"
    EXPIRED = "expired"

class Device(Base):
    __tablename__ = "devices"

    device_id = Column(String(64), primary_key=True, index=True)
    secret_key = Column(String(64), nullable=False, index=True)
    model = Column(String(64), nullable=False)
    status = Column(Enum(DeviceStatus), default=DeviceStatus.OFFLINE)
    last_heartbeat = Column(DateTime, nullable=True)
    pending_commands = Column(JSON, default=list)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    data_records = relationship("DeviceData", back_populates="device", cascade="all, delete-orphan")

class DeviceData(Base):
    __tablename__ = "device_data"

    id = Column(String(36), primary_key=True, index=True)
    device_id = Column(String(64), ForeignKey("devices.device_id"), nullable=False, index=True)
    payload = Column(JSON, nullable=False)
    recorded_at = Column(DateTime, default=datetime.utcnow)
    
    device = relationship("Device", back_populates="data_records")
