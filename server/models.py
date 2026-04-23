from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Enum, ForeignKey, JSON, Float, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import enum

Base = declarative_base()

class DeviceStatus(enum.Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"

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
    last_seen = Column(DateTime, nullable=True)
    consecutive_alert_count = Column(Integer, default=0)
    pending_commands = Column(JSON, default=list)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    data_records = relationship("DeviceData", back_populates="device", cascade="all, delete-orphan")
    status_events = relationship("DeviceStatusEvent", back_populates="device", cascade="all, delete-orphan")

class DeviceStatusEvent(Base):
    __tablename__ = "device_status_events"

    id = Column(String(36), primary_key=True, index=True)
    device_id = Column(String(64), ForeignKey("devices.device_id"), nullable=False, index=True)
    event_type = Column(String(32), nullable=False, index=True)
    old_status = Column(String(32), nullable=True)
    new_status = Column(String(32), nullable=True)
    reason = Column(String(500), nullable=True)
    details = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    device = relationship("Device", back_populates="status_events")

class DeviceData(Base):
    __tablename__ = "device_data"

    id = Column(String(36), primary_key=True, index=True)
    device_id = Column(String(64), ForeignKey("devices.device_id"), nullable=False, index=True)
    payload = Column(JSON, nullable=False)
    recorded_at = Column(DateTime, default=datetime.utcnow)
    
    device = relationship("Device", back_populates="data_records")

class DeviceDataHistory(Base):
    __tablename__ = "device_data_history"

    id = Column(String(36), primary_key=True, index=True)
    device_id = Column(String(64), ForeignKey("devices.device_id"), nullable=False, index=True)
    payload = Column(JSON, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    temperature = Column(Float, nullable=True, index=True)
    humidity = Column(Float, nullable=True)
    is_alert = Column(Boolean, default=False, index=True)

class ProtocolType(enum.Enum):
    JSON = "json"
    HEX = "hex"
    STRING = "string"
    BINARY = "binary"

class DeviceProtocol(Base):
    __tablename__ = "device_protocols"

    id = Column(String(36), primary_key=True, index=True)
    device_id = Column(String(64), ForeignKey("devices.device_id"), nullable=False, index=True)
    protocol_type = Column(Enum(ProtocolType), default=ProtocolType.JSON)
    is_active = Column(Boolean, default=True)
    
    parse_config = Column(JSON, nullable=False, default=dict)
    field_mappings = Column(JSON, nullable=False, default=dict)
    transform_formulas = Column(JSON, nullable=False, default=dict)
    
    description = Column(String(500), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class UserRole(enum.Enum):
    GUEST = "guest"
    STAFF = "staff"
    ADMIN = "admin"

class ComplaintStatus(enum.Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"

class User(Base):
    __tablename__ = "users"

    id = Column(String(36), primary_key=True, index=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    password_hash = Column(String(128), nullable=False)
    role = Column(Enum(UserRole), default=UserRole.GUEST)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    complaints = relationship("Complaint", back_populates="user")
    replies = relationship("ComplaintReply", back_populates="user")

class Complaint(Base):
    __tablename__ = "complaints"

    id = Column(String(36), primary_key=True, index=True)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    title = Column(String(255), nullable=False)
    content = Column(String(2000), nullable=False)
    status = Column(Enum(ComplaintStatus), default=ComplaintStatus.PENDING)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    user = relationship("User", back_populates="complaints")
    replies = relationship("ComplaintReply", back_populates="complaint", cascade="all, delete-orphan")

class ComplaintReply(Base):
    __tablename__ = "complaint_replies"

    id = Column(String(36), primary_key=True, index=True)
    complaint_id = Column(String(36), ForeignKey("complaints.id"), nullable=False, index=True)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    content = Column(String(2000), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    complaint = relationship("Complaint", back_populates="replies")
    user = relationship("User", back_populates="replies")
