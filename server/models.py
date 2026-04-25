from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Enum, ForeignKey, JSON, Float, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import enum

Base = declarative_base()

class DeviceStatus(enum.Enum):
    ONLINE = "online"
    PENDING_OFFLINE = "pending_offline"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"

class CommandStatus(enum.Enum):
    PENDING = "pending"
    DELIVERED = "delivered"
    EXECUTED = "executed"
    FAILED = "failed"
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

class OperationType(enum.Enum):
    DEVICE_REGISTER = "device_register"
    DEVICE_DELETE = "device_delete"
    DEVICE_UPDATE = "device_update"
    DEVICE_CONTROL = "device_control"
    DATA_DELETE = "data_delete"
    DATA_CLEAR = "data_clear"
    PROTOCOL_UPDATE = "protocol_update"
    THRESHOLD_UPDATE = "threshold_update"
    USER_CREATE = "user_create"
    USER_UPDATE = "user_update"
    USER_DELETE = "user_delete"
    COMPLAINT_UPDATE = "complaint_update"
    SETTING_UPDATE = "setting_update"

class RiskLevel(enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(String(36), primary_key=True, index=True)
    operation_type = Column(Enum(OperationType), nullable=False, index=True)
    operation_desc = Column(String(500), nullable=True)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    device_id = Column(String(64), ForeignKey("devices.device_id"), nullable=True, index=True)
    ip_address = Column(String(64), nullable=True)
    risk_level = Column(Enum(RiskLevel), default=RiskLevel.LOW, index=True)
    changed_fields = Column(JSON, nullable=True)
    status = Column(String(32), default="success")
    error_message = Column(String(1000), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    user = relationship("User", foreign_keys=[user_id])

class ReportTaskStatus(enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class ScheduledReportType(enum.Enum):
    DAILY_BRIEFING = "daily_briefing"
    WEEKLY_SUMMARY = "weekly_summary"
    MONTHLY_REPORT = "monthly_report"

class ReportTask(Base):
    __tablename__ = "report_tasks"

    id = Column(String(36), primary_key=True, index=True)
    task_name = Column(String(255), nullable=True)
    device_id = Column(String(64), ForeignKey("devices.device_id"), nullable=True, index=True)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    
    status = Column(Enum(ReportTaskStatus), default=ReportTaskStatus.PENDING, index=True)
    progress = Column(Integer, default=0)
    
    file_path = Column(String(500), nullable=True)
    download_url = Column(String(500), nullable=True)
    file_name = Column(String(255), nullable=True)
    file_size_bytes = Column(Integer, default=0)
    
    record_count = Column(Integer, default=0)
    error_message = Column(String(2000), nullable=True)
    
    start_time = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    expire_at = Column(DateTime, nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    filters = Column(JSON, nullable=True)
    report_type = Column(String(64), nullable=True)
    export_format = Column(String(16), default="csv")
    
    device = relationship("Device", foreign_keys=[device_id])
    user = relationship("User", foreign_keys=[user_id])

class ScheduledReportConfig(Base):
    __tablename__ = "scheduled_report_configs"

    id = Column(String(36), primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(String(500), nullable=True)
    
    report_type = Column(Enum(ScheduledReportType), default=ScheduledReportType.DAILY_BRIEFING, index=True)
    cron_expression = Column(String(128), nullable=True)
    
    device_ids = Column(JSON, nullable=True)
    filters = Column(JSON, nullable=True)
    export_format = Column(String(16), default="csv")
    
    output_directory = Column(String(500), nullable=True)
    retention_days = Column(Integer, default=7)
    file_name_template = Column(String(255), nullable=True)
    
    is_active = Column(Boolean, default=True, index=True)
    last_run_at = Column(DateTime, nullable=True)
    last_run_status = Column(String(32), nullable=True)
    last_run_error = Column(String(2000), nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class DeviceCommand(Base):
    __tablename__ = "device_commands"
    
    id = Column(String(36), primary_key=True, index=True)
    device_id = Column(String(64), ForeignKey("devices.device_id"), nullable=False, index=True)
    
    client_msg_id = Column(String(128), nullable=True, index=True, unique=True)
    
    command_type = Column(String(64), nullable=False)
    command_value = Column(String(255), nullable=True)
    
    status = Column(Enum(CommandStatus), default=CommandStatus.PENDING, index=True)
    
    ttl_seconds = Column(Integer, default=600)
    expires_at = Column(DateTime, nullable=False, index=True)
    
    delivered_at = Column(DateTime, nullable=True)
    executed_at = Column(DateTime, nullable=True)
    failed_at = Column(DateTime, nullable=True)
    expired_at = Column(DateTime, nullable=True)
    
    error_message = Column(String(2000), nullable=True)
    result_data = Column(JSON, nullable=True)
    
    reason = Column(String(500), nullable=True)
    source = Column(String(64), default="api")
    
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    device = relationship("Device")

class RuleAction(enum.Enum):
    SEND_COMMAND = "send_command"
    NOTIFY = "notify"

class RuleOperator(enum.Enum):
    GT = ">"
    LT = "<"
    EQ = "=="
    GE = ">="
    LE = "<="
    NE = "!="

class Rule(Base):
    __tablename__ = "rules"
    
    id = Column(String(36), primary_key=True, index=True)
    rule_name = Column(String(255), nullable=False, index=True)
    device_id = Column(String(64), ForeignKey("devices.device_id"), nullable=False, index=True)
    
    metric = Column(String(64), nullable=False)
    operator = Column(Enum(RuleOperator), nullable=False)
    threshold = Column(Float, nullable=False)
    
    action = Column(Enum(RuleAction), nullable=False)
    
    command_type = Column(String(64), nullable=True)
    command_value = Column(String(255), nullable=True)
    
    is_active = Column(Boolean, default=True, index=True)
    description = Column(String(500), nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    triggered_at = Column(DateTime, nullable=True)
    trigger_count = Column(Integer, default=0)
    
    device = relationship("Device")
