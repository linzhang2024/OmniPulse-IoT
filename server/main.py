from fastapi import FastAPI, Depends, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, desc, and_, func, text
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import os
import uuid
import hashlib
import secrets
import string
import time

from .models import (
    Base, Device, DeviceStatus, DeviceData, DeviceDataHistory, CommandStatus,
    User, UserRole, Complaint, ComplaintStatus, ComplaintReply
)

DATABASE_URL = "sqlite:///./iot_devices.db"
HEARTBEAT_TIMEOUT = 30
CHECK_INTERVAL = 10
COMMAND_TTL_SECONDS = 600
SIGNATURE_TIMESTAMP_TOLERANCE = 60

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)

scheduler = AsyncIOScheduler()

TEMPERATURE_THRESHOLD = 50
ALERT_CONSECUTIVE_THRESHOLD = 3

def check_all_devices_status():
    db = SessionLocal()
    try:
        devices = db.query(Device).all()
        
        now = datetime.utcnow()
        updated_count = 0
        alert_count = 0
        
        for device in devices:
            if device.last_heartbeat is not None:
                time_since_heartbeat = (now - device.last_heartbeat).total_seconds()
                
                if device.status == DeviceStatus.ONLINE and time_since_heartbeat > HEARTBEAT_TIMEOUT:
                    device.status = DeviceStatus.OFFLINE
                    updated_count += 1
            
            latest_data = get_latest_device_data(device.device_id, db)
            if latest_data and latest_data.payload:
                payload = latest_data.payload
                temperature = None
                
                if 'temperature' in payload:
                    temperature = payload['temperature']
                elif 'temp' in payload:
                    temperature = payload['temp']
                
                if temperature is not None and isinstance(temperature, (int, float)):
                    if temperature > TEMPERATURE_THRESHOLD:
                        if device.consecutive_alert_count is None:
                            device.consecutive_alert_count = 0
                        device.consecutive_alert_count += 1
                        
                        print(f"[Alert] Device {device.device_id}: Temperature {temperature}°C exceeds threshold, consecutive count: {device.consecutive_alert_count}/{ALERT_CONSECUTIVE_THRESHOLD}")
                        
                        if device.consecutive_alert_count >= ALERT_CONSECUTIVE_THRESHOLD:
                            if device.pending_commands is None:
                                device.pending_commands = []
                            
                            existing_pending_alert = any(
                                cmd.get('command') == 'alert_buzzer' and
                                cmd.get('status') in [CommandStatus.PENDING.value, CommandStatus.DELIVERED.value]
                                for cmd in device.pending_commands
                            )
                            
                            if not existing_pending_alert:
                                alert_cmd = create_command(
                                    "alert_buzzer",
                                    "on",
                                    reason=f"Temperature exceeded threshold {ALERT_CONSECUTIVE_THRESHOLD} consecutive times, latest: {temperature}°C"
                                )
                                device.pending_commands.append(alert_cmd)
                                alert_count += 1
                                print(f"[Alert] Device {device.device_id}: Triggering alert_buzzer after {ALERT_CONSECUTIVE_THRESHOLD} consecutive alerts (id={alert_cmd['id']})")
                    else:
                        if device.consecutive_alert_count is None or device.consecutive_alert_count > 0:
                            print(f"[Alert] Device {device.device_id}: Temperature {temperature}°C normalized, resetting consecutive alert count")
                            device.consecutive_alert_count = 0
        
        if updated_count > 0 or alert_count > 0:
            db.commit()
            if updated_count > 0:
                print(f"[Scheduler] Updated {updated_count} devices to OFFLINE status")
            if alert_count > 0:
                print(f"[Scheduler] Triggered {alert_count} temperature alerts")
    except Exception as e:
        print(f"[Scheduler] Error checking devices: {e}")
    finally:
        db.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(
        check_all_devices_status,
        'interval',
        seconds=CHECK_INTERVAL
    )
    scheduler.start()
    print(f"[Scheduler] Started - checking devices every {CHECK_INTERVAL} seconds")
    yield
    scheduler.shutdown()
    print("[Scheduler] Stopped")

app = FastAPI(title="IoT Management Platform", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def index():
    return FileResponse("templates/index.html")

def generate_secret_key(length: int = 32) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def compute_signature(device_id: str, timestamp: str, secret_key: str) -> str:
    raw_string = f"{device_id}{timestamp}{secret_key}"
    return hashlib.md5(raw_string.encode('utf-8')).hexdigest().lower()

def verify_signature(device_id: str, timestamp: str, signature: str, secret_key: str) -> tuple[bool, str]:
    expected_signature = compute_signature(device_id, timestamp, secret_key)
    
    if signature.lower() != expected_signature:
        return False, f"Invalid signature. Expected: {expected_signature}"
    
    try:
        timestamp_int = int(timestamp)
        current_timestamp = int(time.time())
        time_diff = abs(current_timestamp - timestamp_int)
        
        if time_diff > SIGNATURE_TIMESTAMP_TOLERANCE:
            return False, f"Timestamp expired. Time difference: {time_diff}s, tolerance: {SIGNATURE_TIMESTAMP_TOLERANCE}s"
    except ValueError:
        return False, "Invalid timestamp format"
    
    return True, "Signature valid"

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class DeviceRegister(BaseModel):
    device_id: str
    model: str

class ControlCommand(BaseModel):
    command: str
    value: str

class HeartbeatRequest(BaseModel):
    executed_commands: list = []

class HeartbeatResponse(BaseModel):
    device_id: str
    status: str
    last_heartbeat: datetime
    pending_commands: list = []
    acknowledged_count: int = 0
    expired_count: int = 0

class DeviceDataReport(BaseModel):
    device_id: str
    payload: dict

@app.post("/devices/register")
def register_device(device_data: DeviceRegister, db: Session = Depends(get_db)):
    existing_device = db.query(Device).filter(
        Device.device_id == device_data.device_id
    ).first()
    
    if existing_device:
        return {
            "message": "Device already registered",
            "device_id": existing_device.device_id,
            "model": existing_device.model,
            "status": existing_device.status.value,
            "secret_key": existing_device.secret_key
        }
    
    secret_key = generate_secret_key()
    new_device = Device(
        device_id=device_data.device_id,
        secret_key=secret_key,
        model=device_data.model,
        status=DeviceStatus.OFFLINE,
        last_heartbeat=None
    )
    db.add(new_device)
    db.commit()
    db.refresh(new_device)
    
    return {
        "message": "Device registered successfully",
        "device_id": new_device.device_id,
        "model": new_device.model,
        "status": new_device.status.value,
        "secret_key": new_device.secret_key
    }

def create_command(command: str, value: str, reason: str = None) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "command": command,
        "value": value,
        "status": CommandStatus.PENDING.value,
        "created_at": datetime.utcnow().isoformat(),
        "delivered_at": None,
        "reason": reason
    }

def is_command_expired(cmd: dict, now: datetime) -> bool:
    if cmd.get("status") == CommandStatus.EXPIRED.value:
        return True
    
    created_at_str = cmd.get("created_at")
    if not created_at_str:
        return False
    
    try:
        created_at = datetime.fromisoformat(created_at_str)
        time_since_created = (now - created_at).total_seconds()
        return time_since_created > COMMAND_TTL_SECONDS
    except (ValueError, TypeError):
        return False

def process_heartbeat_commands(
    device: Device,
    executed_commands: list,
    now: datetime
) -> tuple:
    if device.pending_commands is None:
        device.pending_commands = []
    
    commands = device.pending_commands
    acknowledged_count = 0
    expired_count = 0
    commands_to_deliver = []
    
    executed_ids = set()
    for cmd_id in executed_commands:
        if isinstance(cmd_id, str):
            executed_ids.add(cmd_id)
        elif isinstance(cmd_id, dict) and "id" in cmd_id:
            executed_ids.add(cmd_id["id"])
    
    updated_commands = []
    for cmd in commands:
        cmd_id = cmd.get("id")
        
        if cmd_id in executed_ids:
            acknowledged_count += 1
            print(f"[Command] Command {cmd_id} acknowledged by device")
            continue
        
        if is_command_expired(cmd, now):
            if cmd.get("status") != CommandStatus.EXPIRED.value:
                cmd["status"] = CommandStatus.EXPIRED.value
                expired_count += 1
                print(f"[Command] Command {cmd_id} expired (TTL: {COMMAND_TTL_SECONDS}s)")
            updated_commands.append(cmd)
            continue
        
        if cmd.get("status") == CommandStatus.PENDING.value:
            cmd["status"] = CommandStatus.DELIVERED.value
            cmd["delivered_at"] = now.isoformat()
            commands_to_deliver.append({
                "id": cmd["id"],
                "command": cmd["command"],
                "value": cmd["value"],
                "created_at": cmd["created_at"]
            })
            updated_commands.append(cmd)
        elif cmd.get("status") == CommandStatus.DELIVERED.value:
            updated_commands.append(cmd)
    
    device.pending_commands = updated_commands
    
    return commands_to_deliver, acknowledged_count, expired_count

@app.post("/devices/heartbeat/{device_id}")
def device_heartbeat(
    device_id: str,
    request: HeartbeatRequest = None,
    x_signature: str = Header(None, alias="X-Signature"),
    x_timestamp: str = Header(None, alias="X-Timestamp"),
    db: Session = Depends(get_db)
):
    if not x_signature or not x_timestamp:
        raise HTTPException(
            status_code=401,
            detail="Missing authentication headers: X-Signature and X-Timestamp are required"
        )
    
    device = db.query(Device).filter(
        Device.device_id == device_id
    ).first()
    
    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found. Please register the device first."
        )
    
    is_valid, error_msg = verify_signature(device_id, x_timestamp, x_signature, device.secret_key)
    if not is_valid:
        raise HTTPException(
            status_code=401,
            detail=f"Authentication failed: {error_msg}"
        )
    
    if request is None:
        request = HeartbeatRequest()
    
    now = datetime.utcnow()
    device.last_heartbeat = now
    device.status = DeviceStatus.ONLINE
    
    commands_to_deliver, acknowledged_count, expired_count = process_heartbeat_commands(
        device,
        request.executed_commands,
        now
    )
    
    db.commit()
    db.refresh(device)
    
    return HeartbeatResponse(
        device_id=device.device_id,
        status=device.status.value,
        last_heartbeat=device.last_heartbeat,
        pending_commands=commands_to_deliver,
        acknowledged_count=acknowledged_count,
        expired_count=expired_count
    )

@app.post("/devices/control/{device_id}")
def control_device(device_id: str, command: ControlCommand, db: Session = Depends(get_db)):
    device = db.query(Device).filter(
        Device.device_id == device_id
    ).first()
    
    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found."
        )
    
    if device.pending_commands is None:
        device.pending_commands = []
    
    new_cmd = create_command(command.command, command.value)
    device.pending_commands.append(new_cmd)
    db.commit()
    
    print(f"[Control] Command queued for device {device_id}: {command.command}={command.value}, id={new_cmd['id']}")
    
    return {
        "message": "Command queued successfully",
        "device_id": device.device_id,
        "command_id": new_cmd["id"],
        "command": command.command,
        "value": command.value,
        "status": new_cmd["status"],
        "queued_at": new_cmd["created_at"],
        "ttl_seconds": COMMAND_TTL_SECONDS
    }

@app.post("/devices/data")
def report_device_data(
    data_report: DeviceDataReport,
    x_signature: str = Header(None, alias="X-Signature"),
    x_timestamp: str = Header(None, alias="X-Timestamp"),
    db: Session = Depends(get_db)
):
    if not x_signature or not x_timestamp:
        raise HTTPException(
            status_code=401,
            detail="Missing authentication headers: X-Signature and X-Timestamp are required"
        )
    
    device = db.query(Device).filter(
        Device.device_id == data_report.device_id
    ).first()
    
    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found. Please register the device first."
        )
    
    is_valid, error_msg = verify_signature(data_report.device_id, x_timestamp, x_signature, device.secret_key)
    if not is_valid:
        raise HTTPException(
            status_code=401,
            detail=f"Authentication failed: {error_msg}"
        )
    
    payload = data_report.payload
    temperature = None
    humidity = None
    
    if 'temperature' in payload:
        temperature = payload['temperature']
    elif 'temp' in payload:
        temperature = payload['temp']
    
    if 'humidity' in payload:
        humidity = payload['humidity']
    
    is_alert = False
    if temperature is not None and isinstance(temperature, (int, float)):
        is_alert = temperature > TEMPERATURE_THRESHOLD
    
    new_data = DeviceData(
        id=str(uuid.uuid4()),
        device_id=data_report.device_id,
        payload=payload,
        recorded_at=datetime.utcnow()
    )
    db.add(new_data)
    
    history_record = DeviceDataHistory(
        id=str(uuid.uuid4()),
        device_id=data_report.device_id,
        payload=payload,
        timestamp=datetime.utcnow(),
        temperature=temperature,
        humidity=humidity,
        is_alert=is_alert
    )
    db.add(history_record)
    
    now = datetime.utcnow()
    device.last_heartbeat = now
    device.status = DeviceStatus.ONLINE
    db.commit()
    db.refresh(new_data)
    db.refresh(history_record)
    
    return {
        "message": "Data reported successfully",
        "data_id": new_data.id,
        "history_id": history_record.id,
        "device_id": new_data.device_id,
        "payload": new_data.payload,
        "recorded_at": new_data.recorded_at
    }

def get_latest_device_data(device_id: str, db: Session):
    latest_data = db.query(DeviceData).filter(
        DeviceData.device_id == device_id
    ).order_by(desc(DeviceData.recorded_at)).first()
    return latest_data

@app.get("/devices/{device_id}")
def get_device(device_id: str, db: Session = Depends(get_db)):
    device = db.query(Device).filter(
        Device.device_id == device_id
    ).first()
    
    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found"
        )
    
    check_device_status(device)
    
    latest_data = get_latest_device_data(device_id, db)
    
    return {
        "device_id": device.device_id,
        "model": device.model,
        "status": device.status.value,
        "last_heartbeat": device.last_heartbeat,
        "created_at": device.created_at,
        "latest_payload": latest_data.payload if latest_data else None,
        "latest_data_time": latest_data.recorded_at if latest_data else None
    }

@app.get("/devices")
def get_all_devices(db: Session = Depends(get_db)):
    devices = db.query(Device).all()
    
    result = []
    for device in devices:
        check_device_status(device)
        latest_data = get_latest_device_data(device.device_id, db)
        result.append({
            "device_id": device.device_id,
            "model": device.model,
            "status": device.status.value,
            "last_heartbeat": device.last_heartbeat,
            "latest_payload": latest_data.payload if latest_data else None,
            "latest_data_time": latest_data.recorded_at if latest_data else None
        })
    
    db.commit()
    return result

def check_device_status(device: Device):
    if device.last_heartbeat is None:
        return
    
    now = datetime.utcnow()
    time_since_heartbeat = (now - device.last_heartbeat).total_seconds()
    
    if time_since_heartbeat > HEARTBEAT_TIMEOUT:
        device.status = DeviceStatus.OFFLINE

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode('utf-8')).hexdigest()

def verify_password(password: str, password_hash: str) -> bool:
    return hash_password(password) == password_hash

class UserRegister(BaseModel):
    username: str
    password: str

class UserLogin(BaseModel):
    username: str
    password: str

class ComplaintCreate(BaseModel):
    title: str
    content: str

class ComplaintUpdate(BaseModel):
    reply_content: str = None
    status: str = None

def get_current_user(db: Session = Depends(get_db)) -> User:
    from fastapi import Header
    user_id = None
    
    async def get_user(x_user_id: str = Header(None, alias="X-User-ID")):
        if not x_user_id:
            raise HTTPException(
                status_code=401,
                detail="User authentication required. Please provide X-User-ID header."
            )
        user = db.query(User).filter(User.id == x_user_id).first()
        if not user:
            raise HTTPException(
                status_code=401,
                detail="Invalid user ID."
            )
        return user
    
    return None

@app.post("/users/register")
def register_user(user_data: UserRegister, db: Session = Depends(get_db)):
    existing_user = db.query(User).filter(
        User.username == user_data.username
    ).first()
    
    if existing_user:
        raise HTTPException(
            status_code=400,
            detail="Username already exists."
        )
    
    new_user = User(
        id=str(uuid.uuid4()),
        username=user_data.username,
        password_hash=hash_password(user_data.password),
        role=UserRole.GUEST
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    
    return {
        "message": "User registered successfully",
        "user_id": new_user.id,
        "username": new_user.username,
        "role": new_user.role.value
    }

@app.post("/users/login")
def login_user(login_data: UserLogin, db: Session = Depends(get_db)):
    user = db.query(User).filter(
        User.username == login_data.username
    ).first()
    
    if not user or not verify_password(login_data.password, user.password_hash):
        raise HTTPException(
            status_code=401,
            detail="Invalid username or password."
        )
    
    return {
        "message": "Login successful",
        "user_id": user.id,
        "username": user.username,
        "role": user.role.value
    }

@app.get("/users/{user_id}")
def get_user_info(user_id: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=404,
            detail="User not found."
        )
    
    return {
        "user_id": user.id,
        "username": user.username,
        "role": user.role.value,
        "created_at": user.created_at
    }

@app.post("/users/create-admin")
def create_admin_user(db: Session = Depends(get_db)):
    admin_user = db.query(User).filter(User.username == "admin").first()
    if admin_user:
        return {
            "message": "Admin user already exists",
            "user_id": admin_user.id,
            "username": admin_user.username,
            "role": admin_user.role.value
        }
    
    new_admin = User(
        id=str(uuid.uuid4()),
        username="admin",
        password_hash=hash_password("admin123"),
        role=UserRole.ADMIN
    )
    db.add(new_admin)
    
    staff_user = User(
        id=str(uuid.uuid4()),
        username="staff",
        password_hash=hash_password("staff123"),
        role=UserRole.STAFF
    )
    db.add(staff_user)
    
    db.commit()
    
    return {
        "message": "Admin and Staff users created successfully",
        "users": [
            {"user_id": new_admin.id, "username": "admin", "role": "ADMIN", "password": "admin123"},
            {"user_id": staff_user.id, "username": "staff", "role": "STAFF", "password": "staff123"}
        ]
    }

def require_staff_or_admin(user_id: str, db: Session):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=401,
            detail="User not found."
        )
    if user.role not in [UserRole.STAFF, UserRole.ADMIN]:
        raise HTTPException(
            status_code=403,
            detail="Permission denied. Staff or Admin role required."
        )
    return user

@app.post("/complaints")
def create_complaint(
    complaint_data: ComplaintCreate,
    x_user_id: str = Header(None, alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    if not x_user_id:
        raise HTTPException(
            status_code=401,
            detail="User authentication required. Please provide X-User-ID header."
        )
    
    user = db.query(User).filter(User.id == x_user_id).first()
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Invalid user ID."
        )
    
    new_complaint = Complaint(
        id=str(uuid.uuid4()),
        user_id=user.id,
        title=complaint_data.title,
        content=complaint_data.content,
        status=ComplaintStatus.PENDING
    )
    db.add(new_complaint)
    db.commit()
    db.refresh(new_complaint)
    
    return {
        "message": "Complaint created successfully",
        "complaint_id": new_complaint.id,
        "title": new_complaint.title,
        "content": new_complaint.content,
        "status": new_complaint.status.value,
        "created_at": new_complaint.created_at
    }

@app.get("/complaints")
def get_all_complaints(
    x_user_id: str = Header(None, alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    if x_user_id:
        user = db.query(User).filter(User.id == x_user_id).first()
        if user and user.role in [UserRole.STAFF, UserRole.ADMIN]:
            complaints = db.query(Complaint).order_by(
                desc(Complaint.created_at)
            ).all()
            result = []
            for complaint in complaints:
                replies = db.query(ComplaintReply).filter(
                    ComplaintReply.complaint_id == complaint.id
                ).order_by(ComplaintReply.created_at).all()
                
                user = db.query(User).filter(User.id == complaint.user_id).first()
                
                result.append({
                    "complaint_id": complaint.id,
                    "user_id": complaint.user_id,
                    "username": user.username if user else None,
                    "title": complaint.title,
                    "content": complaint.content,
                    "status": complaint.status.value,
                    "created_at": complaint.created_at,
                    "updated_at": complaint.updated_at,
                    "replies": [
                        {
                            "id": r.id,
                            "user_id": r.user_id,
                            "content": r.content,
                            "created_at": r.created_at
                        }
                        for r in replies
                    ]
                })
            return result
    
    if not x_user_id:
        raise HTTPException(
            status_code=401,
            detail="User authentication required."
        )
    
    complaints = db.query(Complaint).filter(
        Complaint.user_id == x_user_id
    ).order_by(desc(Complaint.created_at)).all()
    
    result = []
    for complaint in complaints:
        replies = db.query(ComplaintReply).filter(
            ComplaintReply.complaint_id == complaint.id
        ).order_by(ComplaintReply.created_at).all()
        
        result.append({
            "complaint_id": complaint.id,
            "title": complaint.title,
            "content": complaint.content,
            "status": complaint.status.value,
            "created_at": complaint.created_at,
            "updated_at": complaint.updated_at,
            "replies": [
                {
                    "id": r.id,
                    "user_id": r.user_id,
                    "content": r.content,
                    "created_at": r.created_at
                }
                for r in replies
            ]
        })
    return result

@app.get("/complaints/{complaint_id}")
def get_complaint_detail(
    complaint_id: str,
    x_user_id: str = Header(None, alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    complaint = db.query(Complaint).filter(
        Complaint.id == complaint_id
    ).first()
    
    if not complaint:
        raise HTTPException(
            status_code=404,
            detail="Complaint not found."
        )
    
    if x_user_id:
        user = db.query(User).filter(User.id == x_user_id).first()
        if user and user.role in [UserRole.STAFF, UserRole.ADMIN]:
            pass
        elif complaint.user_id != x_user_id:
            raise HTTPException(
                status_code=403,
                detail="Permission denied."
            )
    else:
        raise HTTPException(
            status_code=401,
            detail="User authentication required."
        )
    
    replies = db.query(ComplaintReply).filter(
        ComplaintReply.complaint_id == complaint.id
    ).order_by(ComplaintReply.created_at).all()
    
    return {
        "complaint_id": complaint.id,
        "user_id": complaint.user_id,
        "title": complaint.title,
        "content": complaint.content,
        "status": complaint.status.value,
        "created_at": complaint.created_at,
        "updated_at": complaint.updated_at,
        "replies": [
            {
                "id": r.id,
                "user_id": r.user_id,
                "content": r.content,
                "created_at": r.created_at
            }
            for r in replies
        ]
    }

@app.patch("/complaints/{complaint_id}")
def update_complaint(
    complaint_id: str,
    update_data: ComplaintUpdate,
    x_user_id: str = Header(None, alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    if not x_user_id:
        raise HTTPException(
            status_code=401,
            detail="User authentication required."
        )
    
    require_staff_or_admin(x_user_id, db)
    
    complaint = db.query(Complaint).filter(
        Complaint.id == complaint_id
    ).first()
    
    if not complaint:
        raise HTTPException(
            status_code=404,
            detail="Complaint not found."
        )
    
    if update_data.reply_content is not None:
        if not update_data.reply_content or update_data.reply_content.strip() == "":
            raise HTTPException(
                status_code=422,
                detail="Reply content cannot be empty."
            )
        
        new_reply = ComplaintReply(
            id=str(uuid.uuid4()),
            complaint_id=complaint.id,
            user_id=x_user_id,
            content=update_data.reply_content
        )
        db.add(new_reply)
    
    if update_data.status:
        status_map = {
            "pending": ComplaintStatus.PENDING,
            "in_progress": ComplaintStatus.IN_PROGRESS,
            "resolved": ComplaintStatus.RESOLVED
        }
        if update_data.status not in status_map:
            raise HTTPException(
                status_code=400,
                detail="Invalid status. Must be one of: pending, in_progress, resolved"
            )
        complaint.status = status_map[update_data.status]
    
    db.commit()
    db.refresh(complaint)
    
    replies = db.query(ComplaintReply).filter(
        ComplaintReply.complaint_id == complaint.id
    ).order_by(ComplaintReply.created_at).all()
    
    return {
        "message": "Complaint updated successfully",
        "complaint_id": complaint.id,
        "status": complaint.status.value,
        "updated_at": complaint.updated_at,
        "replies": [
            {
                "id": r.id,
                "user_id": r.user_id,
                "content": r.content,
                "created_at": r.created_at
            }
            for r in replies
        ]
    }

MIN_INTERVAL_SECONDS = 10

@app.get("/devices/{device_id}/history")
def get_device_history(
    device_id: str,
    start_time: str = Query(..., description="Start time in ISO format (e.g., 2026-04-20T00:00:00)"),
    end_time: str = Query(..., description="End time in ISO format (e.g., 2026-04-22T00:00:00)"),
    interval_seconds: int = Query(60, ge=1, description="Aggregation interval in seconds (minimum 10s enforced)"),
    db: Session = Depends(get_db)
):
    try:
        start_dt = datetime.fromisoformat(start_time)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid start_time format. Use ISO format: YYYY-MM-DDTHH:MM:SS"
        )
    
    try:
        end_dt = datetime.fromisoformat(end_time)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid end_time format. Use ISO format: YYYY-MM-DDTHH:MM:SS"
        )
    
    if start_dt > end_dt:
        raise HTTPException(
            status_code=422,
            detail="start_time cannot be later than end_time"
        )
    
    if interval_seconds < MIN_INTERVAL_SECONDS:
        raise HTTPException(
            status_code=422,
            detail=f"interval_seconds must be at least {MIN_INTERVAL_SECONDS} seconds"
        )
    
    device = db.query(Device).filter(Device.device_id == device_id).first()
    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found"
        )
    
    sql_query = text("""
        SELECT
            datetime(
                (strftime('%s', timestamp) / :interval) * :interval,
                'unixepoch'
            ) as window_start,
            COUNT(*) as record_count,
            MAX(temperature) as temp_max,
            MIN(temperature) as temp_min,
            AVG(temperature) as temp_avg,
            MAX(humidity) as humidity_max,
            MIN(humidity) as humidity_min,
            AVG(humidity) as humidity_avg,
            MAX(CASE WHEN is_alert = 1 THEN 1 ELSE 0 END) as has_alert
        FROM device_data_history
        WHERE 
            device_id = :device_id
            AND timestamp >= :start_time
            AND timestamp <= :end_time
            AND temperature IS NOT NULL
        GROUP BY window_start
        ORDER BY window_start ASC
    """)
    
    result = db.execute(
        sql_query,
        {
            "device_id": device_id,
            "start_time": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "interval": interval_seconds
        }
    )
    
    data_points = []
    for row in result:
        window_start = row[0]
        record_count = row[1]
        temp_max = row[2]
        temp_min = row[3]
        temp_avg = row[4]
        humidity_max = row[5]
        humidity_min = row[6]
        humidity_avg = row[7]
        has_alert = row[8] == 1
        
        try:
            window_dt = datetime.strptime(window_start, "%Y-%m-%d %H:%M:%S")
            window_end_dt = window_dt + timedelta(seconds=interval_seconds)
            window_end = window_end_dt.strftime("%Y-%m-%dT%H:%M:%S")
            window_start_iso = window_dt.strftime("%Y-%m-%dT%H:%M:%S")
        except:
            window_start_iso = window_start
            window_end = window_start
        
        data_point = {
            "window_start": window_start_iso,
            "window_end": window_end,
            "interval_seconds": interval_seconds,
            "record_count": record_count,
            "temperature": {
                "max": float(temp_max) if temp_max is not None else None,
                "min": float(temp_min) if temp_min is not None else None,
                "avg": float(temp_avg) if temp_avg is not None else None,
                "count": record_count
            },
            "humidity": {
                "max": float(humidity_max) if humidity_max is not None else None,
                "min": float(humidity_min) if humidity_min is not None else None,
                "avg": float(humidity_avg) if humidity_avg is not None else None,
                "count": record_count
            },
            "has_alert": has_alert
        }
        data_points.append(data_point)
    
    return {
        "device_id": device_id,
        "time_range": {
            "start": start_time,
            "end": end_time
        },
        "interval_seconds": interval_seconds,
        "min_interval_seconds": MIN_INTERVAL_SECONDS,
        "total_data_points": len(data_points),
        "data_points": data_points
    }
