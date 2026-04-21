from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import os
import uuid
import hashlib
import secrets
import string

from .models import Base, Device, DeviceStatus, DeviceData, CommandStatus

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
                                reason=f"Temperature exceeded threshold: {temperature}°C"
                            )
                            device.pending_commands.append(alert_cmd)
                            alert_count += 1
                            print(f"[Alert] Device {device.device_id}: Temperature {temperature}°C exceeds {TEMPERATURE_THRESHOLD}°C - Triggering alert_buzzer (id={alert_cmd['id']})")
        
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
        current_timestamp = int(datetime.utcnow().timestamp())
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
    
    new_data = DeviceData(
        id=str(uuid.uuid4()),
        device_id=data_report.device_id,
        payload=data_report.payload,
        recorded_at=datetime.utcnow()
    )
    db.add(new_data)
    
    now = datetime.utcnow()
    device.last_heartbeat = now
    device.status = DeviceStatus.ONLINE
    db.commit()
    db.refresh(new_data)
    
    return {
        "message": "Data reported successfully",
        "data_id": new_data.id,
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
