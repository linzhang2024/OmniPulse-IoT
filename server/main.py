from fastapi import FastAPI, Depends, HTTPException
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

from .models import Base, Device, DeviceStatus, DeviceData

DATABASE_URL = "sqlite:///./iot_devices.db"
HEARTBEAT_TIMEOUT = 30
CHECK_INTERVAL = 10

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
                        
                        existing_alert = any(
                            cmd.get('command') == 'alert_buzzer' 
                            for cmd in device.pending_commands
                        )
                        
                        if not existing_alert:
                            device.pending_commands.append({
                                "command": "alert_buzzer",
                                "value": "on",
                                "timestamp": datetime.utcnow().isoformat(),
                                "reason": f"Temperature exceeded threshold: {temperature}°C"
                            })
                            alert_count += 1
                            print(f"[Alert] Device {device.device_id}: Temperature {temperature}°C exceeds {TEMPERATURE_THRESHOLD}°C - Triggering alert_buzzer")
        
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

class HeartbeatResponse(BaseModel):
    device_id: str
    status: str
    last_heartbeat: datetime
    pending_commands: list = []

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
            "status": existing_device.status.value
        }
    
    new_device = Device(
        device_id=device_data.device_id,
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
        "status": new_device.status.value
    }

@app.post("/devices/heartbeat/{device_id}")
def device_heartbeat(device_id: str, db: Session = Depends(get_db)):
    device = db.query(Device).filter(
        Device.device_id == device_id
    ).first()
    
    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found. Please register the device first."
        )
    
    now = datetime.utcnow()
    device.last_heartbeat = now
    device.status = DeviceStatus.ONLINE
    
    pending_commands = device.pending_commands or []
    device.pending_commands = []
    
    db.commit()
    db.refresh(device)
    
    return HeartbeatResponse(
        device_id=device.device_id,
        status=device.status.value,
        last_heartbeat=device.last_heartbeat,
        pending_commands=pending_commands
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
    
    device.pending_commands.append({
        "command": command.command,
        "value": command.value,
        "timestamp": datetime.utcnow().isoformat()
    })
    db.commit()
    
    return {
        "message": "Command queued successfully",
        "device_id": device.device_id,
        "command": command.command,
        "value": command.value,
        "queued_at": datetime.utcnow().isoformat()
    }

@app.post("/devices/data")
def report_device_data(data_report: DeviceDataReport, db: Session = Depends(get_db)):
    device = db.query(Device).filter(
        Device.device_id == data_report.device_id
    ).first()
    
    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found. Please register the device first."
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
