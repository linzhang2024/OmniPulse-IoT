from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import os

from .models import Base, Device, DeviceStatus

DATABASE_URL = "sqlite:///./iot_devices.db"
HEARTBEAT_TIMEOUT = 30
CHECK_INTERVAL = 10

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)

scheduler = AsyncIOScheduler()

def check_all_devices_status():
    db = SessionLocal()
    try:
        devices = db.query(Device).filter(
            Device.status == DeviceStatus.ONLINE
        ).all()
        
        now = datetime.utcnow()
        updated_count = 0
        
        for device in devices:
            if device.last_heartbeat is None:
                continue
            
            time_since_heartbeat = (now - device.last_heartbeat).total_seconds()
            
            if time_since_heartbeat > HEARTBEAT_TIMEOUT:
                device.status = DeviceStatus.OFFLINE
                updated_count += 1
        
        if updated_count > 0:
            db.commit()
            print(f"[Scheduler] Updated {updated_count} devices to OFFLINE status")
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

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class DeviceRegister(BaseModel):
    device_id: str
    model: str

class HeartbeatResponse(BaseModel):
    device_id: str
    status: str
    last_heartbeat: datetime

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
    db.commit()
    db.refresh(device)
    
    return HeartbeatResponse(
        device_id=device.device_id,
        status=device.status.value,
        last_heartbeat=device.last_heartbeat
    )

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
    
    return {
        "device_id": device.device_id,
        "model": device.model,
        "status": device.status.value,
        "last_heartbeat": device.last_heartbeat,
        "created_at": device.created_at
    }

@app.get("/devices")
def get_all_devices(db: Session = Depends(get_db)):
    devices = db.query(Device).all()
    
    result = []
    for device in devices:
        check_device_status(device)
        result.append({
            "device_id": device.device_id,
            "model": device.model,
            "status": device.status.value,
            "last_heartbeat": device.last_heartbeat
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
