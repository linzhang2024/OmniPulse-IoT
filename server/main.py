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
import re
import json
import struct

from .models import (
    Base, Device, DeviceStatus, DeviceData, DeviceDataHistory, CommandStatus,
    User, UserRole, Complaint, ComplaintStatus, ComplaintReply,
    DeviceProtocol, ProtocolType
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

def parse_hex_payload(raw_payload: str, parse_config: dict) -> dict:
    """
    解析 HEX 格式的原始数据
    parse_config 支持:
    - byte_order: 字节序 ('big' 或 'little')
    - fields: 字段定义，包含 offset(字节偏移)、length(字节数)、type(数据类型)
    """
    cleaned_hex = re.sub(r'[\s\-:,]', '', raw_payload).upper()
    
    if not re.match(r'^[0-9A-F]+$', cleaned_hex):
        raise ValueError(f"Invalid HEX format: {raw_payload}")
    
    if len(cleaned_hex) % 2 != 0:
        raise ValueError(f"HEX string has odd length: {len(cleaned_hex)}")
    
    byte_data = bytes.fromhex(cleaned_hex)
    result = {}
    
    fields = parse_config.get('fields', [])
    byte_order = parse_config.get('byte_order', 'big')
    
    for field in fields:
        field_name = field.get('name')
        offset = field.get('offset', 0)
        length = field.get('length', 1)
        data_type = field.get('type', 'uint')
        
        end_offset = offset + length
        if end_offset > len(byte_data):
            continue
        
        field_bytes = byte_data[offset:end_offset]
        
        if data_type == 'uint':
            value = int.from_bytes(field_bytes, byteorder=byte_order, signed=False)
        elif data_type == 'int':
            value = int.from_bytes(field_bytes, byteorder=byte_order, signed=True)
        elif data_type == 'float':
            if length == 4:
                value = struct.unpack('>f' if byte_order == 'big' else '<f', field_bytes)[0]
            elif length == 8:
                value = struct.unpack('>d' if byte_order == 'big' else '<d', field_bytes)[0]
            else:
                value = None
        elif data_type == 'bcd':
            value = 0
            for b in field_bytes:
                value = value * 100 + ((b >> 4) * 10 + (b & 0x0F))
        else:
            value = int.from_bytes(field_bytes, byteorder=byte_order, signed=False)
        
        if value is not None:
            result[field_name] = value
    
    return result

def parse_string_payload(raw_payload: str, parse_config: dict) -> dict:
    """
    解析字符串格式的原始数据
    parse_config 支持:
    - delimiter: 分隔符 (如 ',' 或 ' ')
    - pattern: 正则表达式模式，使用命名组
    - fields: 字段索引映射，如 {"temperature": 0, "humidity": 1}
    """
    result = {}
    
    if 'pattern' in parse_config:
        pattern = parse_config['pattern']
        match = re.match(pattern, raw_payload)
        if match:
            result = match.groupdict()
    
    elif 'delimiter' in parse_config:
        delimiter = parse_config['delimiter']
        parts = raw_payload.split(delimiter)
        fields = parse_config.get('fields', {})
        
        for field_name, index in fields.items():
            if isinstance(index, int) and 0 <= index < len(parts):
                value = parts[index].strip()
                try:
                    if '.' in value or 'e' in value.lower():
                        value = float(value)
                    else:
                        value = int(value)
                except ValueError:
                    pass
                result[field_name] = value
    
    elif 'json_path' in parse_config:
        try:
            json_data = json.loads(raw_payload)
            mappings = parse_config.get('json_path', {})
            for field_name, path in mappings.items():
                keys = path.split('.')
                value = json_data
                for key in keys:
                    if isinstance(value, dict) and key in value:
                        value = value[key]
                    elif isinstance(value, list) and key.isdigit():
                        idx = int(key)
                        if 0 <= idx < len(value):
                            value = value[idx]
                        else:
                            value = None
                            break
                    else:
                        value = None
                        break
                if value is not None:
                    result[field_name] = value
        except json.JSONDecodeError:
            pass
    
    return result

def apply_transform_formulas(parsed_data: dict, transform_formulas: dict) -> dict:
    """
    应用转换公式将原始值转换为物理单位
    transform_formulas 格式示例:
    {
        "temperature": {
            "formula": "val * 0.1",
            "input_range": [0, 1023],
            "output_range": [0, 100]
        },
        "humidity": {
            "formula": "val * 0.0976",
            "input_range": [0, 1023]
        }
    }
    
    支持的公式变量:
    - val: 原始值
    - 支持基本数学运算: +, -, *, /, **, ()
    """
    result = dict(parsed_data)
    
    for field_name, config in transform_formulas.items():
        if field_name not in parsed_data:
            continue
        
        raw_value = parsed_data[field_name]
        
        if not isinstance(raw_value, (int, float)):
            continue
        
        input_range = config.get('input_range')
        if input_range and len(input_range) == 2:
            min_val, max_val = input_range
            if raw_value < min_val or raw_value > max_val:
                continue
        
        formula = config.get('formula')
        if formula:
            try:
                safe_vars = {
                    'val': raw_value,
                    'abs': abs,
                    'min': min,
                    'max': max,
                    'int': int,
                    'float': float,
                    'round': round
                }
                
                transformed_value = eval(formula, {"__builtins__": {}}, safe_vars)
                
                output_range = config.get('output_range')
                if output_range and len(output_range) == 2:
                    out_min, out_max = output_range
                    transformed_value = max(out_min, min(out_max, transformed_value))
                
                result[field_name] = transformed_value
                
            except Exception as e:
                print(f"[Transform] Error applying formula to {field_name}: {e}")
                continue
    
    return result

def get_device_protocol(device_id: str, db: Session) -> DeviceProtocol:
    """
    获取设备的活跃协议配置
    """
    protocol = db.query(DeviceProtocol).filter(
        DeviceProtocol.device_id == device_id,
        DeviceProtocol.is_active == True
    ).first()
    return protocol

def parse_raw_payload(raw_payload: str, protocol: DeviceProtocol) -> dict:
    """
    根据协议配置解析原始数据
    """
    if not protocol:
        return {}
    
    protocol_type = protocol.protocol_type
    parse_config = protocol.parse_config or {}
    
    if protocol_type == ProtocolType.HEX:
        return parse_hex_payload(raw_payload, parse_config)
    elif protocol_type == ProtocolType.STRING:
        return parse_string_payload(raw_payload, parse_config)
    elif protocol_type == ProtocolType.JSON:
        try:
            return json.loads(raw_payload)
        except json.JSONDecodeError:
            return {}
    else:
        return {}

def apply_field_mappings(parsed_data: dict, field_mappings: dict) -> dict:
    """
    应用字段映射，将解析出的字段映射到标准字段名
    field_mappings 格式示例:
    {
        "temp": "temperature",
        "hum": "humidity",
        "t": "temperature"
    }
    """
    result = dict(parsed_data)
    
    for source_field, target_field in field_mappings.items():
        if source_field in parsed_data and source_field != target_field:
            result[target_field] = parsed_data[source_field]
    
    return result

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
    payload: dict = None
    raw_payload: str = None

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
    parsed_info = {}
    
    if data_report.raw_payload is not None and data_report.raw_payload != "":
        protocol = get_device_protocol(data_report.device_id, db)
        
        if protocol is None:
            raise HTTPException(
                status_code=400,
                detail=f"No active protocol configured for device {data_report.device_id}. "
                       f"Please configure a DeviceProtocol for this device to use raw_payload."
            )
        
        try:
            parsed_data = parse_raw_payload(data_report.raw_payload, protocol)
            
            field_mappings = protocol.field_mappings or {}
            mapped_data = apply_field_mappings(parsed_data, field_mappings)
            
            transform_formulas = protocol.transform_formulas or {}
            final_data = apply_transform_formulas(mapped_data, transform_formulas)
            
            payload = final_data
            
            parsed_info = {
                "raw_payload": data_report.raw_payload,
                "protocol_type": protocol.protocol_type.value,
                "parsed_data": parsed_data,
                "mapped_data": mapped_data,
                "final_payload": final_data
            }
            
            print(f"[Protocol Adapter] Device {data_report.device_id} parsed raw_payload: "
                  f"raw={data_report.raw_payload} -> final={final_data}")
            
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to parse raw_payload: {str(e)}"
            )
    
    if payload is None:
        raise HTTPException(
            status_code=400,
            detail="Either 'payload' or 'raw_payload' must be provided"
        )
    
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
    
    full_payload = dict(payload)
    if parsed_info:
        full_payload["_raw_parsed"] = parsed_info
    
    new_data = DeviceData(
        id=str(uuid.uuid4()),
        device_id=data_report.device_id,
        payload=full_payload,
        recorded_at=datetime.utcnow()
    )
    db.add(new_data)
    
    history_record = DeviceDataHistory(
        id=str(uuid.uuid4()),
        device_id=data_report.device_id,
        payload=full_payload,
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
    
    response_data = {
        "message": "Data reported successfully",
        "data_id": new_data.id,
        "history_id": history_record.id,
        "device_id": new_data.device_id,
        "payload": new_data.payload,
        "recorded_at": new_data.recorded_at
    }
    
    if parsed_info:
        response_data["protocol_info"] = {
            "protocol_type": parsed_info.get("protocol_type"),
            "raw_payload_length": len(data_report.raw_payload) if data_report.raw_payload else 0
        }
    
    return response_data

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
    """
    获取设备历史数据的降采样接口 - 生产级性能实现
    
    核心设计原则：
    1. 数据库层面聚合：所有聚合操作在 SQLite 层面完成，不加载全量数据到 Python 内存
    2. 多维聚合：每个时间窗口返回 max/min/avg/count
    3. 告警穿透：MAX(CASE WHEN is_alert = 1 THEN 1 ELSE 0 END)
    
    性能保证：
    - 即使查询时间范围是一年，也不会造成内存溢出
    - 时间窗口由 GROUP BY 操作在数据库层面执行
    - 只返回聚合后的少量数据点
    
    边界保护：
    - start_time > end_time: 返回 422 错误
    - interval_seconds < 10: 返回 422 错误（防止恶意请求）
    """
    
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
    
    """
    ============================================================================
    数据库层面聚合 SQL 语句 - 核心性能调优确认
    ============================================================================
    
    关键设计说明：
    1. 所有聚合操作都在 SQLite 引擎层面完成，不加载原始数据到 Python 内存
    2. 时间窗口计算使用 strftime + 整数除法实现降采样
    3. 告警穿透使用 MAX(CASE WHEN is_alert = 1 THEN 1 ELSE 0 END)
    
    避免的问题：
    - 不会将 10000 条原始数据加载到 Python 内存
    - 不会在 Python 中进行循环计算
    - 内存占用恒定，与数据量无关
    
    SQL 语句详解：
    
    SELECT
        -- 1. 时间窗口计算：将时间戳整除 interval 秒，再乘以 interval 秒
        --    例如：interval=60，则 12:34:56 -> 12:34:00
        datetime(
            (strftime('%s', timestamp) / :interval) * :interval,
            'unixepoch'
        ) as window_start,
        
        -- 2. 多维聚合：每个时间窗口内的统计指标
        COUNT(*) as record_count,           -- 该窗口内的原始记录数
        MAX(temperature) as temp_max,       -- 该窗口内温度最大值（峰值预警）
        MIN(temperature) as temp_min,       -- 该窗口内温度最小值
        AVG(temperature) as temp_avg,       -- 该窗口内温度平均值
        MAX(humidity) as humidity_max,
        MIN(humidity) as humidity_min,
        AVG(humidity) as humidity_avg,
        
        -- 3. 告警穿透逻辑：MAX(CASE WHEN ...)
        --    只要窗口内有任何一条 is_alert=1 的记录，返回 1
        --    否则返回 0
        --    IoT 价值：峰值告警不会被平均淹没
        MAX(CASE WHEN is_alert = 1 THEN 1 ELSE 0 END) as has_alert
        
    FROM device_data_history
    WHERE 
        device_id = :device_id
        AND timestamp >= :start_time
        AND timestamp <= :end_time
        AND temperature IS NOT NULL          -- 过滤无效数据
    GROUP BY window_start                    -- 按时间窗口聚合
    ORDER BY window_start ASC               -- 按时间排序
    
    性能优化：
    - device_data_history 表有索引：
      - ix_device_data_history_device_id (设备过滤)
      - ix_device_data_history_timestamp (时间范围过滤)
      - ix_device_data_history_temperature (聚合列)
      - ix_device_data_history_is_alert (告警过滤)
    ============================================================================
    """
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
