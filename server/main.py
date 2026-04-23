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
from collections import deque
import os
import uuid
import hashlib
import secrets
import string
import time
import re
import json
import struct
import logging
import threading
import asyncio
import atexit
import signal
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ProtocolAdapter")

from .models import (
    Base, Device, DeviceStatus, DeviceData, DeviceDataHistory, CommandStatus,
    User, UserRole, Complaint, ComplaintStatus, ComplaintReply,
    DeviceProtocol, ProtocolType, DeviceStatusEvent,
    AuditLog, OperationType, RiskLevel
)

DATABASE_URL = "sqlite:///./iot_devices.db"
PENDING_OFFLINE_THRESHOLD = 60
OFFLINE_THRESHOLD = 120
HEARTBEAT_TIMEOUT = OFFLINE_THRESHOLD
CHECK_INTERVAL = 30
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

MAX_MEMORY_EVENTS = 50
WEBHOOK_URL = os.getenv("DEVICE_WEBHOOK_URL", "http://localhost:8080/api/device-status")
WEBHOOK_TIMEOUT = 5

memory_event_queue = deque(maxlen=MAX_MEMORY_EVENTS)
memory_event_lock = threading.Lock()

class NotificationEngine:
    def __init__(self):
        self.webhook_url = WEBHOOK_URL
        self.webhook_timeout = WEBHOOK_TIMEOUT
        self.enabled = False
        self._lock = threading.Lock()
    
    def _print_alert_banner(self, device_id: str, event_type: str, details: dict):
        banner = "\n"
        banner += "╔" + "═" * 78 + "╗\n"
        banner += "║" + " " * 78 + "║\n"
        
        if event_type == "offline":
            banner += "║  ⚠️  DEVICE OFFLINE ALERT  ⚠️" + " " * 42 + "║\n"
        elif event_type == "pending_offline":
            banner += "║  ⚠️  DEVICE PENDING OFFLINE WARNING  ⚠️" + " " * 33 + "║\n"
        elif event_type == "online":
            banner += "║  ✅  DEVICE ONLINE  ✅" + " " * 47 + "║\n"
        
        banner += "║" + " " * 78 + "║\n"
        banner += f"║  Device ID: {device_id:<63}║\n"
        banner += "║" + "-" * 78 + "║\n"
        
        for key, value in details.items():
            value_str = str(value)
            if len(value_str) > 45:
                value_str = value_str[:42] + "..."
            banner += f"║  {key}: {value_str:<55 - len(key)}║\n"
        
        banner += "║" + " " * 78 + "║\n"
        banner += "╚" + "═" * 78 + "╝\n"
        
        print(banner)
    
    def _call_webhook(self, device_id: str, event_type: str, details: dict):
        if not self.enabled:
            logger.debug(f"[NotificationEngine] Webhook disabled, skipping for device {device_id}")
            return
        
        try:
            import httpx
            
            payload = {
                "device_id": device_id,
                "event_type": event_type,
                "timestamp": datetime.utcnow().isoformat(),
                "details": details
            }
            
            logger.info(f"[NotificationEngine] Calling webhook for device {device_id}, event: {event_type}")
            logger.info(f"[NotificationEngine] Webhook URL: {self.webhook_url}")
            logger.info(f"[NotificationEngine] Payload: {json.dumps(payload, indent=2)}")
            
            logger.info(f"[NotificationEngine] Webhook call simulated (would be real in production)")
            
        except ImportError:
            logger.warning(f"[NotificationEngine] httpx not installed, webhook call skipped")
        except Exception as e:
            logger.error(f"[NotificationEngine] Webhook call failed: {e}")
    
    def notify(self, device_id: str, event_type: str, old_status: str = None, 
               new_status: str = None, reason: str = None, details: dict = None):
        with self._lock:
            event_details = {
                "old_status": old_status,
                "new_status": new_status,
                "reason": reason,
                **(details or {})
            }
            
            self._print_alert_banner(device_id, event_type, event_details)
            
            if event_type == "offline":
                self._call_webhook(device_id, event_type, event_details)
            
            logger.info(f"[NotificationEngine] Notification sent: device={device_id}, event={event_type}")

notification_engine = NotificationEngine()

def add_event_to_memory(event: dict):
    with memory_event_lock:
        event["_added_at"] = datetime.utcnow().isoformat()
        memory_event_queue.append(event)
        logger.debug(f"[MemoryQueue] Added event to memory queue, queue size: {len(memory_event_queue)}")

def get_events_from_memory(device_id: str = None, event_type: str = None, 
                            since: datetime = None, limit: int = 100) -> list:
    with memory_event_lock:
        events = list(memory_event_queue)
    
    filtered = []
    for event in events:
        if device_id and event.get("device_id") != device_id:
            continue
        if event_type and event.get("event_type") != event_type:
            continue
        if since:
            added_at = event.get("_added_at")
            if added_at:
                try:
                    event_dt = datetime.fromisoformat(added_at)
                    if event_dt < since:
                        continue
                except ValueError:
                    continue
        
        filtered.append(event)
        
        if len(filtered) >= limit:
            break
    
    return filtered

def parse_hex_payload(raw_payload: str, parse_config: dict) -> dict:
    """
    解析 HEX 格式的原始数据
    parse_config 支持:
    - byte_order: 字节序 ('big' 或 'little')
    - fields: 字段定义，包含 offset(字节偏移)、length(字节数)、type(数据类型)
    
    增强的容错能力：
    - 捕获所有异常并返回明确的错误信息
    - 详细的日志记录，便于调试
    """
    logger.info(f"[HEX Parser] 开始解析 HEX 数据: {raw_payload}")
    
    if not raw_payload or not isinstance(raw_payload, str):
        error_msg = f"HEX 数据不能为空或无效类型，实际类型: {type(raw_payload)}"
        logger.error(f"[HEX Parser] {error_msg}")
        raise ValueError(error_msg)
    
    try:
        cleaned_hex = re.sub(r'[\s\-:,\.]', '', raw_payload).upper()
        logger.info(f"[HEX Parser] 清洗后的 HEX: {cleaned_hex}")
    except Exception as e:
        error_msg = f"清洗 HEX 数据时出错: {str(e)}"
        logger.error(f"[HEX Parser] {error_msg}")
        raise ValueError(error_msg)
    
    if not re.match(r'^[0-9A-F]*$', cleaned_hex):
        invalid_chars = set(cleaned_hex) - set('0123456789ABCDEF')
        error_msg = f"HEX 字符串包含非法字符: {invalid_chars}，原始数据: {raw_payload}"
        logger.error(f"[HEX Parser] {error_msg}")
        raise ValueError(error_msg)
    
    if len(cleaned_hex) == 0:
        error_msg = f"HEX 字符串清洗后为空，原始数据: {raw_payload}"
        logger.error(f"[HEX Parser] {error_msg}")
        raise ValueError(error_msg)
    
    if len(cleaned_hex) % 2 != 0:
        error_msg = f"HEX 字符串长度为奇数: {len(cleaned_hex)} 个字符 (应为偶数)，数据: {cleaned_hex}"
        logger.error(f"[HEX Parser] {error_msg}")
        raise ValueError(error_msg)
    
    try:
        byte_data = bytes.fromhex(cleaned_hex)
        logger.info(f"[HEX Parser] 转换为字节数组成功，共 {len(byte_data)} 字节")
    except ValueError as e:
        error_msg = f"HEX 字符串转换为字节失败: {str(e)}，数据: {cleaned_hex}"
        logger.error(f"[HEX Parser] {error_msg}")
        raise ValueError(error_msg)
    
    result = {}
    parse_details = []
    
    fields = parse_config.get('fields', [])
    byte_order = parse_config.get('byte_order', 'big')
    
    logger.info(f"[HEX Parser] 协议配置: 字节序={byte_order}, 字段数量={len(fields)}")
    
    for field_idx, field in enumerate(fields):
        field_name = field.get('name')
        offset = field.get('offset', 0)
        length = field.get('length', 1)
        data_type = field.get('type', 'uint')
        
        logger.info(f"[HEX Parser] 处理字段 [{field_idx}] '{field_name}': offset={offset}, length={length}, type={data_type}")
        
        if not field_name:
            logger.warning(f"[HEX Parser] 字段 {field_idx} 没有定义 name，跳过")
            continue
        
        if not isinstance(offset, int) or offset < 0:
            error_msg = f"字段 '{field_name}' 的 offset 必须是非负整数，实际: {offset}"
            logger.error(f"[HEX Parser] {error_msg}")
            raise ValueError(error_msg)
        
        if not isinstance(length, int) or length <= 0:
            error_msg = f"字段 '{field_name}' 的 length 必须是正整数，实际: {length}"
            logger.error(f"[HEX Parser] {error_msg}")
            raise ValueError(error_msg)
        
        end_offset = offset + length
        
        if offset >= len(byte_data):
            error_msg = (f"字段 '{field_name}' 的 offset={offset} 超出数据范围，"
                        f"数据仅 {len(byte_data)} 字节 (数据: {cleaned_hex})")
            logger.error(f"[HEX Parser] {error_msg}")
            raise ValueError(error_msg)
        
        if end_offset > len(byte_data):
            error_msg = (f"字段 '{field_name}' 长度不足: offset={offset}, length={length}, "
                        f"需要 {end_offset} 字节，但数据仅 {len(byte_data)} 字节 (数据: {cleaned_hex})")
            logger.error(f"[HEX Parser] {error_msg}")
            raise ValueError(error_msg)
        
        try:
            field_bytes = byte_data[offset:end_offset]
            hex_bytes_str = ' '.join([f'{b:02X}' for b in field_bytes])
            logger.info(f"[HEX Parser] 字段 '{field_name}' 原始字节: {hex_bytes_str}")
            
            value = None
            
            if data_type == 'uint':
                value = int.from_bytes(field_bytes, byteorder=byte_order, signed=False)
                parse_details.append(f"{field_name}: {hex_bytes_str} -> 无符号整数={value}")
                
            elif data_type == 'int':
                value = int.from_bytes(field_bytes, byteorder=byte_order, signed=True)
                parse_details.append(f"{field_name}: {hex_bytes_str} -> 有符号整数={value}")
                
            elif data_type == 'float':
                if length == 4:
                    value = struct.unpack('>f' if byte_order == 'big' else '<f', field_bytes)[0]
                    parse_details.append(f"{field_name}: {hex_bytes_str} -> 32位浮点数={value}")
                elif length == 8:
                    value = struct.unpack('>d' if byte_order == 'big' else '<d', field_bytes)[0]
                    parse_details.append(f"{field_name}: {hex_bytes_str} -> 64位浮点数={value}")
                else:
                    error_msg = f"字段 '{field_name}' 类型为 float 时，length 必须是 4 或 8，实际: {length}"
                    logger.error(f"[HEX Parser] {error_msg}")
                    raise ValueError(error_msg)
                    
            elif data_type == 'bcd':
                value = 0
                for i, b in enumerate(field_bytes):
                    high_nibble = (b >> 4) & 0x0F
                    low_nibble = b & 0x0F
                    if high_nibble > 9 or low_nibble > 9:
                        error_msg = f"字段 '{field_name}' 的 BCD 数据包含非法值: 字节 {i} = 0x{b:02X}"
                        logger.error(f"[HEX Parser] {error_msg}")
                        raise ValueError(error_msg)
                    value = value * 100 + (high_nibble * 10 + low_nibble)
                parse_details.append(f"{field_name}: {hex_bytes_str} -> BCD={value}")
                
            else:
                value = int.from_bytes(field_bytes, byteorder=byte_order, signed=False)
                parse_details.append(f"{field_name}: {hex_bytes_str} -> 未知类型(默认uint)={value}")
            
            if value is not None:
                result[field_name] = value
                logger.info(f"[HEX Parser] 字段 '{field_name}' 解析成功: {value}")
                
        except struct.error as e:
            error_msg = f"字段 '{field_name}' 解析浮点数时出错: {str(e)}"
            logger.error(f"[HEX Parser] {error_msg}")
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"字段 '{field_name}' 解析时发生未知错误: {str(e)}"
            logger.error(f"[HEX Parser] {error_msg}")
            raise ValueError(error_msg)
    
    logger.info(f"[HEX Parser] 解析完成，结果: {result}")
    logger.info(f"[HEX Parser] 解析详情: {'; '.join(parse_details)}")
    
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

def record_status_event(
    db: Session,
    device_id: str,
    event_type: str,
    old_status: str = None,
    new_status: str = None,
    reason: str = None,
    details: dict = None
):
    event_id = str(uuid.uuid4())
    event_details = details or {}
    
    event = DeviceStatusEvent(
        id=event_id,
        device_id=device_id,
        event_type=event_type,
        old_status=old_status,
        new_status=new_status,
        reason=reason,
        details=event_details
    )
    db.add(event)
    
    memory_event = {
        "id": event_id,
        "device_id": device_id,
        "event_type": event_type,
        "old_status": old_status,
        "new_status": new_status,
        "reason": reason,
        "details": event_details,
        "created_at": datetime.utcnow().isoformat()
    }
    add_event_to_memory(memory_event)
    
    notification_engine.notify(
        device_id=device_id,
        event_type=event_type,
        old_status=old_status,
        new_status=new_status,
        reason=reason,
        details=event_details
    )
    
    logger.info(f"[StatusEvent] Device {device_id}: {event_type} - {old_status} -> {new_status}. Reason: {reason}")

def check_all_devices_status():
    db = SessionLocal()
    try:
        devices = db.query(Device).all()
        
        now = datetime.utcnow()
        updated_count = 0
        pending_count = 0
        offline_count = 0
        alert_count = 0
        
        for device in devices:
            last_active_time = device.last_seen if device.last_seen else device.last_heartbeat
            
            if last_active_time is not None:
                time_since_active = (now - last_active_time).total_seconds()
                old_status = device.status.value
                
                if device.status == DeviceStatus.ONLINE:
                    if time_since_active > PENDING_OFFLINE_THRESHOLD and time_since_active <= OFFLINE_THRESHOLD:
                        device.status = DeviceStatus.PENDING_OFFLINE
                        new_status = device.status.value
                        pending_count += 1
                        updated_count += 1
                        
                        record_status_event(
                            db,
                            device.device_id,
                            "pending_offline",
                            old_status=old_status,
                            new_status=new_status,
                            reason=f"No heartbeat for {time_since_active:.1f} seconds (pending threshold: {PENDING_OFFLINE_THRESHOLD}s)",
                            details={
                                "last_active_time": last_active_time.isoformat() if last_active_time else None,
                                "pending_threshold": PENDING_OFFLINE_THRESHOLD,
                                "offline_threshold": OFFLINE_THRESHOLD,
                                "time_since_active": time_since_active
                            }
                        )
                        logger.warning(
                            f"[DevicePendingOffline] Device {device.device_id} marked as PENDING_OFFLINE. "
                            f"No activity for {time_since_active:.1f}s (threshold: {PENDING_OFFLINE_THRESHOLD}s). "
                            f"Will be marked OFFLINE in {OFFLINE_THRESHOLD - time_since_active:.1f}s."
                        )
                    
                    elif time_since_active > OFFLINE_THRESHOLD:
                        device.status = DeviceStatus.OFFLINE
                        new_status = device.status.value
                        offline_count += 1
                        updated_count += 1
                        
                        record_status_event(
                            db,
                            device.device_id,
                            "offline",
                            old_status=old_status,
                            new_status=new_status,
                            reason=f"No heartbeat for {time_since_active:.1f} seconds (timeout: {OFFLINE_THRESHOLD}s)",
                            details={
                                "last_active_time": last_active_time.isoformat() if last_active_time else None,
                                "timeout_seconds": OFFLINE_THRESHOLD,
                                "time_since_active": time_since_active
                            }
                        )
                        logger.critical(
                            f"\n{'='*60}\n"
                            f"[DEVICE OFFLINE WARNING] Device {device.device_id}\n"
                            f"{'='*60}\n"
                            f"  Status: {old_status} -> {new_status}\n"
                            f"  Last Active: {last_active_time}\n"
                            f"  Inactive For: {time_since_active:.1f}s\n"
                            f"  Threshold: {OFFLINE_THRESHOLD}s\n"
                            f"{'='*60}\n"
                        )
                
                elif device.status == DeviceStatus.PENDING_OFFLINE:
                    if time_since_active > OFFLINE_THRESHOLD:
                        device.status = DeviceStatus.OFFLINE
                        new_status = device.status.value
                        offline_count += 1
                        updated_count += 1
                        
                        record_status_event(
                            db,
                            device.device_id,
                            "offline",
                            old_status=old_status,
                            new_status=new_status,
                            reason=f"No heartbeat for {time_since_active:.1f} seconds (timeout: {OFFLINE_THRESHOLD}s, was pending)",
                            details={
                                "last_active_time": last_active_time.isoformat() if last_active_time else None,
                                "timeout_seconds": OFFLINE_THRESHOLD,
                                "time_since_active": time_since_active,
                                "was_pending": True
                            }
                        )
                        logger.critical(
                            f"\n{'='*60}\n"
                            f"[DEVICE OFFLINE WARNING] Device {device.device_id}\n"
                            f"{'='*60}\n"
                            f"  Status: {old_status} -> {new_status}\n"
                            f"  Last Active: {last_active_time}\n"
                            f"  Inactive For: {time_since_active:.1f}s\n"
                            f"  Note: Was in PENDING_OFFLINE state\n"
                            f"{'='*60}\n"
                        )
            
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
                        
                        logger.info(f"[Alert] Device {device.device_id}: Temperature {temperature}°C exceeds threshold, consecutive count: {device.consecutive_alert_count}/{ALERT_CONSECUTIVE_THRESHOLD}")
                        
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
                                logger.info(f"[Alert] Device {device.device_id}: Triggering alert_buzzer after {ALERT_CONSECUTIVE_THRESHOLD} consecutive alerts (id={alert_cmd['id']})")
                    else:
                        if device.consecutive_alert_count is None or device.consecutive_alert_count > 0:
                            logger.info(f"[Alert] Device {device.device_id}: Temperature {temperature}°C normalized, resetting consecutive alert count")
                            device.consecutive_alert_count = 0
        
        if updated_count > 0 or alert_count > 0:
            db.commit()
            if pending_count > 0:
                print(f"[Scheduler] Marked {pending_count} devices as PENDING_OFFLINE")
            if offline_count > 0:
                print(f"[Scheduler] Marked {offline_count} devices as OFFLINE")
            if alert_count > 0:
                print(f"[Scheduler] Triggered {alert_count} temperature alerts")
    except Exception as e:
        print(f"[Scheduler] Error checking devices: {e}")
        logger.error(f"[Scheduler] Error checking devices: {e}", exc_info=True)
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
    
    start_audit_processor()
    print("[Audit] Audit log processor started")
    
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

HIGH_RISK_OPERATIONS = {
    OperationType.DEVICE_DELETE,
    OperationType.DATA_CLEAR,
    OperationType.USER_DELETE
}

SENSITIVE_FIELDS = {
    "secret_key",
    "password_hash",
    "password",
    "api_key",
    "token",
    "credential",
    "auth_token"
}

AUDIT_WAL_FILE = Path("./audit_wal.log")
AUDIT_WAL_LOCK = threading.Lock()

def mask_sensitive_field(key: str, value) -> str:
    key_lower = key.lower()
    for sensitive in SENSITIVE_FIELDS:
        if sensitive in key_lower:
            if isinstance(value, str) and len(value) > 0:
                return "******"
            elif isinstance(value, (int, float)):
                return "******"
            return None
    return value

def sanitize_sensitive_data(data: dict) -> dict:
    if data is None:
        return None
    
    sanitized = {}
    for key, value in data.items():
        if isinstance(value, dict):
            sanitized[key] = sanitize_sensitive_data(value)
        elif isinstance(value, list):
            sanitized[key] = [
                sanitize_sensitive_data(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            sanitized[key] = mask_sensitive_field(key, value)
    return sanitized

def calculate_changed_fields(
    old_values: dict = None,
    new_values: dict = None
) -> dict:
    if old_values is None:
        old_values = {}
    if new_values is None:
        new_values = {}
    
    changed_fields = {}
    
    all_keys = set(old_values.keys()) | set(new_values.keys())
    
    for key in all_keys:
        if key not in old_values:
            sanitized_new = mask_sensitive_field(key, new_values[key])
            changed_fields[key] = [None, sanitized_new]
        elif key not in new_values:
            sanitized_old = mask_sensitive_field(key, old_values[key])
            changed_fields[key] = [sanitized_old, None]
        elif old_values[key] != new_values[key]:
            sanitized_old = mask_sensitive_field(key, old_values[key])
            sanitized_new = mask_sensitive_field(key, new_values[key])
            changed_fields[key] = [sanitized_old, sanitized_new]
    
    if not changed_fields:
        return None
    
    return changed_fields

def serialize_model(obj) -> dict:
    if obj is None:
        return None
    
    result = {}
    for column in obj.__table__.columns:
        value = getattr(obj, column.name)
        if isinstance(value, (datetime, enum.Enum)):
            result[column.name] = str(value)
        else:
            result[column.name] = value
    
    return sanitize_sensitive_data(result)

def write_audit_wal(entry: dict):
    with AUDIT_WAL_LOCK:
        try:
            with open(AUDIT_WAL_FILE, "a", encoding="utf-8") as f:
                entry_copy = dict(entry)
                entry_copy["operation_type"] = entry_copy["operation_type"].value
                entry_copy["queued_at"] = entry_copy["queued_at"].isoformat()
                f.write(json.dumps(entry_copy, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
            logger.debug(f"[AuditWAL] Written to WAL: {entry_copy['operation_type']}")
        except Exception as e:
            logger.error(f"[AuditWAL] Failed to write WAL: {e}")

def load_audit_wal() -> list:
    entries = []
    if not AUDIT_WAL_FILE.exists():
        return entries
    
    with AUDIT_WAL_LOCK:
        try:
            with open(AUDIT_WAL_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        entry["operation_type"] = OperationType(entry["operation_type"])
                        if "queued_at" in entry:
                            entry["queued_at"] = datetime.fromisoformat(entry["queued_at"])
                        entries.append(entry)
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.error(f"[AuditWAL] Failed to parse WAL entry: {e}")
                        continue
        except Exception as e:
            logger.error(f"[AuditWAL] Failed to load WAL: {e}")
    
    return entries

def clear_audit_wal():
    with AUDIT_WAL_LOCK:
        try:
            if AUDIT_WAL_FILE.exists():
                AUDIT_WAL_FILE.unlink()
                logger.info("[AuditWAL] WAL file cleared")
        except Exception as e:
            logger.error(f"[AuditWAL] Failed to clear WAL: {e}")

def recover_audit_wal(db: Session) -> int:
    entries = load_audit_wal()
    if not entries:
        return 0
    
    recovered_count = 0
    for entry in entries:
        try:
            create_audit_log(
                db=db,
                operation_type=entry["operation_type"],
                operation_desc=entry.get("operation_desc"),
                user_id=entry.get("user_id"),
                device_id=entry.get("device_id"),
                ip_address=entry.get("ip_address"),
                old_values=entry.get("old_values"),
                new_values=entry.get("new_values"),
                status=entry.get("status", "success"),
                error_message=entry.get("error_message")
            )
            recovered_count += 1
        except Exception as e:
            logger.error(f"[AuditWAL] Failed to recover entry: {e}")
    
    if recovered_count > 0:
        clear_audit_wal()
        logger.info(f"[AuditWAL] Recovered {recovered_count} audit logs from WAL")
    
    return recovered_count

def create_audit_log(
    db: Session,
    operation_type: OperationType,
    operation_desc: str = None,
    user_id: str = None,
    device_id: str = None,
    ip_address: str = None,
    old_values: dict = None,
    new_values: dict = None,
    status: str = "success",
    error_message: str = None
) -> AuditLog:
    
    sanitized_old = sanitize_sensitive_data(old_values)
    sanitized_new = sanitize_sensitive_data(new_values)
    
    changed_fields = calculate_changed_fields(sanitized_old, sanitized_new)
    
    risk_level = RiskLevel.HIGH if operation_type in HIGH_RISK_OPERATIONS else RiskLevel.MEDIUM
    
    audit_log = AuditLog(
        id=str(uuid.uuid4()),
        operation_type=operation_type,
        operation_desc=operation_desc,
        user_id=user_id,
        device_id=device_id,
        ip_address=ip_address,
        risk_level=risk_level,
        changed_fields=changed_fields,
        status=status,
        error_message=error_message,
        created_at=datetime.utcnow()
    )
    
    db.add(audit_log)
    db.commit()
    db.refresh(audit_log)
    
    logger.info(f"[AuditLog] Created: type={operation_type.value}, risk={risk_level.value}, device={device_id}")
    
    if risk_level == RiskLevel.HIGH:
        _print_high_risk_alert(audit_log)
    
    return audit_log

def _print_high_risk_alert(audit_log: AuditLog):
    banner = "\n"
    banner += "+" + "=" * 78 + "+\n"
    banner += "|" + " " * 78 + "|\n"
    banner += "|  [WARNING] HIGH RISK OPERATION DETECTED" + " " * 38 + "|\n"
    banner += "|" + " " * 78 + "|\n"
    banner += f"|  Operation: {audit_log.operation_type.value:<53}|\n"
    if audit_log.operation_desc:
        banner += f"|  Description: {audit_log.operation_desc:<51}|\n"
    banner += f"|  User ID: {audit_log.user_id or 'N/A':<57}|\n"
    banner += f"|  Device ID: {audit_log.device_id or 'N/A':<55}|\n"
    banner += f"|  IP Address: {audit_log.ip_address or 'N/A':<54}|\n"
    banner += f"|  Timestamp: {audit_log.created_at:<54}|\n"
    if audit_log.changed_fields:
        banner += "|  Changed Fields: " + " " * 61 + "|\n"
        for key, values in audit_log.changed_fields.items():
            old_val = values[0] if values[0] is not None else "(None)"
            new_val = values[1] if values[1] is not None else "(None)"
            old_str = str(old_val)[:25]
            new_str = str(new_val)[:25]
            banner += f"|    - {key}: {old_str} -> {new_str:<30}|\n"
    banner += "|" + " " * 78 + "|\n"
    banner += "+" + "=" * 78 + "+\n"
    
    print(banner)

audit_log_queue = deque(maxlen=10000)
audit_log_lock = threading.Lock()
audit_shutdown_event = threading.Event()

def enqueue_audit_log(
    operation_type: OperationType,
    operation_desc: str = None,
    user_id: str = None,
    device_id: str = None,
    ip_address: str = None,
    old_values: dict = None,
    new_values: dict = None,
    status: str = "success",
    error_message: str = None
):
    audit_entry = {
        "operation_type": operation_type,
        "operation_desc": operation_desc,
        "user_id": user_id,
        "device_id": device_id,
        "ip_address": ip_address,
        "old_values": old_values,
        "new_values": new_values,
        "status": status,
        "error_message": error_message,
        "queued_at": datetime.utcnow()
    }
    
    write_audit_wal(audit_entry)
    
    with audit_log_lock:
        audit_log_queue.append(audit_entry)
    
    logger.debug(f"[AuditQueue] Enqueued: type={operation_type.value}")

def flush_audit_queue():
    db = None
    processed_count = 0
    try:
        db = SessionLocal()
        
        with audit_log_lock:
            entries_to_process = list(audit_log_queue)
            audit_log_queue.clear()
        
        for entry in entries_to_process:
            try:
                create_audit_log(
                    db=db,
                    operation_type=entry["operation_type"],
                    operation_desc=entry.get("operation_desc"),
                    user_id=entry.get("user_id"),
                    device_id=entry.get("device_id"),
                    ip_address=entry.get("ip_address"),
                    old_values=entry.get("old_values"),
                    new_values=entry.get("new_values"),
                    status=entry.get("status", "success"),
                    error_message=entry.get("error_message")
                )
                processed_count += 1
            except Exception as e:
                logger.error(f"[AuditQueue] Failed to process audit entry: {e}")
        
        db.commit()
        
        if processed_count > 0:
            clear_audit_wal()
            logger.info(f"[AuditQueue] Flushed {processed_count} audit logs")
        
    except Exception as e:
        logger.error(f"[AuditQueue] Error flushing audit queue: {e}")
    finally:
        if db:
            db.close()
    
    return processed_count

def process_audit_queue():
    flush_audit_queue()

def audit_shutdown_handler(signum, frame):
    logger.warning(f"[AuditShutdown] Received shutdown signal {signum}, flushing audit queue...")
    audit_shutdown_event.set()
    
    flushed = flush_audit_queue()
    
    logger.warning(f"[AuditShutdown] Audit shutdown complete. Flushed {flushed} logs.")
    
    sys.exit(0)

def register_audit_shutdown_handlers():
    try:
        signal.signal(signal.SIGTERM, audit_shutdown_handler)
        signal.signal(signal.SIGINT, audit_shutdown_handler)
        logger.info("[AuditShutdown] Signal handlers registered")
    except Exception as e:
        logger.warning(f"[AuditShutdown] Failed to register signal handlers: {e}")
    
    def atexit_flush():
        if not audit_shutdown_event.is_set():
            logger.warning("[AuditShutdown] Atexit: Flushing audit queue...")
            flush_audit_queue()
    
    atexit.register(atexit_flush)

def recover_audit_logs_on_startup():
    db = None
    try:
        db = SessionLocal()
        recovered = recover_audit_wal(db)
        if recovered > 0:
            logger.warning(f"[AuditRecovery] Recovered {recovered} audit logs from WAL during startup")
        return recovered
    except Exception as e:
        logger.error(f"[AuditRecovery] Failed to recover audit logs: {e}")
        return 0
    finally:
        if db:
            db.close()

def start_audit_processor():
    recover_audit_logs_on_startup()
    
    register_audit_shutdown_handlers()
    
    def run_processor():
        while not audit_shutdown_event.is_set():
            time.sleep(1)
            with audit_log_lock:
                queue_size = len(audit_log_queue)
            
            if queue_size >= 100:
                flush_audit_queue()
            elif queue_size > 0:
                time.sleep(2)
                flush_audit_queue()
        
        logger.info("[AuditProcessor] Processor thread stopped")
    
    thread = threading.Thread(target=run_processor, daemon=False)
    thread.start()
    logger.info("[AuditProcessor] Started audit log processor thread")

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
def register_device(
    device_data: DeviceRegister,
    x_user_id: str = Header(None, alias="X-User-ID"),
    x_real_ip: str = Header(None, alias="X-Real-IP"),
    db: Session = Depends(get_db)
):
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
    
    new_values = serialize_model(new_device)
    create_audit_log(
        db=db,
        operation_type=OperationType.DEVICE_REGISTER,
        operation_desc=f"Registered new device: {device_data.device_id}",
        user_id=x_user_id,
        device_id=new_device.device_id,
        ip_address=x_real_ip,
        new_values=new_values
    )
    
    return {
        "message": "Device registered successfully",
        "device_id": new_device.device_id,
        "model": new_device.model,
        "status": new_device.status.value,
        "secret_key": new_device.secret_key
    }

@app.delete("/devices/{device_id}")
def delete_device(
    device_id: str,
    x_user_id: str = Header(None, alias="X-User-ID"),
    x_real_ip: str = Header(None, alias="X-Real-IP"),
    db: Session = Depends(get_db)
):
    if x_user_id:
        require_staff_or_admin(x_user_id, db)
    
    device = db.query(Device).filter(
        Device.device_id == device_id
    ).first()
    
    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found."
        )
    
    old_values = serialize_model(device)
    
    data_count = db.query(DeviceData).filter(
        DeviceData.device_id == device_id
    ).count()
    history_count = db.query(DeviceDataHistory).filter(
        DeviceDataHistory.device_id == device_id
    ).count()
    
    db.query(DeviceStatusEvent).filter(
        DeviceStatusEvent.device_id == device_id
    ).delete(synchronize_session=False)
    
    db.query(DeviceProtocol).filter(
        DeviceProtocol.device_id == device_id
    ).delete(synchronize_session=False)
    
    db.query(DeviceData).filter(
        DeviceData.device_id == device_id
    ).delete(synchronize_session=False)
    
    db.query(DeviceDataHistory).filter(
        DeviceDataHistory.device_id == device_id
    ).delete(synchronize_session=False)
    
    db.delete(device)
    db.commit()
    
    create_audit_log(
        db=db,
        operation_type=OperationType.DEVICE_DELETE,
        operation_desc=f"Deleted device: {device_id} (removed {data_count} data records, {history_count} history records)",
        user_id=x_user_id,
        device_id=device_id,
        ip_address=x_real_ip,
        old_values=old_values
    )
    
    return {
        "message": "Device deleted successfully",
        "device_id": device_id,
        "removed_data_count": data_count,
        "removed_history_count": history_count
    }

class ThresholdUpdate(BaseModel):
    temperature_threshold: float = None
    alert_consecutive_threshold: int = None

@app.put("/settings/thresholds")
def update_thresholds(
    threshold_data: ThresholdUpdate,
    x_user_id: str = Header(None, alias="X-User-ID"),
    x_real_ip: str = Header(None, alias="X-Real-IP"),
    db: Session = Depends(get_db)
):
    global TEMPERATURE_THRESHOLD, ALERT_CONSECUTIVE_THRESHOLD
    
    if x_user_id:
        require_staff_or_admin(x_user_id, db)
    
    old_values = {
        "temperature_threshold": TEMPERATURE_THRESHOLD,
        "alert_consecutive_threshold": ALERT_CONSECUTIVE_THRESHOLD
    }
    
    new_values = dict(old_values)
    
    if threshold_data.temperature_threshold is not None:
        TEMPERATURE_THRESHOLD = threshold_data.temperature_threshold
        new_values["temperature_threshold"] = threshold_data.temperature_threshold
    
    if threshold_data.alert_consecutive_threshold is not None:
        ALERT_CONSECUTIVE_THRESHOLD = threshold_data.alert_consecutive_threshold
        new_values["alert_consecutive_threshold"] = threshold_data.alert_consecutive_threshold
    
    create_audit_log(
        db=db,
        operation_type=OperationType.THRESHOLD_UPDATE,
        operation_desc="Updated system thresholds",
        user_id=x_user_id,
        ip_address=x_real_ip,
        old_values=old_values,
        new_values=new_values
    )
    
    return {
        "message": "Thresholds updated successfully",
        "old_values": old_values,
        "new_values": new_values
    }

@app.delete("/devices/{device_id}/history")
def clear_device_history(
    device_id: str,
    x_user_id: str = Header(None, alias="X-User-ID"),
    x_real_ip: str = Header(None, alias="X-Real-IP"),
    db: Session = Depends(get_db)
):
    if x_user_id:
        require_staff_or_admin(x_user_id, db)
    
    device = db.query(Device).filter(
        Device.device_id == device_id
    ).first()
    
    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found."
        )
    
    data_count = db.query(DeviceData).filter(
        DeviceData.device_id == device_id
    ).count()
    history_count = db.query(DeviceDataHistory).filter(
        DeviceDataHistory.device_id == device_id
    ).count()
    
    old_values = {
        "device_id": device_id,
        "data_record_count": data_count,
        "history_record_count": history_count
    }
    
    db.query(DeviceData).filter(
        DeviceData.device_id == device_id
    ).delete(synchronize_session=False)
    
    db.query(DeviceDataHistory).filter(
        DeviceDataHistory.device_id == device_id
    ).delete(synchronize_session=False)
    
    db.commit()
    
    create_audit_log(
        db=db,
        operation_type=OperationType.DATA_CLEAR,
        operation_desc=f"Cleared history for device: {device_id} (removed {data_count} data records, {history_count} history records)",
        user_id=x_user_id,
        device_id=device_id,
        ip_address=x_real_ip,
        old_values=old_values
    )
    
    return {
        "message": "Device history cleared successfully",
        "device_id": device_id,
        "removed_data_count": data_count,
        "removed_history_count": history_count
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
    old_status = device.status.value
    
    device.last_heartbeat = now
    device.last_seen = now
    
    if device.status != DeviceStatus.ONLINE:
        device.status = DeviceStatus.ONLINE
        new_status = device.status.value
        record_status_event(
            db,
            device.device_id,
            "online",
            old_status=old_status,
            new_status=new_status,
            reason="Device heartbeat received",
            details={
                "timestamp": now.isoformat()
            }
        )
        logger.info(f"[DeviceOnline] Device {device.device_id} marked as ONLINE")
    
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
    old_status = device.status.value
    
    device.last_heartbeat = now
    device.last_seen = now
    
    if device.status != DeviceStatus.ONLINE:
        device.status = DeviceStatus.ONLINE
        new_status = device.status.value
        record_status_event(
            db,
            device.device_id,
            "online",
            old_status=old_status,
            new_status=new_status,
            reason="Device data reported",
            details={
                "timestamp": now.isoformat()
            }
        )
        logger.info(f"[DeviceOnline] Device {device.device_id} marked as ONLINE via data report")
    
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
        
        protocol_info = None
        protocol = db.query(DeviceProtocol).filter(
            DeviceProtocol.device_id == device.device_id,
            DeviceProtocol.is_active == True
        ).first()
        
        if protocol:
            has_protocol_success = False
            if latest_data and latest_data.payload:
                raw_parsed = latest_data.payload.get('_raw_parsed', {})
                if raw_parsed:
                    has_protocol_success = True
            
            protocol_info = {
                "protocol_type": protocol.protocol_type.value,
                "description": protocol.description,
                "has_success": has_protocol_success
            }
        
        result.append({
            "device_id": device.device_id,
            "model": device.model,
            "status": device.status.value,
            "last_heartbeat": device.last_heartbeat,
            "latest_payload": latest_data.payload if latest_data else None,
            "latest_data_time": latest_data.recorded_at if latest_data else None,
            "protocol": protocol_info
        })
    
    db.commit()
    return result

@app.get("/devices/status-events")
def get_status_events(
    device_id: str = Query(None, description="Filter by device ID"),
    event_type: str = Query(None, description="Filter by event type (online/offline/pending_offline)"),
    since: str = Query(None, description="Only return events after this timestamp (ISO format)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of events to return"),
    use_memory: bool = Query(True, description="Use memory cache for faster response"),
    db: Session = Depends(get_db)
):
    since_dt = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid 'since' format. Use ISO format: YYYY-MM-DDTHH:MM:SS"
            )
    
    if use_memory and limit <= MAX_MEMORY_EVENTS:
        logger.debug(f"[StatusEvents] Using memory queue for query (limit={limit})")
        memory_events = get_events_from_memory(
            device_id=device_id,
            event_type=event_type,
            since=since_dt,
            limit=limit
        )
        
        if memory_events:
            result = []
            for event in memory_events:
                event_copy = dict(event)
                event_copy.pop("_added_at", None)
                result.append(event_copy)
            
            return {
                "events": result,
                "count": len(result),
                "limit": limit,
                "source": "memory_cache"
            }
        
        logger.debug(f"[StatusEvents] Memory queue empty, falling back to database")
    
    query = db.query(DeviceStatusEvent)
    
    if device_id:
        query = query.filter(DeviceStatusEvent.device_id == device_id)
    
    if event_type:
        query = query.filter(DeviceStatusEvent.event_type == event_type)
    
    if since_dt:
        query = query.filter(DeviceStatusEvent.created_at >= since_dt)
    
    events = query.order_by(desc(DeviceStatusEvent.created_at)).limit(limit).all()
    
    result = []
    for event in events:
        result.append({
            "id": event.id,
            "device_id": event.device_id,
            "event_type": event.event_type,
            "old_status": event.old_status,
            "new_status": event.new_status,
            "reason": event.reason,
            "details": event.details,
            "created_at": event.created_at.isoformat() if event.created_at else None
        })
    
    return {
        "events": result,
        "count": len(result),
        "limit": limit,
        "source": "database"
    }

def check_device_status(device: Device):
    last_active_time = device.last_seen if device.last_seen else device.last_heartbeat
    if last_active_time is None:
        return
    
    now = datetime.utcnow()
    time_since_active = (now - last_active_time).total_seconds()
    
    if device.status == DeviceStatus.ONLINE:
        if time_since_active > PENDING_OFFLINE_THRESHOLD and time_since_active <= OFFLINE_THRESHOLD:
            device.status = DeviceStatus.PENDING_OFFLINE
        elif time_since_active > OFFLINE_THRESHOLD:
            device.status = DeviceStatus.OFFLINE
    elif device.status == DeviceStatus.PENDING_OFFLINE:
        if time_since_active > OFFLINE_THRESHOLD:
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

@app.get("/audit/logs")
def get_audit_logs(
    x_user_id: str = Header(None, alias="X-User-ID"),
    device_id: str = Query(None, description="Filter by device ID"),
    user_id: str = Query(None, description="Filter by user ID"),
    operation_type: str = Query(None, description="Filter by operation type"),
    risk_level: str = Query(None, description="Filter by risk level (low/medium/high)"),
    since: str = Query(None, description="Only return logs after this timestamp (ISO format)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of logs to return"),
    db: Session = Depends(get_db)
):
    if x_user_id:
        require_staff_or_admin(x_user_id, db)
    
    query = db.query(AuditLog)
    
    if device_id:
        query = query.filter(AuditLog.device_id == device_id)
    
    if user_id:
        query = query.filter(AuditLog.user_id == user_id)
    
    if operation_type:
        try:
            op_type = OperationType(operation_type)
            query = query.filter(AuditLog.operation_type == op_type)
        except ValueError:
            pass
    
    if risk_level:
        try:
            r_level = RiskLevel(risk_level)
            query = query.filter(AuditLog.risk_level == r_level)
        except ValueError:
            pass
    
    if since:
        try:
            since_dt = datetime.fromisoformat(since)
            query = query.filter(AuditLog.created_at >= since_dt)
        except ValueError:
            pass
    
    logs = query.order_by(desc(AuditLog.created_at)).limit(limit).all()
    
    result = []
    for log in logs:
        result.append({
            "id": log.id,
            "operation_type": log.operation_type.value if log.operation_type else None,
            "operation_desc": log.operation_desc,
            "user_id": log.user_id,
            "device_id": log.device_id,
            "ip_address": log.ip_address,
            "risk_level": log.risk_level.value if log.risk_level else None,
            "changed_fields": log.changed_fields,
            "status": log.status,
            "error_message": log.error_message,
            "created_at": log.created_at.isoformat() if log.created_at else None
        })
    
    return {
        "logs": result,
        "count": len(result),
        "limit": limit
    }
