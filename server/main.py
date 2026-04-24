from fastapi import FastAPI, Depends, HTTPException, Header, Query, WebSocket, WebSocketDisconnect
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
from typing import Dict, Set, Optional, Any
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
import enum
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
    AuditLog, OperationType, RiskLevel,
    ReportTask, ReportTaskStatus, ScheduledReportConfig, ScheduledReportType,
    DeviceCommand
)

from .report_engine import (
    report_engine, 
    ScheduledReportManager,
    REPORTS_DIR,
    EXPORT_BATCH_SIZE,
    DEFAULT_REPORT_RETENTION_HOURS
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
        if event_type == "offline":
            logger.warning(f"[DEVICE ALERT] Device OFFLINE: {device_id}")
        elif event_type == "pending_offline":
            logger.warning(f"[DEVICE WARNING] Device PENDING OFFLINE: {device_id}")
        elif event_type == "online":
            logger.info(f"[DEVICE STATUS] Device ONLINE: {device_id}")
        else:
            logger.info(f"[DEVICE EVENT] {event_type}: {device_id}")
        
        for key, value in details.items():
            if value is not None:
                logger.info(f"    {key}: {value}")
    
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
        
        expired_commands_count = expire_overdue_commands(db, now)
        
        if updated_count > 0 or alert_count > 0 or expired_commands_count > 0:
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

def get_session_local_factory():
    return SessionLocal

def init_report_engine():
    db = SessionLocal()
    try:
        report_engine.ensure_reports_dir()
        
        scheduled_manager = ScheduledReportManager(report_engine, scheduler)
        daily_config = scheduled_manager.ensure_daily_briefing_config(db)
        
        print(f"[ReportEngine] Report directory initialized: {REPORTS_DIR}")
        print(f"[ReportEngine] Daily briefing config: {daily_config.name} (cron: {daily_config.cron_expression})")
        
        scheduled_manager.load_active_configs(db)
        
        def schedule_with_session():
            scheduled_manager._execute_scheduled_report(daily_config.id, get_session_local_factory)
        
        try:
            from apscheduler.triggers.cron import CronTrigger
            cron_expr = daily_config.cron_expression or "0 8 * * *"
            cron_parts = cron_expr.split()
            if len(cron_parts) == 5:
                trigger = CronTrigger(
                    minute=cron_parts[0],
                    hour=cron_parts[1],
                    day=cron_parts[2],
                    month=cron_parts[3],
                    day_of_week=cron_parts[4]
                )
            else:
                trigger = CronTrigger(hour=8, minute=0)
            
            scheduler.add_job(
                schedule_with_session,
                trigger=trigger,
                id=f"scheduled_report_{daily_config.id}",
                name=f"Scheduled Report: {daily_config.name}",
                replace_existing=True
            )
            print(f"[ReportEngine] Scheduled daily briefing with cron: {cron_expr}")
        except Exception as e:
            print(f"[ReportEngine] Warning: Failed to schedule daily briefing: {e}")
        
        try:
            scheduler.add_job(
                lambda: report_engine.cleanup_expired_reports(SessionLocal()),
                'interval',
                hours=1,
                id="cleanup_expired_reports",
                name="Cleanup expired reports",
                replace_existing=True
            )
            print(f"[ReportEngine] Scheduled hourly cleanup of expired reports")
        except Exception as e:
            print(f"[ReportEngine] Warning: Failed to schedule cleanup: {e}")
        
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
    
    try:
        init_report_engine()
    except Exception as e:
        print(f"[ReportEngine] Failed to initialize: {e}")
        logger.error(f"[ReportEngine] Init failed: {e}", exc_info=True)
    
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

AUDIT_WAL_DIR = Path("./audit_wal")
AUDIT_WAL_LOCK = threading.Lock()

def ensure_wal_dir():
    AUDIT_WAL_DIR.mkdir(parents=True, exist_ok=True)

def mask_sensitive_field(key: str, value) -> str:
    key_lower = key.lower()
    for sensitive in SENSITIVE_FIELDS:
        if sensitive in key_lower:
            if isinstance(value, str) and len(value) > 0:
                return "******"
            elif isinstance(value, (int, float)):
                return "******"
            return value
    return value

def sanitize_sensitive_data(data) -> dict:
    if data is None:
        return None
    
    if isinstance(data, dict):
        sanitized = {}
        for key, value in data.items():
            key_lower = key.lower()
            is_sensitive = any(sensitive in key_lower for sensitive in SENSITIVE_FIELDS)
            if is_sensitive:
                if isinstance(value, (str, int, float)):
                    sanitized[key] = mask_sensitive_field(key, value)
                else:
                    sanitized[key] = sanitize_sensitive_data(value)
            else:
                sanitized[key] = sanitize_sensitive_data(value)
        return sanitized
    
    elif isinstance(data, list):
        return [sanitize_sensitive_data(item) for item in data]
    
    else:
        return data

def _deep_diff_values(old_val, new_val, path: str, result: dict):
    if old_val == new_val:
        return
    
    if isinstance(old_val, dict) and isinstance(new_val, dict):
        old_keys = set(old_val.keys())
        new_keys = set(new_val.keys())
        all_keys = old_keys | new_keys
        
        for key in all_keys:
            nested_path = f"{path}.{key}" if path else key
            if key not in old_val:
                if isinstance(new_val[key], (dict, list)):
                    _deep_diff_values({}, new_val[key], nested_path, result)
                else:
                    sanitized = mask_sensitive_field(key, new_val[key])
                    result[nested_path] = [None, sanitized]
            elif key not in new_val:
                if isinstance(old_val[key], (dict, list)):
                    _deep_diff_values(old_val[key], {}, nested_path, result)
                else:
                    sanitized = mask_sensitive_field(key, old_val[key])
                    result[nested_path] = [sanitized, None]
            else:
                _deep_diff_values(old_val[key], new_val[key], nested_path, result)
    
    elif isinstance(old_val, list) and isinstance(new_val, list):
        if len(old_val) != len(new_val):
            key = path.split(".")[-1] if "." in path else path
            sanitized_old = sanitize_sensitive_data(old_val)
            sanitized_new = sanitize_sensitive_data(new_val)
            result[path] = [sanitized_old, sanitized_new]
        else:
            for i, (ov, nv) in enumerate(zip(old_val, new_val)):
                nested_path = f"{path}[{i}]"
                _deep_diff_values(ov, nv, nested_path, result)
    
    else:
        key = path.split(".")[-1] if "." in path else path
        sanitized_old = mask_sensitive_field(key, old_val)
        sanitized_new = mask_sensitive_field(key, new_val)
        result[path] = [sanitized_old, sanitized_new]

def calculate_changed_fields_deep(
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
            new_val = new_values[key]
            if isinstance(new_val, (dict, list)):
                _deep_diff_values({}, new_val, key, changed_fields)
            else:
                sanitized = mask_sensitive_field(key, new_val)
                changed_fields[key] = [None, sanitized]
        elif key not in new_values:
            old_val = old_values[key]
            if isinstance(old_val, (dict, list)):
                _deep_diff_values(old_val, {}, key, changed_fields)
            else:
                sanitized = mask_sensitive_field(key, old_val)
                changed_fields[key] = [sanitized, None]
        else:
            old_val = old_values[key]
            new_val = new_values[key]
            _deep_diff_values(old_val, new_val, key, changed_fields)
    
    if not changed_fields:
        return None
    
    return changed_fields

def calculate_changed_fields(
    old_values: dict = None,
    new_values: dict = None
) -> dict:
    return calculate_changed_fields_deep(old_values, new_values)

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

def _atomic_write_wal_file(entry_json: str, entry_id: str) -> Path:
    ensure_wal_dir()
    
    timestamp = int(time.time() * 1000000)
    temp_file = AUDIT_WAL_DIR / f".tmp_{timestamp}_{entry_id}.json"
    final_file = AUDIT_WAL_DIR / f"wal_{timestamp}_{entry_id}.json"
    
    with open(temp_file, "w", encoding="utf-8") as f:
        f.write(entry_json)
        f.flush()
        os.fsync(f.fileno())
    
    temp_file.replace(final_file)
    
    return final_file

def write_audit_wal(entry: dict) -> Path:
    with AUDIT_WAL_LOCK:
        try:
            entry_copy = dict(entry)
            entry_copy["operation_type"] = entry_copy["operation_type"].value
            entry_copy["queued_at"] = entry_copy["queued_at"].isoformat()
            
            entry_id = entry_copy.get("id", str(uuid.uuid4()))
            entry_json = json.dumps(entry_copy, ensure_ascii=False)
            
            final_file = _atomic_write_wal_file(entry_json, entry_id)
            
            logger.debug(f"[AuditWAL] Atomically written to WAL: {final_file.name}")
            return final_file
            
        except Exception as e:
            logger.error(f"[AuditWAL] Failed to write WAL: {e}")
            raise

def load_audit_wal() -> list:
    ensure_wal_dir()
    entries = []
    
    wal_files = sorted(AUDIT_WAL_DIR.glob("wal_*.json"))
    
    with AUDIT_WAL_LOCK:
        for wal_file in wal_files:
            try:
                with open(wal_file, "r", encoding="utf-8") as f:
                    content = f.read()
                    entry = json.loads(content)
                    entry["operation_type"] = OperationType(entry["operation_type"])
                    if "queued_at" in entry:
                        entry["queued_at"] = datetime.fromisoformat(entry["queued_at"])
                    entry["_wal_file"] = wal_file
                    entries.append(entry)
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"[AuditWAL] Failed to parse WAL file {wal_file.name}: {e}")
                continue
            except Exception as e:
                logger.error(f"[AuditWAL] Failed to load WAL file {wal_file.name}: {e}")
                continue
    
    return entries

def clear_wal_file(wal_file: Path):
    with AUDIT_WAL_LOCK:
        try:
            if wal_file.exists():
                wal_file.unlink()
                logger.debug(f"[AuditWAL] Cleared WAL file: {wal_file.name}")
        except Exception as e:
            logger.error(f"[AuditWAL] Failed to clear WAL file {wal_file.name}: {e}")

def clear_audit_wal():
    ensure_wal_dir()
    wal_files = list(AUDIT_WAL_DIR.glob("wal_*.json"))
    temp_files = list(AUDIT_WAL_DIR.glob(".tmp_*.json"))
    
    with AUDIT_WAL_LOCK:
        for f in wal_files + temp_files:
            try:
                f.unlink()
            except Exception as e:
                logger.error(f"[AuditWAL] Failed to remove {f.name}: {e}")
    
    logger.info(f"[AuditWAL] Cleared {len(wal_files) + len(temp_files)} WAL files")

def recover_audit_wal(db: Session) -> int:
    entries = load_audit_wal()
    if not entries:
        return 0
    
    recovered_count = 0
    failed_entries = []
    
    for entry in entries:
        try:
            create_audit_log_sync(
                db=db,
                operation_type=entry["operation_type"],
                operation_desc=entry.get("operation_desc"),
                user_id=entry.get("user_id"),
                device_id=entry.get("device_id"),
                ip_address=entry.get("ip_address"),
                old_values=entry.get("old_values"),
                new_values=entry.get("new_values"),
                status=entry.get("status", "success"),
                error_message=entry.get("error_message"),
                auto_commit=False
            )
            
            if "_wal_file" in entry:
                clear_wal_file(entry["_wal_file"])
            
            recovered_count += 1
            
        except Exception as e:
            logger.error(f"[AuditWAL] Failed to recover entry: {e}")
            failed_entries.append(entry)
    
    try:
        db.commit()
    except Exception as e:
        logger.error(f"[AuditWAL] Failed to commit recovered logs: {e}")
        db.rollback()
        return 0
    
    if recovered_count > 0:
        logger.warning(f"[AuditWAL] Recovered {recovered_count} audit logs from WAL")
    
    return recovered_count

def create_audit_log_sync(
    db: Session,
    operation_type: OperationType,
    operation_desc: str = None,
    user_id: str = None,
    device_id: str = None,
    ip_address: str = None,
    old_values: dict = None,
    new_values: dict = None,
    status: str = "success",
    error_message: str = None,
    auto_commit: bool = True
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
    
    if auto_commit:
        db.commit()
        db.refresh(audit_log)
    
    logger.info(f"[AuditLog] Sync created: type={operation_type.value}, risk={risk_level.value}, device={device_id}")
    
    if risk_level == RiskLevel.HIGH:
        _print_high_risk_alert(audit_log)
    
    return audit_log

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
    return create_audit_log_sync(
        db=db,
        operation_type=operation_type,
        operation_desc=operation_desc,
        user_id=user_id,
        device_id=device_id,
        ip_address=ip_address,
        old_values=old_values,
        new_values=new_values,
        status=status,
        error_message=error_message,
        auto_commit=True
    )

class AuditedOperation:
    def __init__(
        self,
        db: Session,
        operation_type: OperationType,
        user_id: str = None,
        device_id: str = None,
        ip_address: str = None,
        operation_desc: str = None
    ):
        self.db = db
        self.operation_type = operation_type
        self.user_id = user_id
        self.device_id = device_id
        self.ip_address = ip_address
        self.operation_desc = operation_desc
        self.old_values = None
        self.new_values = None
        self.committed = False
    
    def set_old_values(self, values: dict):
        self.old_values = dict(values) if values else None
        return self
    
    def set_new_values(self, values: dict):
        self.new_values = dict(values) if values else None
        return self
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logger.error(f"[AuditOperation] Operation failed: {exc_val}")
            try:
                create_audit_log_sync(
                    db=self.db,
                    operation_type=self.operation_type,
                    operation_desc=f"FAILED: {self.operation_desc}",
                    user_id=self.user_id,
                    device_id=self.device_id,
                    ip_address=self.ip_address,
                    old_values=self.old_values,
                    new_values=self.new_values,
                    status="failed",
                    error_message=str(exc_val),
                    auto_commit=False
                )
            except Exception as e:
                logger.error(f"[AuditOperation] Failed to log failure: {e}")
            return False
        
        try:
            create_audit_log_sync(
                db=self.db,
                operation_type=self.operation_type,
                operation_desc=self.operation_desc,
                user_id=self.user_id,
                device_id=self.device_id,
                ip_address=self.ip_address,
                old_values=self.old_values,
                new_values=self.new_values,
                status="success",
                auto_commit=False
            )
            self.committed = True
            return True
        except Exception as e:
            logger.error(f"[AuditOperation] Failed to create audit log, rolling back: {e}")
            self.db.rollback()
            raise

def audited_operation(
    db: Session,
    operation_type: OperationType,
    user_id: str = None,
    device_id: str = None,
    ip_address: str = None,
    operation_desc: str = None
) -> AuditedOperation:
    return AuditedOperation(
        db=db,
        operation_type=operation_type,
        user_id=user_id,
        device_id=device_id,
        ip_address=ip_address,
        operation_desc=operation_desc
    )

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
        "id": str(uuid.uuid4()),
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
                create_audit_log_sync(
                    db=db,
                    operation_type=entry["operation_type"],
                    operation_desc=entry.get("operation_desc"),
                    user_id=entry.get("user_id"),
                    device_id=entry.get("device_id"),
                    ip_address=entry.get("ip_address"),
                    old_values=entry.get("old_values"),
                    new_values=entry.get("new_values"),
                    status=entry.get("status", "success"),
                    error_message=entry.get("error_message"),
                    auto_commit=False
                )
                processed_count += 1
            except Exception as e:
                logger.error(f"[AuditQueue] Failed to process audit entry: {e}")
        
        db.commit()
        
        if processed_count > 0:
            logger.info(f"[AuditQueue] Flushed {processed_count} audit logs")
        
    except Exception as e:
        logger.error(f"[AuditQueue] Error flushing audit queue: {e}")
        if db:
            db.rollback()
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
    
    wal_entries = load_audit_wal()
    if wal_entries:
        logger.warning(f"[AuditShutdown] Still {len(wal_entries)} WAL files, attempting final recovery...")
        db = SessionLocal()
        try:
            recover_audit_wal(db)
        finally:
            db.close()
    
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
            
            wal_entries = load_audit_wal()
            if wal_entries:
                db = SessionLocal()
                try:
                    recover_audit_wal(db)
                finally:
                    db.close()
    
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
    ensure_wal_dir()
    
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

class DeviceCommandCreate(BaseModel):
    command: str
    value: str = None
    ttl_seconds: int = COMMAND_TTL_SECONDS
    reason: str = None
    client_msg_id: str = None

class CommandAck(BaseModel):
    status: str = "executed"
    result: dict = None
    error_message: str = None
    client_msg_id: str = None

class WebSocketConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self._lock: threading.Lock = threading.Lock()
        self._frontend_connections: Set[WebSocket] = set()
    
    async def connect(self, websocket: WebSocket, device_id: str = None):
        await websocket.accept()
        with self._lock:
            if device_id:
                if device_id not in self.active_connections:
                    self.active_connections[device_id] = set()
                self.active_connections[device_id].add(websocket)
                logger.info(f"[WebSocket] Device {device_id} connected")
            else:
                self._frontend_connections.add(websocket)
                logger.info(f"[WebSocket] Frontend client connected")
    
    def disconnect(self, websocket: WebSocket, device_id: str = None):
        with self._lock:
            if device_id and device_id in self.active_connections:
                self.active_connections[device_id].discard(websocket)
                if not self.active_connections[device_id]:
                    del self.active_connections[device_id]
                logger.info(f"[WebSocket] Device {device_id} disconnected")
            else:
                self._frontend_connections.discard(websocket)
                logger.info(f"[WebSocket] Frontend client disconnected")
    
    async def send_to_device(self, device_id: str, message: Dict[str, Any]):
        if device_id not in self.active_connections:
            return False
        
        connections = list(self.active_connections[device_id])
        for websocket in connections:
            try:
                await websocket.send_json(message)
                logger.info(f"[WebSocket] Sent message to device {device_id}: {message}")
            except Exception as e:
                logger.error(f"[WebSocket] Failed to send to device {device_id}: {e}")
                self.disconnect(websocket, device_id)
        
        return len(connections) > 0
    
    async def send_to_frontend(self, message: Dict[str, Any]):
        connections = list(self._frontend_connections)
        for websocket in connections:
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.error(f"[WebSocket] Failed to send to frontend: {e}")
                self.disconnect(websocket)
        
        return len(connections) > 0
    
    async def broadcast_command(self, device_id: str, command: DeviceCommand):
        message = {
            "type": "new_command",
            "device_id": device_id,
            "command_id": command.id,
            "command": command.command_type,
            "value": command.command_value,
            "ttl_seconds": command.ttl_seconds,
            "expires_at": command.expires_at.isoformat() if command.expires_at else None,
            "created_at": command.created_at.isoformat() if command.created_at else None
        }
        
        await self.send_to_device(device_id, message)
        await self.send_to_frontend(message)
        
        logger.info(f"[WebSocket] Broadcasted command {command.id} to device {device_id}")
    
    def is_device_online(self, device_id: str) -> bool:
        with self._lock:
            return device_id in self.active_connections and len(self.active_connections[device_id]) > 0

ws_manager = WebSocketConnectionManager()

def create_device_command(
    device_id: str,
    command: str,
    value: str = None,
    ttl_seconds: int = COMMAND_TTL_SECONDS,
    reason: str = None,
    source: str = "api",
    client_msg_id: str = None,
    db: Session = None
) -> DeviceCommand:
    now = datetime.utcnow()
    expires_at = now + timedelta(seconds=ttl_seconds)
    
    cmd = DeviceCommand(
        id=str(uuid.uuid4()),
        device_id=device_id,
        client_msg_id=client_msg_id,
        command_type=command,
        command_value=value,
        status=CommandStatus.PENDING,
        ttl_seconds=ttl_seconds,
        expires_at=expires_at,
        reason=reason,
        source=source,
        created_at=now,
        updated_at=now
    )
    
    if db:
        db.add(cmd)
        try:
            db.commit()
            db.refresh(cmd)
            logger.info(f"[DeviceCommand] Created command {cmd.id} for device {device_id}: {command}={value}, TTL={ttl_seconds}s")
        except Exception as e:
            db.rollback()
            if client_msg_id:
                existing = db.query(DeviceCommand).filter(
                    DeviceCommand.client_msg_id == client_msg_id
                ).first()
                if existing:
                    logger.warning(f"[DeviceCommand] Duplicate client_msg_id: {client_msg_id}, returning existing command {existing.id}")
                    return existing
            raise
    
    return cmd

def get_pending_commands(device_id: str, db: Session, now: datetime = None) -> list:
    if now is None:
        now = datetime.utcnow()
    
    commands = db.query(DeviceCommand).filter(
        DeviceCommand.device_id == device_id,
        DeviceCommand.status == CommandStatus.PENDING,
        DeviceCommand.expires_at > now
    ).order_by(DeviceCommand.created_at.asc()).all()
    
    return commands

def update_command_status(
    cmd_id: str,
    new_status: CommandStatus,
    db: Session,
    result_data: dict = None,
    error_message: str = None,
    client_msg_id: str = None
) -> tuple:
    if client_msg_id:
        existing_ack = db.query(DeviceCommand).filter(
            DeviceCommand.client_msg_id == client_msg_id
        ).first()
        if existing_ack:
            logger.warning(f"[DeviceCommand] Duplicate ack with client_msg_id: {client_msg_id}")
            return existing_ack, False
    
    cmd = db.query(DeviceCommand).filter(
        DeviceCommand.id == cmd_id
    ).with_for_update().first()
    
    if not cmd:
        return None, False
    
    if cmd.status in [CommandStatus.EXECUTED, CommandStatus.FAILED, CommandStatus.EXPIRED]:
        logger.warning(f"[DeviceCommand] Command {cmd_id} already in final state: {cmd.status.value}")
        return cmd, False
    
    now = datetime.utcnow()
    cmd.status = new_status
    cmd.updated_at = now
    
    if client_msg_id:
        cmd.client_msg_id = client_msg_id
    
    if new_status == CommandStatus.DELIVERED:
        cmd.delivered_at = now
    elif new_status == CommandStatus.EXECUTED:
        cmd.executed_at = now
        cmd.result_data = result_data
    elif new_status == CommandStatus.FAILED:
        cmd.failed_at = now
        cmd.error_message = error_message
    elif new_status == CommandStatus.EXPIRED:
        cmd.expired_at = now
    
    try:
        db.commit()
        db.refresh(cmd)
        logger.info(f"[DeviceCommand] Command {cmd_id} status updated to {new_status.value}")
        return cmd, True
    except Exception as e:
        db.rollback()
        logger.error(f"[DeviceCommand] Failed to update command {cmd_id}: {e}")
        return None, False

def expire_overdue_commands(db: Session, now: datetime = None) -> int:
    if now is None:
        now = datetime.utcnow()
    
    expired_count = db.query(DeviceCommand).filter(
        DeviceCommand.status.in_([CommandStatus.PENDING, CommandStatus.DELIVERED]),
        DeviceCommand.expires_at <= now
    ).update(
        {
            DeviceCommand.status: CommandStatus.EXPIRED,
            DeviceCommand.expired_at: now,
            DeviceCommand.updated_at: now
        },
        synchronize_session=False
    )
    
    if expired_count > 0:
        db.commit()
        logger.info(f"[DeviceCommand] Expired {expired_count} overdue commands")
    
    return expired_count

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

@app.post("/devices/{device_id}/commands")
def send_device_command(
    device_id: str,
    command_data: DeviceCommandCreate,
    x_user_id: str = Header(None, alias="X-User-ID"),
    x_real_ip: str = Header(None, alias="X-Real-IP"),
    db: Session = Depends(get_db)
):
    device = db.query(Device).filter(
        Device.device_id == device_id
    ).first()
    
    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found."
        )
    
    cmd = create_device_command(
        device_id=device_id,
        command=command_data.command,
        value=command_data.value,
        ttl_seconds=command_data.ttl_seconds,
        reason=command_data.reason,
        source="api",
        client_msg_id=command_data.client_msg_id,
        db=db
    )
    
    create_audit_log(
        db=db,
        operation_type=OperationType.DEVICE_CONTROL,
        operation_desc=f"Sent command to device {device_id}: {command_data.command}",
        user_id=x_user_id,
        device_id=device_id,
        ip_address=x_real_ip,
        new_values={
            "command_id": cmd.id,
            "command": command_data.command,
            "value": command_data.value,
            "ttl_seconds": command_data.ttl_seconds
        }
    )
    
    return {
        "message": "Command created successfully",
        "command_id": cmd.id,
        "device_id": cmd.device_id,
        "command": cmd.command_type,
        "value": cmd.command_value,
        "status": cmd.status.value,
        "ttl_seconds": cmd.ttl_seconds,
        "expires_at": cmd.expires_at.isoformat() if cmd.expires_at else None,
        "created_at": cmd.created_at.isoformat() if cmd.created_at else None
    }

@app.get("/devices/{device_id}/commands/pending")
def get_pending_device_commands(
    device_id: str,
    x_signature: str = Header(None, alias="X-Signature"),
    x_timestamp: str = Header(None, alias="X-Timestamp"),
    limit: int = Query(50, ge=1, le=200, description="Maximum number of commands to return"),
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
    
    now = datetime.utcnow()
    expire_overdue_commands(db, now)
    
    commands = db.query(DeviceCommand).filter(
        DeviceCommand.device_id == device_id,
        DeviceCommand.status == CommandStatus.PENDING,
        DeviceCommand.expires_at > now
    ).order_by(DeviceCommand.created_at.asc()).limit(limit).all()
    
    for cmd in commands:
        cmd.status = CommandStatus.DELIVERED
        cmd.delivered_at = now
        cmd.updated_at = now
    
    db.commit()
    
    result = []
    for cmd in commands:
        result.append({
            "id": cmd.id,
            "command": cmd.command_type,
            "value": cmd.command_value,
            "created_at": cmd.created_at.isoformat() if cmd.created_at else None,
            "ttl_remaining": int((cmd.expires_at - now).total_seconds()) if cmd.expires_at else 0
        })
    
    return {
        "device_id": device_id,
        "pending_count": len(result),
        "commands": result,
        "queried_at": now.isoformat()
    }

@app.post("/devices/{device_id}/commands/{command_id}/ack")
def acknowledge_command(
    device_id: str,
    command_id: str,
    ack_data: CommandAck = None,
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
    
    if ack_data is None:
        ack_data = CommandAck()
    
    status_str = ack_data.status.lower()
    if status_str == "executed":
        new_status = CommandStatus.EXECUTED
    elif status_str == "failed":
        new_status = CommandStatus.FAILED
    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid status. Must be 'executed' or 'failed'."
        )
    
    cmd, success = update_command_status(
        cmd_id=command_id,
        new_status=new_status,
        db=db,
        result_data=ack_data.result,
        error_message=ack_data.error_message,
        client_msg_id=ack_data.client_msg_id
    )
    
    if cmd is None:
        raise HTTPException(
            status_code=404,
            detail="Command not found."
        )
    
    if not success:
        if cmd.status in [CommandStatus.EXECUTED, CommandStatus.FAILED, CommandStatus.EXPIRED]:
            return {
                "message": f"Command already {cmd.status.value} (idempotent response)",
                "command_id": cmd.id,
                "device_id": cmd.device_id,
                "status": cmd.status.value,
                "updated_at": cmd.updated_at.isoformat() if cmd.updated_at else None,
                "is_idempotent": True
            }
    
    return {
        "message": f"Command {new_status.value} successfully",
        "command_id": cmd.id,
        "device_id": cmd.device_id,
        "status": cmd.status.value,
        "updated_at": cmd.updated_at.isoformat() if cmd.updated_at else None,
        "is_idempotent": False
    }

@app.get("/devices/{device_id}/commands")
def get_device_commands(
    device_id: str,
    status: str = Query(None, description="Filter by status: pending, delivered, executed, failed, expired"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of commands to return"),
    x_user_id: str = Header(None, alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    device = db.query(Device).filter(
        Device.device_id == device_id
    ).first()
    
    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found."
        )
    
    query = db.query(DeviceCommand).filter(DeviceCommand.device_id == device_id)
    
    if status:
        try:
            status_enum = CommandStatus(status.lower())
            query = query.filter(DeviceCommand.status == status_enum)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid status: {status}. Must be one of: {', '.join([s.value for s in CommandStatus])}"
            )
    
    commands = query.order_by(desc(DeviceCommand.created_at)).limit(limit).all()
    
    result = []
    for cmd in commands:
        result.append({
            "id": cmd.id,
            "device_id": cmd.device_id,
            "command": cmd.command_type,
            "value": cmd.command_value,
            "status": cmd.status.value,
            "ttl_seconds": cmd.ttl_seconds,
            "expires_at": cmd.expires_at.isoformat() if cmd.expires_at else None,
            "delivered_at": cmd.delivered_at.isoformat() if cmd.delivered_at else None,
            "executed_at": cmd.executed_at.isoformat() if cmd.executed_at else None,
            "failed_at": cmd.failed_at.isoformat() if cmd.failed_at else None,
            "expired_at": cmd.expired_at.isoformat() if cmd.expired_at else None,
            "error_message": cmd.error_message,
            "reason": cmd.reason,
            "source": cmd.source,
            "created_at": cmd.created_at.isoformat() if cmd.created_at else None
        })
    
    return {
        "device_id": device_id,
        "count": len(result),
        "commands": result
    }

@app.websocket("/ws/devices/{device_id}")
async def websocket_device_endpoint(websocket: WebSocket, device_id: str):
    await ws_manager.connect(websocket, device_id)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                msg_type = message.get("type")
                
                if msg_type == "ping":
                    await websocket.send_json({"type": "pong", "timestamp": datetime.utcnow().isoformat()})
                elif msg_type == "ack":
                    command_id = message.get("command_id")
                    status = message.get("status", "executed")
                    result = message.get("result")
                    client_msg_id = message.get("client_msg_id")
                    
                    logger.info(f"[WebSocket] Received ack for command {command_id} from device {device_id}")
                    
                    await websocket.send_json({
                        "type": "ack_received",
                        "command_id": command_id,
                        "status": "processing"
                    })
                else:
                    logger.warning(f"[WebSocket] Unknown message type: {msg_type}")
                    
            except json.JSONDecodeError:
                logger.warning(f"[WebSocket] Invalid JSON message from device {device_id}")
                
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket, device_id)
        logger.info(f"[WebSocket] Device {device_id} disconnected")
    except Exception as e:
        logger.error(f"[WebSocket] Error for device {device_id}: {e}")
        ws_manager.disconnect(websocket, device_id)

@app.websocket("/ws/frontend")
async def websocket_frontend_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket, device_id=None)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                msg_type = message.get("type")
                
                if msg_type == "ping":
                    await websocket.send_json({"type": "pong", "timestamp": datetime.utcnow().isoformat()})
                elif msg_type == "subscribe_device":
                    device_id = message.get("device_id")
                    logger.info(f"[WebSocket] Frontend subscribed to device: {device_id}")
                    await websocket.send_json({
                        "type": "subscribed",
                        "device_id": device_id
                    })
                    
            except json.JSONDecodeError:
                logger.warning("[WebSocket] Invalid JSON message from frontend")
                
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket, device_id=None)
        logger.info("[WebSocket] Frontend disconnected")
    except Exception as e:
        logger.error(f"[WebSocket] Error for frontend: {e}")
        ws_manager.disconnect(websocket, device_id=None)

@app.get("/ws/status")
def get_websocket_status():
    return {
        "device_connections": {
            device_id: len(connections)
            for device_id, connections in ws_manager.active_connections.items()
        },
        "frontend_connections": len(ws_manager._frontend_connections)
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

@app.get("/devices/{device_id}/history/latest")
def get_device_history_latest(
    device_id: str,
    limit: int = Query(50, ge=1, le=200, description="Maximum number of records to return (max 200)"),
    db: Session = Depends(get_db)
):
    """
    获取设备最近 N 条原始历史数据（用于实时看板）
    
    与聚合接口不同，此接口返回原始数据点，不进行降采样。
    适用于实时监控场景，需要展示最近的温度波动曲线。
    
    返回字段：
    - timestamp: 数据记录时间（UTC）
    - temperature: 温度值
    - humidity: 湿度值
    - is_alert: 是否为告警数据
    """
    
    device = db.query(Device).filter(Device.device_id == device_id).first()
    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found"
        )
    
    history_records = db.query(DeviceDataHistory).filter(
        DeviceDataHistory.device_id == device_id,
        DeviceDataHistory.temperature != None
    ).order_by(desc(DeviceDataHistory.timestamp)).limit(limit).all()
    
    data_points = []
    for record in reversed(history_records):
        data_points.append({
            "timestamp": record.timestamp.isoformat() if record.timestamp else None,
            "temperature": float(record.temperature) if record.temperature is not None else None,
            "humidity": float(record.humidity) if record.humidity is not None else None,
            "is_alert": record.is_alert
        })
    
    return {
        "device_id": device_id,
        "limit": limit,
        "actual_count": len(data_points),
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

class ExportTaskCreate(BaseModel):
    device_id: str
    task_name: str = None
    start_time: str = None
    end_time: str = None
    include_payload: bool = True
    retention_hours: int = DEFAULT_REPORT_RETENTION_HOURS

@app.post("/api/reports/export")
def create_export_task(
    export_data: ExportTaskCreate,
    x_user_id: str = Header(None, alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    device = db.query(Device).filter(Device.device_id == export_data.device_id).first()
    if not device:
        raise HTTPException(
            status_code=404,
            detail=f"Device not found: {export_data.device_id}"
        )
    
    start_time = None
    if export_data.start_time:
        try:
            start_time = datetime.fromisoformat(export_data.start_time)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid start_time format. Use ISO format: YYYY-MM-DDTHH:MM:SS"
            )
    
    end_time = None
    if export_data.end_time:
        try:
            end_time = datetime.fromisoformat(export_data.end_time)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid end_time format. Use ISO format: YYYY-MM-DDTHH:MM:SS"
            )
    
    task = report_engine.create_export_task(
        db=db,
        device_id=export_data.device_id,
        task_name=export_data.task_name,
        user_id=x_user_id,
        start_time=start_time,
        end_time=end_time,
        include_payload=export_data.include_payload,
        retention_hours=export_data.retention_hours
    )
    
    success = report_engine.start_export_task_async(db, task.id, get_session_local_factory)
    
    if not success:
        raise HTTPException(
            status_code=500,
            detail="Failed to start export task"
        )
    
    task_status = report_engine.get_task_status(db, task.id)
    
    return {
        "message": "Export task created successfully",
        "task": task_status
    }

@app.get("/api/reports/{task_id}")
def get_report_task_status(
    task_id: str,
    x_user_id: str = Header(None, alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    task_status = report_engine.get_task_status(db, task_id)
    
    if not task_status:
        raise HTTPException(
            status_code=404,
            detail=f"Report task not found: {task_id}"
        )
    
    return task_status

@app.get("/api/reports/download/{task_id}")
def download_report(
    task_id: str,
    x_user_id: str = Header(None, alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    task = db.query(ReportTask).filter(ReportTask.id == task_id).first()
    
    if not task:
        raise HTTPException(
            status_code=404,
            detail=f"Report task not found: {task_id}"
        )
    
    if task.status != ReportTaskStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"Report is not ready. Current status: {task.status.value}"
        )
    
    if not task.file_path or not os.path.exists(task.file_path):
        raise HTTPException(
            status_code=404,
            detail="Report file not found"
        )
    
    if task.expire_at and datetime.utcnow() > task.expire_at:
        raise HTTPException(
            status_code=410,
            detail="Report has expired"
        )
    
    file_name = task.file_name or f"report_{task_id}.csv"
    media_type = "text/csv" if file_name.endswith('.csv') else "application/octet-stream"
    
    return FileResponse(
        path=task.file_path,
        media_type=media_type,
        filename=file_name
    )

@app.get("/api/reports")
def list_report_tasks(
    x_user_id: str = Header(None, alias="X-User-ID"),
    device_id: str = Query(None, description="Filter by device ID"),
    status: str = Query(None, description="Filter by status"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of tasks to return"),
    db: Session = Depends(get_db)
):
    query = db.query(ReportTask)
    
    if device_id:
        query = query.filter(ReportTask.device_id == device_id)
    
    if status:
        try:
            status_enum = ReportTaskStatus(status)
            query = query.filter(ReportTask.status == status_enum)
        except ValueError:
            pass
    
    tasks = query.order_by(desc(ReportTask.created_at)).limit(limit).all()
    
    result = []
    for task in tasks:
        result.append({
            "id": task.id,
            "task_name": task.task_name,
            "device_id": task.device_id,
            "status": task.status.value,
            "progress": task.progress,
            "record_count": task.record_count,
            "file_size_bytes": task.file_size_bytes,
            "file_name": task.file_name,
            "download_url": task.download_url,
            "error_message": task.error_message,
            "start_time": task.start_time.isoformat() if task.start_time else None,
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
            "expire_at": task.expire_at.isoformat() if task.expire_at else None,
            "created_at": task.created_at.isoformat() if task.created_at else None
        })
    
    return {
        "tasks": result,
        "count": len(result),
        "limit": limit
    }

@app.get("/api/scheduled-reports")
def list_scheduled_reports(
    x_user_id: str = Header(None, alias="X-User-ID"),
    is_active: bool = Query(None, description="Filter by active status"),
    db: Session = Depends(get_db)
):
    query = db.query(ScheduledReportConfig)
    
    if is_active is not None:
        query = query.filter(ScheduledReportConfig.is_active == is_active)
    
    configs = query.order_by(desc(ScheduledReportConfig.created_at)).all()
    
    result = []
    for config in configs:
        result.append({
            "id": config.id,
            "name": config.name,
            "description": config.description,
            "report_type": config.report_type.value if config.report_type else None,
            "cron_expression": config.cron_expression,
            "device_ids": config.device_ids,
            "filters": config.filters,
            "export_format": config.export_format,
            "output_directory": config.output_directory,
            "retention_days": config.retention_days,
            "file_name_template": config.file_name_template,
            "is_active": config.is_active,
            "last_run_at": config.last_run_at.isoformat() if config.last_run_at else None,
            "last_run_status": config.last_run_status,
            "last_run_error": config.last_run_error,
            "created_at": config.created_at.isoformat() if config.created_at else None,
            "updated_at": config.updated_at.isoformat() if config.updated_at else None
        })
    
    return {
        "configs": result,
        "count": len(result)
    }

class ScheduledReportCreate(BaseModel):
    name: str
    description: str = None
    report_type: str = "daily_briefing"
    cron_expression: str = "0 8 * * *"
    device_ids: list = None
    filters: dict = None
    export_format: str = "csv"
    output_directory: str = "./reports"
    retention_days: int = 7
    file_name_template: str = None
    is_active: bool = True

@app.post("/api/scheduled-reports")
def create_scheduled_report(
    config_data: ScheduledReportCreate,
    x_user_id: str = Header(None, alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    if x_user_id:
        require_staff_or_admin(x_user_id, db)
    
    try:
        report_type = ScheduledReportType(config_data.report_type)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid report_type. Must be one of: {[t.value for t in ScheduledReportType]}"
        )
    
    config = ScheduledReportConfig(
        id=str(uuid.uuid4()),
        name=config_data.name,
        description=config_data.description,
        report_type=report_type,
        cron_expression=config_data.cron_expression,
        device_ids=config_data.device_ids,
        filters=config_data.filters,
        export_format=config_data.export_format,
        output_directory=config_data.output_directory,
        retention_days=config_data.retention_days,
        file_name_template=config_data.file_name_template,
        is_active=config_data.is_active
    )
    
    db.add(config)
    db.commit()
    db.refresh(config)
    
    if config.is_active and scheduler:
        try:
            scheduled_manager = ScheduledReportManager(report_engine, scheduler)
            scheduled_manager.schedule_config(config, db)
        except Exception as e:
            logger.warning(f"[ScheduledReport] Failed to schedule new config: {e}")
    
    return {
        "message": "Scheduled report config created successfully",
        "config": {
            "id": config.id,
            "name": config.name,
            "report_type": config.report_type.value if config.report_type else None,
            "cron_expression": config.cron_expression,
            "is_active": config.is_active
        }
    }

@app.put("/api/scheduled-reports/{config_id}")
def update_scheduled_report(
    config_id: str,
    config_data: ScheduledReportCreate,
    x_user_id: str = Header(None, alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    if x_user_id:
        require_staff_or_admin(x_user_id, db)
    
    config = db.query(ScheduledReportConfig).filter(
        ScheduledReportConfig.id == config_id
    ).first()
    
    if not config:
        raise HTTPException(
            status_code=404,
            detail=f"Scheduled report config not found: {config_id}"
        )
    
    config.name = config_data.name
    config.description = config_data.description
    config.cron_expression = config_data.cron_expression
    config.device_ids = config_data.device_ids
    config.filters = config_data.filters
    config.export_format = config_data.export_format
    config.output_directory = config_data.output_directory
    config.retention_days = config_data.retention_days
    config.file_name_template = config_data.file_name_template
    config.is_active = config_data.is_active
    
    try:
        config.report_type = ScheduledReportType(config_data.report_type)
    except ValueError:
        pass
    
    db.commit()
    db.refresh(config)
    
    if scheduler:
        try:
            scheduled_manager = ScheduledReportManager(report_engine, scheduler)
            scheduled_manager.schedule_config(config, db)
        except Exception as e:
            logger.warning(f"[ScheduledReport] Failed to reschedule config: {e}")
    
    return {
        "message": "Scheduled report config updated successfully",
        "config": {
            "id": config.id,
            "name": config.name,
            "is_active": config.is_active
        }
    }

@app.delete("/api/scheduled-reports/{config_id}")
def delete_scheduled_report(
    config_id: str,
    x_user_id: str = Header(None, alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    if x_user_id:
        require_staff_or_admin(x_user_id, db)
    
    config = db.query(ScheduledReportConfig).filter(
        ScheduledReportConfig.id == config_id
    ).first()
    
    if not config:
        raise HTTPException(
            status_code=404,
            detail=f"Scheduled report config not found: {config_id}"
        )
    
    if scheduler:
        try:
            job_id = f"scheduled_report_{config_id}"
            scheduler.remove_job(job_id)
        except Exception as e:
            logger.warning(f"[ScheduledReport] Failed to remove job: {e}")
    
    db.delete(config)
    db.commit()
    
    return {
        "message": "Scheduled report config deleted successfully",
        "config_id": config_id
    }
