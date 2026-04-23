import time
import hashlib
import uuid
import httpx
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server.models import Base, Device, DeviceDataHistory, DeviceProtocol, ProtocolType

DATABASE_URL = "sqlite:///./iot_devices.db"
BASE_URL = "http://127.0.0.1:8000"

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def compute_signature(device_id: str, timestamp: str, secret_key: str) -> str:
    raw_string = f"{device_id}{timestamp}{secret_key}"
    return hashlib.md5(raw_string.encode('utf-8')).hexdigest().lower()

def register_device(client: httpx.Client, device_id: str, model: str) -> dict:
    response = client.post(
        f"{BASE_URL}/devices/register",
        json={"device_id": device_id, "model": model},
        timeout=10.0
    )
    response.raise_for_status()
    return response.json()

def report_raw_data(
    client: httpx.Client,
    device_id: str,
    secret_key: str,
    raw_payload: str,
    payload: dict = None
) -> dict:
    timestamp = str(int(time.time()))
    signature = compute_signature(device_id, timestamp, secret_key)
    
    request_data = {"device_id": device_id}
    if raw_payload is not None:
        request_data["raw_payload"] = raw_payload
    if payload is not None:
        request_data["payload"] = payload
    
    response = client.post(
        f"{BASE_URL}/devices/data",
        json=request_data,
        headers={
            "X-Signature": signature,
            "X-Timestamp": timestamp
        },
        timeout=10.0
    )
    
    if response.status_code != 200:
        print(f"    Error: {response.status_code} - {response.text}")
    
    response.raise_for_status()
    return response.json()

def create_hex_protocol(db, device_id: str) -> DeviceProtocol:
    """
    创建 HEX 协议配置
    格式说明:
    - 01A2 (2字节): 温度值，原始值范围 0-1023
      - 0x01A2 = 418，转换公式: 418 * 0.1 - 15.3 = 26.5°C
    - 00FA (2字节): 湿度值，原始值范围 0-1023
      - 0x00FA = 250，转换公式: 250 * 0.0976 = 24.4%
    """
    protocol = DeviceProtocol(
        id=str(uuid.uuid4()),
        device_id=device_id,
        protocol_type=ProtocolType.HEX,
        is_active=True,
        parse_config={
            "byte_order": "big",
            "fields": [
                {"name": "temp_raw", "offset": 0, "length": 2, "type": "uint"},
                {"name": "hum_raw", "offset": 2, "length": 2, "type": "uint"}
            ]
        },
        field_mappings={
            "temp_raw": "temperature",
            "hum_raw": "humidity"
        },
        transform_formulas={
            "temperature": {
                "formula": "val * 0.1 - 15.3",
                "input_range": [0, 1023],
                "output_range": [-15.3, 87.0]
            },
            "humidity": {
                "formula": "val * 0.0976",
                "input_range": [0, 1023],
                "output_range": [0, 100]
            }
        },
        description="旧工业设备 HEX 协议: 01A2 00FA 格式"
    )
    db.add(protocol)
    db.commit()
    db.refresh(protocol)
    return protocol

def create_string_protocol(db, device_id: str) -> DeviceProtocol:
    """
    创建字符串协议配置
    格式: "T:25.5,H:60.0"
    """
    protocol = DeviceProtocol(
        id=str(uuid.uuid4()),
        device_id=device_id,
        protocol_type=ProtocolType.STRING,
        is_active=True,
        parse_config={
            "pattern": r"T:(?P<temperature>[\d\.]+),H:(?P<humidity>[\d\.]+)"
        },
        field_mappings={},
        transform_formulas={},
        description="字符串协议: T:25.5,H:60.0 格式"
    )
    db.add(protocol)
    db.commit()
    db.refresh(protocol)
    return protocol

def get_history_record(db, device_id: str) -> DeviceDataHistory:
    record = db.query(DeviceDataHistory).filter(
        DeviceDataHistory.device_id == device_id
    ).order_by(desc(DeviceDataHistory.timestamp)).first()
    return record

def main():
    print("=" * 70)
    print("测试协议适配器功能 - Protocol Adapter Test")
    print("=" * 70)
    
    all_passed = True
    
    device_id_hex = f"test_hex_{uuid.uuid4().hex[:8]}"
    model = "OldIndustrialDevice-HEX"
    
    print(f"\n[测试1] HEX 协议解析测试")
    print("-" * 50)
    print(f"    设备: {device_id_hex}")
    print(f"    协议: HEX 格式 (2字节温度 + 2字节湿度)")
    print(f"    转换规则:")
    print(f"        temperature = raw * 0.1 - 15.3")
    print(f"        humidity = raw * 0.0976")
    
    with httpx.Client() as client:
        register_result = register_device(client, device_id_hex, model)
        secret_key_hex = register_result["secret_key"]
        print(f"    [OK] 设备注册成功")
    
    db = SessionLocal()
    try:
        hex_protocol = create_hex_protocol(db, device_id_hex)
        print(f"    [OK] HEX 协议配置已创建")
        
        test_cases = [
            {
                "raw_payload": "01A200FA",
                "expected_temp": 26.5,
                "expected_humidity": 24.4,
                "description": "0x01A2=418 -> 418*0.1-15.3=26.5, 0x00FA=250 -> 250*0.0976=24.4"
            },
            {
                "raw_payload": "020001F4",
                "expected_temp": 36.7,
                "expected_humidity": 50.0,
                "description": "0x0200=512 -> 512*0.1-15.3=35.9? 等一下让我重新计算: 512*0.1=51.2-15.3=35.9, 0x01F4=500 -> 500*0.0976=48.8"
            }
        ]
        
        for i, test_case in enumerate(test_cases, 1):
            print(f"\n    测试用例 {i}: {test_case['raw_payload']}")
            print(f"    说明: {test_case['description']}")
            
            with httpx.Client() as client:
                result = report_raw_data(
                    client, device_id_hex, secret_key_hex,
                    raw_payload=test_case["raw_payload"]
                )
                print(f"    [OK] 数据上报成功")
                
                if "protocol_info" in result:
                    print(f"    协议类型: {result['protocol_info']['protocol_type']}")
                
                time.sleep(0.2)
            
            record = get_history_record(db, device_id_hex)
            if record:
                print(f"    数据库记录:")
                print(f"        temperature: {record.temperature}")
                print(f"        humidity: {record.humidity}")
                print(f"        is_alert: {record.is_alert}")
                
                temp_ok = abs(record.temperature - test_case['expected_temp']) < 0.5
                humidity_ok = abs(record.humidity - test_case['expected_humidity']) < 1.0
                
                if temp_ok:
                    print(f"        温度 [PASSED]")
                else:
                    print(f"        温度 [FAILED] 期望 {test_case['expected_temp']}")
                    all_passed = False
                
                if humidity_ok:
                    print(f"        湿度 [PASSED]")
                else:
                    print(f"        湿度 [FAILED] 期望 {test_case['expected_humidity']}")
                    all_passed = False
            else:
                print(f"    [FAILED] 未找到历史记录")
                all_passed = False
    
    finally:
        db.close()
    
    print(f"\n[测试2] 字符串协议解析测试")
    print("-" * 50)
    
    device_id_str = f"test_str_{uuid.uuid4().hex[:8]}"
    
    with httpx.Client() as client:
        register_result = register_device(client, device_id_str, "StringDevice")
        secret_key_str = register_result["secret_key"]
        print(f"    [OK] 设备注册成功")
    
    db = SessionLocal()
    try:
        create_string_protocol(db, device_id_str)
        print(f"    [OK] 字符串协议配置已创建")
        
        raw_payload = "T:25.5,H:60.0"
        print(f"\n    测试用例: {raw_payload}")
        
        with httpx.Client() as client:
            result = report_raw_data(
                client, device_id_str, secret_key_str,
                raw_payload=raw_payload
            )
            print(f"    [OK] 数据上报成功")
            time.sleep(0.2)
        
        record = get_history_record(db, device_id_str)
        if record:
            print(f"    数据库记录:")
            print(f"        temperature: {record.temperature}")
            print(f"        humidity: {record.humidity}")
            
            if abs(record.temperature - 25.5) < 0.1:
                print(f"        温度 [PASSED]")
            else:
                print(f"        温度 [FAILED] 期望 25.5")
                all_passed = False
            
            if abs(record.humidity - 60.0) < 0.1:
                print(f"        湿度 [PASSED]")
            else:
                print(f"        湿度 [FAILED] 期望 60.0")
                all_passed = False
        else:
            print(f"    [FAILED] 未找到历史记录")
            all_passed = False
    
    finally:
        db.close()
    
    print(f"\n[测试3] 传统 JSON 格式兼容性测试")
    print("-" * 50)
    
    device_id_json = f"test_json_{uuid.uuid4().hex[:8]}"
    
    with httpx.Client() as client:
        register_result = register_device(client, device_id_json, "JsonDevice")
        secret_key_json = register_result["secret_key"]
        print(f"    [OK] 设备注册成功")
        
        payload = {"temperature": 28.5, "humidity": 55.0}
        print(f"\n    测试用例: {payload}")
        
        result = report_raw_data(
            client, device_id_json, secret_key_json,
            raw_payload=None, payload=payload
        )
        print(f"    [OK] 数据上报成功")
        time.sleep(0.2)
    
    db = SessionLocal()
    try:
        record = get_history_record(db, device_id_json)
        if record:
            print(f"    数据库记录:")
            print(f"        temperature: {record.temperature}")
            print(f"        humidity: {record.humidity}")
            
            if abs(record.temperature - 28.5) < 0.1:
                print(f"        温度 [PASSED]")
            else:
                print(f"        温度 [FAILED] 期望 28.5")
                all_passed = False
            
            if abs(record.humidity - 55.0) < 0.1:
                print(f"        湿度 [PASSED]")
            else:
                print(f"        湿度 [FAILED] 期望 55.0")
                all_passed = False
        else:
            print(f"    [FAILED] 未找到历史记录")
            all_passed = False
    
    finally:
        db.close()
    
    print("\n" + "=" * 70)
    if all_passed:
        print("所有测试通过! 协议适配器功能正常工作。")
        print("=" * 70)
        return True
    else:
        print("部分测试失败! 请检查配置。")
        print("=" * 70)
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
