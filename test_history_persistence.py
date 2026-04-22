import time
import hashlib
import uuid
import httpx
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server.models import Base, Device, DeviceDataHistory

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

def report_device_data(
    client: httpx.Client,
    device_id: str,
    secret_key: str,
    payload: dict
) -> dict:
    timestamp = str(int(time.time()))
    signature = compute_signature(device_id, timestamp, secret_key)
    
    response = client.post(
        f"{BASE_URL}/devices/data",
        json={"device_id": device_id, "payload": payload},
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

def get_history_count(db, device_id: str) -> int:
    count = db.query(DeviceDataHistory).filter(
        DeviceDataHistory.device_id == device_id
    ).count()
    return count

def get_all_history(db, device_id: str) -> list:
    records = db.query(DeviceDataHistory).filter(
        DeviceDataHistory.device_id == device_id
    ).order_by(desc(DeviceDataHistory.timestamp)).all()
    return records

def main():
    print("=" * 60)
    print("测试设备历史数据持久化功能")
    print("=" * 60)
    
    device_id = f"test_{uuid.uuid4().hex[:8]}"
    model = "TempSensor-Test"
    
    print(f"\n[1] 注册测试设备: {device_id}")
    with httpx.Client() as client:
        register_result = register_device(client, device_id, model)
        secret_key = register_result["secret_key"]
        print(f"    注册成功")
    
    print("\n[2] 清理历史记录 (如果存在)")
    db = SessionLocal()
    try:
        existing = db.query(DeviceDataHistory).filter(
            DeviceDataHistory.device_id == device_id
        ).all()
        for record in existing:
            db.delete(record)
        db.commit()
        print(f"    已清理 {len(existing)} 条旧记录")
    finally:
        db.close()
    
    print("\n[3] 连续上报 5 条不同的温度数据")
    temperature_data = [
        {"temperature": 25.5, "humidity": 60},
        {"temperature": 26.2, "humidity": 62},
        {"temperature": 24.8, "humidity": 58},
        {"temperature": 27.1, "humidity": 65},
        {"temperature": 25.9, "humidity": 61},
    ]
    
    with httpx.Client() as client:
        for i, payload in enumerate(temperature_data, 1):
            print(f"    上报第 {i} 条数据: temperature={payload['temperature']}°C")
            result = report_device_data(client, device_id, secret_key, payload)
            print(f"        成功: history_id={result['history_id'][:8]}...")
            time.sleep(0.2)
    
    print("\n[4] 验证数据库中的历史记录")
    db = SessionLocal()
    try:
        history_count = get_history_count(db, device_id)
        print(f"    数据库中历史记录数量: {history_count}")
        
        if history_count != 5:
            print(f"    [FAILED] 期望 5 条记录，但实际只有 {history_count} 条")
            return False
        
        print("    [PASSED] 记录数量正确")
        
        history_records = get_all_history(db, device_id)
        print(f"\n[5] 验证每条记录的数据完整性")
        
        all_match = True
        for i, record in enumerate(reversed(history_records), 1):
            expected_payload = temperature_data[i - 1]
            actual_payload = record.payload
            
            print(f"\n    记录 {i}:")
            print(f"        期望: {expected_payload}")
            print(f"        实际: {actual_payload}")
            
            if actual_payload != expected_payload:
                print(f"        [FAILED] 数据不匹配!")
                all_match = False
            else:
                print(f"        [PASSED] 数据匹配")
                print(f"        时间戳: {record.timestamp}")
        
        if not all_match:
            return False
        
        print("\n" + "=" * 60)
        print("所有测试通过! 历史数据持久化功能正常工作。")
        print("=" * 60)
        return True
        
    finally:
        db.close()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
