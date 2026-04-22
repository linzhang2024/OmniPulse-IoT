import sys
import os
import time
import hashlib
import uuid
import random
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import httpx
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "sqlite:///./iot_devices.db"
BASE_URL = "http://127.0.0.1:8000"
TEMPERATURE_THRESHOLD = 50

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def compute_signature(device_id: str, timestamp: str, secret_key: str) -> str:
    raw_string = f"{device_id}{timestamp}{secret_key}"
    return hashlib.md5(raw_string.encode('utf-8')).hexdigest().lower()

async def register_device(client: httpx.AsyncClient, device_id: str, model: str) -> dict:
    response = await client.post(
        f"{BASE_URL}/devices/register",
        json={"device_id": device_id, "model": model},
        timeout=10.0
    )
    response.raise_for_status()
    return response.json()

async def report_device_data(
    client: httpx.AsyncClient,
    device_id: str,
    secret_key: str,
    payload: dict
) -> dict:
    timestamp = str(int(time.time()))
    signature = compute_signature(device_id, timestamp, secret_key)
    
    response = await client.post(
        f"{BASE_URL}/devices/data",
        json={"device_id": device_id, "payload": payload},
        headers={
            "X-Signature": signature,
            "X-Timestamp": timestamp
        },
        timeout=10.0
    )
    
    response.raise_for_status()
    return response.json()

async def get_device_history(
    client: httpx.AsyncClient,
    device_id: str,
    start_time: str,
    end_time: str,
    interval_seconds: int
) -> dict:
    url = f"{BASE_URL}/devices/{device_id}/history"
    params = {
        "start_time": start_time,
        "end_time": end_time,
        "interval_seconds": interval_seconds
    }
    
    response = await client.get(url, params=params, timeout=30.0)
    return response

async def test_quick():
    print("=" * 70)
    print("时序数据聚合接口快速验证")
    print("=" * 70)
    
    device_id = f"quick_{uuid.uuid4().hex[:6]}"
    model = "QuickTestSensor-001"
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        print("\n[1] 检查接口是否存在...")
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(hours=1)
        start_time_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
        end_time_str = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
        
        response = await get_device_history(
            client, "nonexistent",
            start_time_str, end_time_str, 60
        )
        
        print(f"    HTTP 状态码: {response.status_code}")
        print(f"    响应: {response.text[:200] if response.text else '空'}")
        
        if response.status_code == 404:
            print("    [OK] 接口存在（对不存在的设备返回 404）")
        elif response.status_code == 422:
            print("    [OK] 接口存在（参数校验返回 422）")
        elif response.status_code == 400:
            print("    [OK] 接口存在（返回 400）")
        else:
            print(f"    接口状态未知: {response.status_code}")
        
        print("\n[2] 测试参数校验...")
        
        print("\n    测试: start_time > end_time")
        response = await get_device_history(
            client, "test_device",
            end_time_str, start_time_str, 60
        )
        print(f"        状态码: {response.status_code}")
        if response.status_code == 422:
            print(f"        [OK] 正确返回 422: {response.json().get('detail')}")
        elif response.status_code == 404:
            print(f"        接口存在，但设备不存在")
        else:
            print(f"        响应: {response.text[:200]}")
        
        print("\n    测试: interval_seconds < 10")
        response = await get_device_history(
            client, "test_device",
            start_time_str, end_time_str, 5
        )
        print(f"        状态码: {response.status_code}")
        if response.status_code == 422:
            print(f"        [OK] 正确返回 422: {response.json().get('detail')}")
        elif response.status_code == 404:
            print(f"        接口存在，但设备不存在")
        else:
            print(f"        响应: {response.text[:200]}")
        
        print("\n[3] 注册测试设备并写入数据...")
        
        print(f"\n    注册设备: {device_id}")
        try:
            register_result = await register_device(client, device_id, model)
            secret_key = register_result["secret_key"]
            print(f"    注册成功: secret_key={secret_key[:8]}...")
        except Exception as e:
            print(f"    注册失败: {e}")
            return False
        
        print("\n    写入 100 条测试数据（包含 10% 告警数据）...")
        test_records = 100
        normal_count = 0
        alert_count = 0
        all_temps = []
        
        start_write = time.time()
        for i in range(test_records):
            if random.random() < 0.1:
                temperature = round(random.uniform(55, 100), 1)
                alert_count += 1
            else:
                temperature = round(random.uniform(20, 45), 1)
                normal_count += 1
            
            humidity = round(random.uniform(30, 80), 1)
            all_temps.append(temperature)
            
            payload = {
                "temperature": temperature,
                "humidity": humidity
            }
            
            try:
                await report_device_data(client, device_id, secret_key, payload)
            except Exception as e:
                print(f"    写入第 {i+1} 条失败: {e}")
        
        write_elapsed = time.time() - start_write
        print(f"    写入完成:")
        print(f"        正常数据: {normal_count} 条")
        print(f"        告警数据: {alert_count} 条")
        print(f"        总耗时: {write_elapsed:.2f} 秒")
        
        print("\n[4] 验证数据库中的聚合字段...")
        db = SessionLocal()
        try:
            result = db.execute(text("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(temperature) as temp_count,
                    SUM(CASE WHEN is_alert = 1 THEN 1 ELSE 0 END) as alert_records,
                    MAX(temperature) as max_temp,
                    MIN(temperature) as min_temp,
                    AVG(temperature) as avg_temp
                FROM device_data_history 
                WHERE device_id = :device_id
            """), {"device_id": device_id})
            
            row = result.fetchone()
            total = row[0]
            temp_count = row[1]
            alert_records = row[2]
            max_temp = row[3]
            min_temp = row[4]
            avg_temp = row[5]
            
            print(f"\n    数据库验证:")
            print(f"        总记录数: {total}")
            print(f"        有温度值的记录: {temp_count}")
            print(f"        告警记录数: {alert_records}")
            print(f"        最大温度: {max_temp}")
            print(f"        最小温度: {min_temp}")
            print(f"        平均温度: {avg_temp}")
            
            if temp_count == total and temp_count > 0:
                print(f"        [OK] 所有记录都有 temperature 值")
            else:
                print(f"        [WARNING] 部分记录没有 temperature 值")
            
            if alert_records > 0:
                print(f"        [OK] is_alert 字段正确标记了告警数据")
            
        finally:
            db.close()
        
        print("\n[5] 测试聚合查询...")
        
        end_dt = datetime.utcnow() + timedelta(hours=12)
        start_dt = end_dt - timedelta(hours=48)
        start_time_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
        end_time_str = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
        
        print(f"\n    查询参数 (UTC):")
        print(f"        时间范围: {start_time_str} ~ {end_time_str}")
        print(f"        聚合间隔: 60 秒")
        
        query_start = time.time()
        response = await get_device_history(
            client, device_id,
            start_time_str, end_time_str, 60
        )
        query_elapsed = time.time() - query_start
        
        print(f"\n    查询耗时: {query_elapsed:.3f} 秒")
        print(f"    HTTP 状态码: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n    [OK] 聚合查询成功!")
            print(f"        设备ID: {result.get('device_id')}")
            print(f"        数据点数量: {result.get('total_data_points')}")
            
            data_points = result.get('data_points', [])
            if data_points:
                print(f"\n    聚合数据点详情:")
                for i, point in enumerate(data_points[:5], 1):
                    temp = point.get('temperature', {})
                    avg_str = f"{temp.get('avg'):.2f}" if temp.get('avg') is not None else "None"
                    print(f"\n        数据点 {i}:")
                    print(f"            时间窗口: {point.get('window_start')} ~ {point.get('window_end')}")
                    print(f"            记录数量: {point.get('record_count')}")
                    print(f"            温度 max: {temp.get('max')}, min: {temp.get('min')}, avg: {avg_str}")
                    print(f"            有告警: {point.get('has_alert')}")
                
                total_records = sum(p.get('record_count', 0) for p in data_points)
                print(f"\n    聚合验证:")
                print(f"        聚合后总记录数: {total_records}")
                print(f"        原始记录数: {total}")
                
                has_alert_points = sum(1 for p in data_points if p.get('has_alert'))
                print(f"        有告警的聚合点数量: {has_alert_points}")
                
                if has_alert_points > 0 and alert_records > 0:
                    print(f"        [OK] 告警穿透逻辑正常工作")
        
        else:
            print(f"    查询失败: {response.text}")
        
        print("\n" + "=" * 70)
        print("快速验证完成")
        print("=" * 70)
        
        return True

async def main():
    try:
        success = await test_quick()
        return 0 if success else 1
    except Exception as e:
        print(f"\n测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    import asyncio
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
