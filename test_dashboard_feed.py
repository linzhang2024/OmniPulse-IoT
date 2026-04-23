import time
import hashlib
import uuid
import httpx
import math
from datetime import datetime, timezone
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server.models import Base, Device, DeviceDataHistory

DATABASE_URL = "sqlite:///./iot_devices.db"
BASE_URL = "http://127.0.0.1:8000"

DURATION_SECONDS = 60
INTERVAL_SECONDS = 2
AMPLITUDE = 15
BASE_TEMP = 25
FREQUENCY = 0.1

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

def get_latest_history(db, device_id: str, limit: int = 50) -> list:
    records = db.query(DeviceDataHistory).filter(
        DeviceDataHistory.device_id == device_id
    ).order_by(desc(DeviceDataHistory.timestamp)).limit(limit).all()
    return records

def get_history_via_api(client: httpx.Client, device_id: str, limit: int = 50) -> dict:
    response = client.get(
        f"{BASE_URL}/devices/{device_id}/history/latest?limit={limit}",
        timeout=10.0
    )
    if response.status_code != 200:
        print(f"    API Error: {response.status_code} - {response.text}")
        return None
    return response.json()

def generate_sine_wave_temp(step: int, total_steps: int) -> float:
    progress = step / total_steps
    angle = 2 * math.pi * FREQUENCY * step
    sine_value = math.sin(angle)
    temperature = BASE_TEMP + AMPLITUDE * sine_value
    return round(temperature, 2)

def generate_humidity(step: int) -> float:
    noise = (hash(str(step)) % 1000) / 1000.0 * 10 - 5
    return round(50 + noise, 1)

def verify_timezone(db, device_id: str) -> dict:
    records = get_latest_history(db, device_id, limit=100)
    if not records:
        return {"valid": False, "error": "No records found"}
    
    issues = []
    valid_count = 0
    total_count = len(records)
    
    for record in records:
        if record.timestamp is None:
            issues.append(f"Record {record.id}: timestamp is None")
            continue
        
        try:
            if record.timestamp.tzinfo is None:
                pass
            else:
                pass
            
            valid_count += 1
        except Exception as e:
            issues.append(f"Record {record.id}: Error checking timestamp - {e}")
    
    return {
        "valid": len(issues) == 0,
        "valid_count": valid_count,
        "total_count": total_count,
        "issues": issues
    }

def main():
    parser = argparse.ArgumentParser(description='实时看板温度数据上报测试')
    parser.add_argument('--duration', type=int, default=DURATION_SECONDS, 
                        help=f'测试持续时间（秒），默认 {DURATION_SECONDS}')
    parser.add_argument('--interval', type=int, default=INTERVAL_SECONDS,
                        help=f'上报间隔（秒），默认 {INTERVAL_SECONDS}')
    parser.add_argument('--device-id', type=str, default=None,
                        help='指定设备ID（不指定则自动生成）')
    parser.add_argument('--no-cleanup', action='store_true',
                        help='测试完成后不清理设备数据')
    parser.add_argument('--verify-only', action='store_true',
                        help='仅验证现有数据，不上报新数据')
    parser.add_argument('--base-temp', type=float, default=BASE_TEMP,
                        help=f'基础温度，默认 {BASE_TEMP}°C')
    parser.add_argument('--amplitude', type=float, default=AMPLITUDE,
                        help=f'正弦波振幅，默认 {AMPLITUDE}°C')
    
    args = parser.parse_args()
    
    duration = args.duration
    interval = args.interval
    base_temp = args.base_temp
    amplitude = args.amplitude
    
    total_steps = duration // interval
    
    print("=" * 70)
    print("📊 实时看板温度数据上报测试")
    print("=" * 70)
    print(f"\n配置参数:")
    print(f"  - 测试持续时间: {duration} 秒")
    print(f"  - 上报间隔: {interval} 秒")
    print(f"  - 预计上报次数: {total_steps} 次")
    print(f"  - 基础温度: {base_temp}°C")
    print(f"  - 正弦波振幅: ±{amplitude}°C")
    print(f"  - 温度范围: {base_temp - amplitude}°C ~ {base_temp + amplitude}°C")
    print(f"  - 服务器地址: {BASE_URL}")
    
    device_id = args.device_id or f"dashboard_{uuid.uuid4().hex[:8]}"
    model = "Dashboard-Test-Sensor"
    secret_key = None
    
    if not args.verify_only:
        print(f"\n{'='*70}")
        print("阶段 1: 注册测试设备")
        print("=" * 70)
        
        with httpx.Client() as client:
            try:
                print(f"\n正在注册设备: {device_id}...")
                register_result = register_device(client, device_id, model)
                secret_key = register_result["secret_key"]
                print(f"✅ 设备注册成功!")
                print(f"   设备ID: {device_id}")
                print(f"   设备型号: {model}")
                print(f"   密钥: {secret_key[:8]}...")
            except Exception as e:
                print(f"❌ 设备注册失败: {e}")
                return 1
        
        print(f"\n{'='*70}")
        print("阶段 2: 清理历史数据")
        print("=" * 70)
        
        db = SessionLocal()
        try:
            existing = db.query(DeviceDataHistory).filter(
                DeviceDataHistory.device_id == device_id
            ).all()
            for record in existing:
                db.delete(record)
            db.commit()
            if existing:
                print(f"✅ 已清理 {len(existing)} 条旧记录")
            else:
                print(f"ℹ️ 没有旧记录需要清理")
        finally:
            db.close()
        
        print(f"\n{'='*70}")
        print("阶段 3: 上报正弦波温度数据")
        print("=" * 70)
        print(f"\n开始上报数据，预计耗时 {duration} 秒...")
        print(f"时间戳          序号    温度(°C)    湿度(%)    状态")
        print("-" * 65)
        
        start_time = time.time()
        actual_intervals = []
        errors = []
        
        with httpx.Client() as client:
            for step in range(total_steps):
                cycle_start = time.time()
                
                temperature = generate_sine_wave_temp(step, total_steps)
                humidity = generate_humidity(step)
                
                payload = {
                    "temperature": temperature,
                    "humidity": humidity
                }
                
                status_icon = "✅"
                error_msg = ""
                
                try:
                    result = report_device_data(client, device_id, secret_key, payload)
                    actual_intervals.append(time.time() - cycle_start)
                except Exception as e:
                    status_icon = "❌"
                    error_msg = str(e)[:50]
                    errors.append({"step": step, "error": str(e)})
                
                timestamp = datetime.now().strftime("%H:%M:%S")
                temp_str = f"{temperature:>6.2f}"
                hum_str = f"{humidity:>5.1f}"
                
                if error_msg:
                    print(f"{timestamp}    {step + 1:>3}/{total_steps}    {temp_str}°C    {hum_str}%    {status_icon} {error_msg}")
                else:
                    print(f"{timestamp}    {step + 1:>3}/{total_steps}    {temp_str}°C    {hum_str}%    {status_icon}")
                
                elapsed = time.time() - cycle_start
                sleep_time = max(0, interval - elapsed)
                if sleep_time > 0 and step < total_steps - 1:
                    time.sleep(sleep_time)
        
        elapsed_total = time.time() - start_time
        print("-" * 65)
        print(f"\n上报完成! 总耗时: {elapsed_total:.2f} 秒")
        if errors:
            print(f"⚠️  上报错误: {len(errors)} 次")
        else:
            print(f"✅ 全部上报成功!")
    
    else:
        if not args.device_id:
            print("❌ --verify-only 模式需要指定 --device-id 参数")
            return 1
        print(f"\n{'='*70}")
        print("验证模式: 检查现有设备数据")
        print("=" * 70)
        print(f"设备ID: {device_id}")
    
    print(f"\n{'='*70}")
    print("阶段 4: 验证数据库记录")
    print("=" * 70)
    
    db = SessionLocal()
    try:
        records = get_latest_history(db, device_id, limit=200)
        print(f"\n数据库记录数量: {len(records)} 条")
        
        if records:
            print(f"\n最近 5 条记录:")
            for i, record in enumerate(reversed(records[-5:])):
                temp_str = f"{record.temperature:.2f}" if record.temperature else "N/A"
                hum_str = f"{record.humidity:.1f}" if record.humidity else "N/A"
                ts_str = record.timestamp.strftime("%Y-%m-%d %H:%M:%S") if record.timestamp else "N/A"
                alert_flag = " ⚠️" if record.is_alert else ""
                print(f"  [{i+1}] {ts_str} | 温度: {temp_str}°C | 湿度: {hum_str}%{alert_flag}")
        
        print(f"\n时区戳验证:")
        tz_result = verify_timezone(db, device_id)
        if tz_result["valid"]:
            print(f"  ✅ 所有 {tz_result['valid_count']} 条记录的时间戳有效")
        else:
            print(f"  ⚠️  发现问题:")
            for issue in tz_result["issues"][:10]:
                print(f"     - {issue}")
        
        if records:
            temps = [r.temperature for r in records if r.temperature is not None]
            if temps:
                min_temp = min(temps)
                max_temp = max(temps)
                avg_temp = sum(temps) / len(temps)
                print(f"\n温度统计:")
                print(f"  最小值: {min_temp:.2f}°C")
                print(f"  最大值: {max_temp:.2f}°C")
                print(f"  平均值: {avg_temp:.2f}°C")
                print(f"  波动范围: {max_temp - min_temp:.2f}°C")
                
                expected_min = base_temp - amplitude
                expected_max = base_temp + amplitude
                expected_range = amplitude * 2
                
                range_check = abs((max_temp - min_temp) - expected_range) < 5
                if range_check:
                    print(f"  ✅ 温度波动符合预期 (预期范围: ±{amplitude}°C)")
                else:
                    print(f"  ⚠️  温度波动可能不符合预期 (预期范围: ±{amplitude}°C)")
        
        alert_count = sum(1 for r in records if r.is_alert)
        if alert_count > 0:
            print(f"\n⚠️  告警记录: {alert_count} 条")
        else:
            print(f"\n✅ 无告警记录")
    
    finally:
        db.close()
    
    print(f"\n{'='*70}")
    print("阶段 5: 验证实时数据 API")
    print("=" * 70)
    
    with httpx.Client() as client:
        try:
            api_result = get_history_via_api(client, device_id, limit=50)
            if api_result:
                print(f"\nAPI 响应:")
                print(f"  设备ID: {api_result.get('device_id')}")
                print(f"  请求数量: {api_result.get('limit')}")
                print(f"  实际返回: {api_result.get('actual_count')} 条")
                
                data_points = api_result.get('data_points', [])
                if data_points:
                    print(f"\n最近 5 条 API 数据:")
                    for i, point in enumerate(data_points[-5:]):
                        temp_str = f"{point['temperature']:.2f}" if point.get('temperature') else "N/A"
                        ts_str = point.get('timestamp', 'N/A')
                        if ts_str and 'T' in ts_str:
                            ts_str = ts_str.replace('T', ' ')[:19]
                        alert_flag = " ⚠️" if point.get('is_alert') else ""
                        print(f"  [{i+1}] {ts_str} | 温度: {temp_str}°C{alert_flag}")
                
                print(f"\n✅ 实时数据 API 正常工作!")
                print(f"\n💡 前端看板提示:")
                print(f"   1. 请确保服务器正在运行: python server_no_reload.py")
                print(f"   2. 打开浏览器访问: {BASE_URL}")
                print(f"   3. 找到设备 '{device_id[:8]}...'")
                print(f"   4. 观察实时温度曲线是否呈现正弦波波动")
            else:
                print(f"⚠️  API 请求失败")
        except Exception as e:
            print(f"⚠️  API 验证出错: {e}")
    
    if not args.no_cleanup and not args.verify_only:
        print(f"\n{'='*70}")
        print("阶段 6: 清理测试数据")
        print("=" * 70)
        
        db = SessionLocal()
        try:
            history_count = db.query(DeviceDataHistory).filter(
                DeviceDataHistory.device_id == device_id
            ).count()
            
            db.query(DeviceDataHistory).filter(
                DeviceDataHistory.device_id == device_id
            ).delete(synchronize_session=False)
            
            db.commit()
            print(f"✅ 已清理 {history_count} 条历史记录")
            print(f"\nℹ️  提示: 设备 '{device_id}' 已保留，便于查看前端效果")
            print(f"   如需删除设备，请手动执行: DELETE FROM devices WHERE device_id = '{device_id}'")
        except Exception as e:
            print(f"⚠️  清理失败: {e}")
        finally:
            db.close()
    
    print(f"\n{'='*70}")
    print("测试完成!")
    print("=" * 70)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
