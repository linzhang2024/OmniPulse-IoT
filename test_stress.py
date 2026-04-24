import asyncio
import hashlib
import time
import uuid
import aiohttp
import random
import json

BASE_URL = "http://127.0.0.1:8000"

NUM_DEVICES = 10
REQUESTS_PER_DEVICE = 50
CONCURRENT_LIMIT = 10

device_info = {}

def compute_signature(device_id: str, timestamp: str, secret_key: str) -> str:
    raw_string = f"{device_id}{timestamp}{secret_key}"
    return hashlib.md5(raw_string.encode('utf-8')).hexdigest().lower()

def get_auth_headers(device_id: str, secret_key: str) -> dict:
    timestamp = str(int(time.time()))
    signature = compute_signature(device_id, timestamp, secret_key)
    return {
        "X-Signature": signature,
        "X-Timestamp": timestamp
    }

def generate_payload():
    return {
        "temperature": round(20.0 + random.random() * 20.0, 1),
        "humidity": round(40.0 + random.random() * 40.0, 1),
        "status": "normal",
        "battery": random.randint(70, 100),
        "signal": random.randint(-80, -40)
    }

async def register_device(session: aiohttp.ClientSession, device_id: str):
    response = await session.post(
        f"{BASE_URL}/devices/register",
        json={
            "device_id": device_id,
            "model": "StressTest-001"
        }
    )
    
    data = await response.json()
    assert response.status == 200, f"注册失败: {data}"
    return data["secret_key"]

async def send_data_once(session: aiohttp.ClientSession, device_id: str, secret_key: str):
    headers = get_auth_headers(device_id, secret_key)
    payload = generate_payload()
    
    response = await session.post(
        f"{BASE_URL}/devices/data",
        json={
            "device_id": device_id,
            "payload": payload
        },
        headers=headers
    )
    
    data = await response.json()
    if response.status != 200:
        print(f"[ERROR] {device_id}: {response.status} - {data}")
        return False, data
    
    return True, data

async def worker(session: aiohttp.ClientSession, device_id: str, secret_key: str, num_requests: int, results: list, start_time: float):
    success_count = 0
    fail_count = 0
    
    for i in range(num_requests):
        success, result = await send_data_once(session, device_id, secret_key)
        if success:
            success_count += 1
        else:
            fail_count += 1
    
    results.append({
        "device_id": device_id,
        "success": success_count,
        "fail": fail_count,
        "total": num_requests
    })

async def main():
    print("="*60)
    print("IoT 平台压力测试 (Stress Test)")
    print("="*60)
    print(f"测试配置:")
    print(f"  - 设备数量: {NUM_DEVICES}")
    print(f"  - 每设备请求数: {REQUESTS_PER_DEVICE}")
    print(f"  - 并发限制: {CONCURRENT_LIMIT}")
    print(f"  - 总请求数: {NUM_DEVICES * REQUESTS_PER_DEVICE}")
    print(f"目标 TPS: >= 100")
    print("="*60)
    
    print("\n[步骤 1] 注册测试设备...")
    
    async with aiohttp.ClientSession() as session:
        register_tasks = []
        for i in range(NUM_DEVICES):
            device_id = f"stress_test_{i:03d}_{uuid.uuid4().hex[:4]}"
            register_tasks.append(register_device(session, device_id))
            device_info[device_id] = None
        
        register_results = await asyncio.gather(*register_tasks)
        
        for i, (device_id, _) in enumerate(device_info.items()):
            device_info[device_id] = register_results[i]
            print(f"  - {device_id} 已注册")
        
        print(f"\n✅ 所有设备注册完成!")
        print("\n[步骤 2] 预热测试...")
        
        warmup_start = time.time()
        await send_data_once(session, list(device_info.keys())[0], list(device_info.values())[0])
        warmup_end = time.time()
        print(f"预热完成，耗时: {(warmup_end - warmup_start)*1000:.1f}ms")
        
        print("\n[步骤 3] 开始压力测试...")
        
        results = []
        start_time = time.time()
        
        tasks = []
        for device_id, secret_key in device_info.items():
            task = worker(session, device_id, secret_key, REQUESTS_PER_DEVICE, results, start_time)
            tasks.append(task)
        
        await asyncio.gather(*tasks)
        
        end_time = time.time()
        
        total_success = sum(r["success"] for r in results)
        total_fail = sum(r["fail"] for r in results)
        total_requests = total_success + total_fail
        duration = end_time - start_time
        tps = total_requests / duration if duration > 0 else 0
        
        print("\n" + "="*60)
        print("测试结果汇总")
        print("="*60)
        
        print(f"\n总请求数: {total_requests}")
        print(f"成功: {total_success}")
        print(f"失败: {total_fail}")
        print(f"耗时: {duration:.2f} 秒")
        print(f"平均 TPS: {tps:.1f}")
        
        print("\n各设备统计:")
        for r in results:
            success_rate = r["success"] / r["total"] * 100 if r["total"] > 0 else 0
            print(f"  {r['device_id']}: 成功={r['success']}, 失败={r['fail']}, 成功率={success_rate:.1f}%")
        
        print("\n" + "="*60)
        if tps >= 100:
            print(f"🎉 目标达成! TPS = {tps:.1f} >= 100")
        else:
            print(f"⚠️ 目标未达成! TPS = {tps:.1f} < 100")
        
        print("="*60)
        
        return tps >= 100, tps

if __name__ == "__main__":
    try:
        success, tps = asyncio.run(main())
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n测试被用户中断")
        exit(1)
    except Exception as e:
        print(f"\n测试执行错误: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
