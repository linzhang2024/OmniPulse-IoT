import asyncio
import hashlib
import time
import uuid
import httpx
import websockets

BASE_URL = "http://127.0.0.1:8000"
WS_BASE_URL = "ws://127.0.0.1:8000"

TEST_DEVICE_ID = f"test_device_{uuid.uuid4().hex[:8]}"
TEST_DEVICE_MODEL = "TestModel-001"

device_secret_key = None

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

async def test_device_register():
    print("\n" + "="*60)
    print("TEST 1: 设备注册 (Device Registration)")
    print("="*60)
    
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        response = await client.post(
            "/devices/register",
            json={
                "device_id": TEST_DEVICE_ID,
                "model": TEST_DEVICE_MODEL
            }
        )
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        
        assert response.status_code == 200, f"注册失败: {response.text}"
        
        data = response.json()
        assert data["device_id"] == TEST_DEVICE_ID
        assert "secret_key" in data
        
        global device_secret_key
        device_secret_key = data["secret_key"]
        
        print(f"✅ 设备注册成功!")
        print(f"   Device ID: {TEST_DEVICE_ID}")
        print(f"   Secret Key: {device_secret_key[:8]}...")
        return True

async def test_device_data_report():
    print("\n" + "="*60)
    print("TEST 2: 数据上报与查询 (Data Report & Query)")
    print("="*60)
    
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        test_payload = {
            "temperature": 25.5,
            "humidity": 60.0,
            "status": "normal"
        }
        
        headers = get_auth_headers(TEST_DEVICE_ID, device_secret_key)
        
        print(f"\n[步骤 1] 上报数据...")
        response = await client.post(
            "/devices/data",
            json={
                "device_id": TEST_DEVICE_ID,
                "payload": test_payload
            },
            headers=headers
        )
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        
        assert response.status_code == 200, f"数据上报失败: {response.text}"
        
        data = response.json()
        assert data["message"] == "Data saved successfully"
        assert "data_id" in data
        assert "history_id" in data
        
        data_id = data["data_id"]
        print(f"✅ 数据上报成功! Data ID: {data_id}")
        
        print(f"\n[步骤 2] 查询设备信息...")
        response = await client.get(f"/devices/{TEST_DEVICE_ID}")
        
        print(f"Status Code: {response.status_code}")
        
        assert response.status_code == 200, f"设备查询失败: {response.text}"
        
        device_info = response.json()
        print(f"Device Info: {device_info}")
        
        assert device_info["device_id"] == TEST_DEVICE_ID
        assert device_info["status"] == "online"
        
        latest_payload = device_info.get("latest_payload")
        if latest_payload:
            assert latest_payload.get("temperature") == 25.5
            print(f"✅ 数据验证成功! 温度: {latest_payload.get('temperature')}")
        
        print(f"✅ 设备状态: {device_info['status']}")
        return True

async def test_websocket_connection():
    print("\n" + "="*60)
    print("TEST 3: WebSocket 连接 (WebSocket Connection)")
    print("="*60)
    
    ws_device_id = f"ws_test_{uuid.uuid4().hex[:8]}"
    
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        response = await client.post(
            "/devices/register",
            json={
                "device_id": ws_device_id,
                "model": "WS-TestModel"
            }
        )
        ws_secret_key = response.json()["secret_key"]
        print(f"WebSocket 测试设备已注册: {ws_device_id}")
    
    print(f"\n[步骤 1] 连接 WebSocket...")
    
    try:
        async with websockets.connect(f"{WS_BASE_URL}/ws/devices/{ws_device_id}") as websocket:
            print(f"✅ WebSocket 连接成功!")
            
            print(f"\n[步骤 2] 发送 ping 消息...")
            ping_message = {
                "type": "ping"
            }
            await websocket.send(ping_message.__str__().replace("'", '"'))
            
            response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
            print(f"收到响应: {response}")
            
            import json
            response_data = json.loads(response)
            assert response_data.get("type") == "pong"
            print(f"✅ Ping-Pong 测试成功!")
            
            print(f"\n[步骤 3] 测试前端 WebSocket 连接...")
            async with websockets.connect(f"{WS_BASE_URL}/ws/frontend") as frontend_ws:
                print(f"✅ 前端 WebSocket 连接成功!")
                
                await frontend_ws.send('{"type": "ping"}')
                frontend_response = await asyncio.wait_for(frontend_ws.recv(), timeout=5.0)
                frontend_data = json.loads(frontend_response)
                assert frontend_data.get("type") == "pong"
                print(f"✅ 前端 Ping-Pong 测试成功!")
            
            return True
            
    except Exception as e:
        print(f"❌ WebSocket 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_device_list():
    print("\n" + "="*60)
    print("TEST 4: 设备列表查询 (Device List Query)")
    print("="*60)
    
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        response = await client.get("/devices")
        
        print(f"Status Code: {response.status_code}")
        
        assert response.status_code == 200, f"设备列表查询失败: {response.text}"
        
        devices = response.json()
        print(f"设备数量: {len(devices)}")
        
        test_device_found = False
        for device in devices:
            if device["device_id"] == TEST_DEVICE_ID:
                test_device_found = True
                print(f"✅ 测试设备在列表中: {device}")
                break
        
        assert test_device_found, "测试设备未在列表中找到"
        return True

async def main():
    print("\n" + "="*60)
    print("IoT 平台健康度验证测试")
    print("IoT Platform Sanity Check")
    print("="*60)
    print(f"测试时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"目标服务器: {BASE_URL}")
    
    tests = [
        ("设备注册", test_device_register),
        ("数据上报与查询", test_device_data_report),
        ("WebSocket 连接", test_websocket_connection),
        ("设备列表查询", test_device_list),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            success = await test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"❌ 测试失败: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))
    
    print("\n" + "="*60)
    print("测试结果汇总")
    print("="*60)
    
    passed = 0
    failed = 0
    
    for test_name, success in results:
        status = "✅ 通过" if success else "❌ 失败"
        print(f"  {test_name}: {status}")
        if success:
            passed += 1
        else:
            failed += 1
    
    print(f"\n总计: {len(results)} 个测试")
    print(f"通过: {passed} 个")
    print(f"失败: {failed} 个")
    
    if failed == 0:
        print("\n🎉 所有测试通过! 系统运行正常!")
        return True
    else:
        print(f"\n⚠️ 有 {failed} 个测试失败，请检查系统状态")
        return False

if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n测试被用户中断")
        exit(1)
    except Exception as e:
        print(f"\n测试执行错误: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
