import asyncio
import time
import hashlib
import uuid
import threading
import gc
import os
import sys
from datetime import datetime, timedelta
from collections import deque
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import statistics

try:
    import httpx
    import websockets
except ImportError:
    print("Error: Required packages not installed.")
    print("Please run: pip install httpx websockets")
    sys.exit(1)

BASE_URL = os.getenv("STRESS_BASE_URL", "http://127.0.0.1:8000")
WS_BASE_URL = BASE_URL.replace("http://", "ws://")

NUM_DEVICES = int(os.getenv("STRESS_NUM_DEVICES", "100"))
NUM_WS_CLIENTS = int(os.getenv("STRESS_NUM_WS", "50"))
COMMANDS_PER_SECOND = int(os.getenv("STRESS_COMMANDS_PER_SEC", "20"))
STRESS_DURATION_MINUTES = int(os.getenv("STRESS_DURATION_MINUTES", "5"))
DATA_REPORT_INTERVAL = float(os.getenv("STRESS_DATA_INTERVAL", "1.0"))

REPORT_INTERVAL_SECONDS = 5

@dataclass
class StatsCollector:
    lock: threading.Lock = field(default_factory=threading.Lock)
    
    data_report_latencies: deque = field(default_factory=lambda: deque(maxlen=10000))
    data_report_errors: int = 0
    data_report_success: int = 0
    
    command_send_latencies: deque = field(default_factory=lambda: deque(maxlen=10000))
    command_send_errors: int = 0
    command_send_success: int = 0
    
    ws_messages_received: int = 0
    ws_errors: int = 0
    ws_connected: int = 0
    
    start_time: Optional[float] = None
    last_report_time: Optional[float] = None
    
    last_data_success: int = 0
    last_command_success: int = 0
    last_ws_messages: int = 0
    
    sqlite_lock_contention: int = 0
    sqlite_lock_wait_times: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    memory_samples: deque = field(default_factory=lambda: deque(maxlen=100))
    
    def start(self):
        self.start_time = time.time()
        self.last_report_time = self.start_time
        self._sample_memory()
    
    def _sample_memory(self):
        try:
            import psutil
            process = psutil.Process()
            mem_info = process.memory_info()
            self.memory_samples.append({
                "timestamp": time.time(),
                "rss_mb": mem_info.rss / 1024 / 1024,
                "vms_mb": mem_info.vms / 1024 / 1024
            })
        except ImportError:
            pass
    
    def record_data_report(self, latency: float, success: bool):
        with self.lock:
            if success:
                self.data_report_latencies.append(latency)
                self.data_report_success += 1
            else:
                self.data_report_errors += 1
    
    def record_command_send(self, latency: float, success: bool):
        with self.lock:
            if success:
                self.command_send_latencies.append(latency)
                self.command_send_success += 1
            else:
                self.command_send_errors += 1
    
    def record_ws_message(self):
        with self.lock:
            self.ws_messages_received += 1
    
    def record_ws_error(self):
        with self.lock:
            self.ws_errors += 1
    
    def record_ws_connect(self):
        with self.lock:
            self.ws_connected += 1
    
    def record_sqlite_lock_wait(self, wait_time: float):
        with self.lock:
            self.sqlite_lock_contention += 1
            self.sqlite_lock_wait_times.append(wait_time)
    
    def get_current_stats(self) -> Dict[str, Any]:
        with self.lock:
            elapsed = time.time() - (self.start_time or time.time())
            
            data_total = self.data_report_success + self.data_report_errors
            command_total = self.command_send_success + self.command_send_errors
            
            data_latency_avg = 0.0
            data_latency_p95 = 0.0
            if self.data_report_latencies:
                data_latency_avg = statistics.mean(self.data_report_latencies)
                sorted_lat = sorted(self.data_report_latencies)
                p95_idx = int(len(sorted_lat) * 0.95)
                data_latency_p95 = sorted_lat[p95_idx] if sorted_lat else 0
            
            command_latency_avg = 0.0
            command_latency_p95 = 0.0
            if self.command_send_latencies:
                command_latency_avg = statistics.mean(self.command_send_latencies)
                sorted_lat = sorted(self.command_send_latencies)
                p95_idx = int(len(sorted_lat) * 0.95)
                command_latency_p95 = sorted_lat[p95_idx] if sorted_lat else 0
            
            sqlite_lock_avg = 0.0
            if self.sqlite_lock_wait_times:
                sqlite_lock_avg = statistics.mean(self.sqlite_lock_wait_times)
            
            report_elapsed = time.time() - (self.last_report_time or time.time())
            
            data_tps = 0
            if report_elapsed > 0:
                data_tps = (self.data_report_success - self.last_data_success) / report_elapsed
            command_tps = 0
            if report_elapsed > 0:
                command_tps = (self.command_send_success - self.last_command_success) / report_elapsed
            
            ws_msg_rate = 0
            if report_elapsed > 0:
                ws_msg_rate = (self.ws_messages_received - self.last_ws_messages) / report_elapsed
            
            self._sample_memory()
            
            return {
                "elapsed_seconds": elapsed,
                "data_report": {
                    "total": data_total,
                    "success": self.data_report_success,
                    "errors": self.data_report_errors,
                    "latency_avg_ms": data_latency_avg * 1000,
                    "latency_p95_ms": data_latency_p95 * 1000,
                    "tps_current": data_tps,
                    "tps_avg": self.data_report_success / elapsed if elapsed > 0 else 0
                },
                "commands": {
                    "total": command_total,
                    "success": self.command_send_success,
                    "errors": self.command_send_errors,
                    "latency_avg_ms": command_latency_avg * 1000,
                    "latency_p95_ms": command_latency_p95 * 1000,
                    "tps_current": command_tps,
                    "tps_avg": self.command_send_success / elapsed if elapsed > 0 else 0
                },
                "websocket": {
                    "connected": self.ws_connected,
                    "messages_total": self.ws_messages_received,
                    "errors": self.ws_errors,
                    "msg_rate_current": ws_msg_rate
                },
                "sqlite": {
                    "lock_contention_count": self.sqlite_lock_contention,
                    "avg_lock_wait_ms": sqlite_lock_avg * 1000
                },
                "memory": {
                    "current_rss_mb": self.memory_samples[-1]["rss_mb"] if self.memory_samples else None,
                    "samples_count": len(self.memory_samples)
                }
            }
    
    def update_last_counts(self):
        with self.lock:
            self.last_data_success = self.data_report_success
            self.last_command_success = self.command_send_success
            self.last_ws_messages = self.ws_messages_received
            self.last_report_time = time.time()

stats = StatsCollector()

def compute_signature(device_id: str, timestamp: str, secret_key: str) -> str:
    raw_string = f"{device_id}{timestamp}{secret_key}"
    return hashlib.md5(raw_string.encode('utf-8')).hexdigest().lower()

async def register_device(client: httpx.AsyncClient, device_id: str, model: str) -> Dict[str, Any]:
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
    payload: Dict[str, Any]
) -> tuple[bool, float]:
    start_time = time.time()
    timestamp = str(int(time.time()))
    signature = compute_signature(device_id, timestamp, secret_key)
    
    try:
        response = await client.post(
            f"{BASE_URL}/devices/data",
            json={"device_id": device_id, "payload": payload},
            headers={
                "X-Signature": signature,
                "X-Timestamp": timestamp
            },
            timeout=5.0
        )
        latency = time.time() - start_time
        success = response.status_code == 200
        stats.record_data_report(latency, success)
        return success, latency
    except Exception as e:
        latency = time.time() - start_time
        stats.record_data_report(latency, False)
        return False, latency

async def send_control_command(
    client: httpx.AsyncClient,
    device_id: str,
    command: str,
    value: str
) -> tuple[bool, float]:
    start_time = time.time()
    try:
        response = await client.post(
            f"{BASE_URL}/devices/control/{device_id}",
            json={"command": command, "value": value},
            timeout=5.0
        )
        latency = time.time() - start_time
        success = response.status_code == 200
        stats.record_command_send(latency, success)
        return success, latency
    except Exception as e:
        latency = time.time() - start_time
        stats.record_command_send(latency, False)
        return False, latency

async def websocket_listener(ws_url: str, stop_event: asyncio.Event):
    try:
        async with websockets.connect(ws_url) as ws:
            stats.record_ws_connect()
            
            while not stop_event.is_set():
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    stats.record_ws_message()
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    stats.record_ws_error()
                    break
    except Exception as e:
        stats.record_ws_error()

async def device_worker(
    device_id: str,
    secret_key: str,
    stop_event: asyncio.Event,
    client: httpx.AsyncClient
):
    step = 0
    base_temp = 25.0
    amplitude = 15.0
    frequency = 0.1
    
    while not stop_event.is_set():
        temp = base_temp + amplitude * __import__('math').sin(2 * __import__('math').pi * frequency * step)
        temp = round(temp, 2)
        humidity = round(40 + (hash(str(step)) % 1000) / 1000.0 * 20, 1)
        
        payload = {
            "temperature": temp,
            "humidity": humidity,
            "pressure": 1013 + (hash(str(step)) % 100) / 10.0,
            "battery": 80 + (hash(str(step)) % 20)
        }
        
        await report_device_data(client, device_id, secret_key, payload)
        step += 1
        
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=DATA_REPORT_INTERVAL)
        except asyncio.TimeoutError:
            pass

async def command_generator(
    device_ids: List[str],
    stop_event: asyncio.Event,
    client: httpx.AsyncClient
):
    commands = [
        ("set_temperature", "25"),
        ("set_humidity", "50"),
        ("alert_buzzer", "on"),
        ("alert_buzzer", "off"),
        ("reboot", "now"),
        ("set_mode", "auto"),
        ("set_mode", "manual"),
    ]
    
    interval = 1.0 / COMMANDS_PER_SECOND if COMMANDS_PER_SECOND > 0 else 1.0
    cmd_index = 0
    device_index = 0
    
    while not stop_event.is_set():
        cmd, value = commands[cmd_index % len(commands)]
        device_id = device_ids[device_index % len(device_ids)]
        
        await send_control_command(client, device_id, cmd, value)
        
        cmd_index += 1
        device_index += 1
        
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass

def print_report_header():
    print("\n" + "=" * 120)
    print(f"{'时间戳':<12} {'类型':<12} {'总数':<8} {'成功':<8} {'错误':<8} {'平均延迟(ms)':<12} {'P95(ms)':<10} {'当前TPS':<10} {'平均TPS':<10}")
    print("-" * 120)

def print_stats_report():
    current_stats = stats.get_current_stats()
    
    elapsed = current_stats["elapsed_seconds"]
    elapsed_str = str(timedelta(seconds=int(elapsed)))
    timestamp = datetime.now().strftime("%H:%M:%S")
    
    print(f"\n[{timestamp}] 运行时间: {elapsed_str}")
    print_report_header()
    
    data = current_stats["data_report"]
    print(f"{timestamp:<12} {'数据上报':<12} {data['total']:<8} {data['success']:<8} {data['errors']:<8} "
          f"{data['latency_avg_ms']:<12.2f} {data['latency_p95_ms']:<10.2f} {data['tps_current']:<10.2f} {data['tps_avg']:<10.2f}")
    
    cmd = current_stats["commands"]
    print(f"{timestamp:<12} {'控制指令':<12} {cmd['total']:<8} {cmd['success']:<8} {cmd['errors']:<8} "
          f"{cmd['latency_avg_ms']:<12.2f} {cmd['latency_p95_ms']:<10.2f} {cmd['tps_current']:<10.2f} {cmd['tps_avg']:<10.2f}")
    
    ws = current_stats["websocket"]
    sqlite = current_stats["sqlite"]
    mem = current_stats["memory"]
    
    print("\n" + "-" * 120)
    print(f"WebSocket: 已连接 {ws['connected']} 个客户端 | 接收消息: {ws['messages_total']} | 消息速率: {ws['msg_rate_current']:.2f}/s")
    print(f"SQLite: 锁竞争次数: {sqlite['lock_contention_count']} | 平均锁等待: {sqlite['avg_lock_wait_ms']:.2f}ms")
    if mem['current_rss_mb']:
        print(f"内存: 当前 RSS: {mem['current_rss_mb']:.2f} MB | 采样次数: {mem['samples_count']}")
    
    stats.update_last_counts()

async def reporter(stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=REPORT_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass
        
        if not stop_event.is_set():
            print_stats_report()

async def run_stress_test():
    print("=" * 120)
    print("IoT 平台全链路压力测试")
    print("=" * 120)
    print(f"\n配置参数:")
    print(f"  - 服务器地址: {BASE_URL}")
    print(f"  - 设备数量: {NUM_DEVICES}")
    print(f"  - WebSocket 客户端数量: {NUM_WS_CLIENTS}")
    print(f"  - 控制指令速率: {COMMANDS_PER_SECOND}/秒")
    print(f"  - 数据上报间隔: {DATA_REPORT_INTERVAL}秒")
    print(f"  - 测试时长: {STRESS_DURATION_MINUTES} 分钟")
    print(f"  - 统计报告间隔: {REPORT_INTERVAL_SECONDS} 秒")
    print("=" * 120)
    
    print("\n[阶段 1] 注册测试设备...")
    device_info = {}
    
    async with httpx.AsyncClient() as client:
        for i in range(NUM_DEVICES):
            device_id = f"stress_dev_{uuid.uuid4().hex[:8]}"
            model = f"Stress-Sensor-{i % 10 + 1}"
            
            try:
                result = await register_device(client, device_id, model)
                device_info[device_id] = {
                    "secret_key": result["secret_key"],
                    "model": model
                }
                if (i + 1) % 20 == 0:
                    print(f"  已注册 {i + 1}/{NUM_DEVICES} 设备")
            except Exception as e:
                print(f"  [错误] 注册设备 {device_id} 失败: {e}")
    
    print(f"\n[OK] 成功注册 {len(device_info)} 个设备")
    device_ids = list(device_info.keys())
    
    if not device_ids:
        print("[错误] 没有成功注册的设备，无法继续测试")
        return
    
    print("\n[阶段 2] 启动测试组件...")
    stop_event = asyncio.Event()
    stats.start()
    
    tasks = []
    
    print("  启动设备数据上报任务...")
    async with httpx.AsyncClient() as http_client:
        for device_id in device_ids:
            secret_key = device_info[device_id]["secret_key"]
            task = asyncio.create_task(
                device_worker(device_id, secret_key, stop_event, http_client)
            )
            tasks.append(task)
        
        print("  启动 WebSocket 监听客户端...")
        ws_tasks = []
        for i in range(min(NUM_WS_CLIENTS, len(device_ids))):
            device_id = device_ids[i % len(device_ids)]
            ws_url = f"{WS_BASE_URL}/ws/frontend"
            ws_task = asyncio.create_task(
                websocket_listener(ws_url, stop_event)
            )
            ws_tasks.append(ws_task)
            tasks.append(ws_task)
        
        print("  启动控制指令生成器...")
        command_task = asyncio.create_task(
            command_generator(device_ids, stop_event, http_client)
        )
        tasks.append(command_task)
        
        print("  启动统计报告器...")
        reporter_task = asyncio.create_task(reporter(stop_event))
        tasks.append(reporter_task)
        
        print(f"\n[阶段 3] 压力测试进行中 (持续 {STRESS_DURATION_MINUTES} 分钟)...")
        print("-" * 120)
        
        stress_duration_seconds = STRESS_DURATION_MINUTES * 60
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=stress_duration_seconds)
        except asyncio.TimeoutError:
            pass
        
        print("\n" + "=" * 120)
        print("[阶段 4] 停止测试并等待任务完成...")
        stop_event.set()
        
        await asyncio.sleep(2)
        
        for task in tasks:
            if not task.done():
                task.cancel()
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except:
            pass
        
        print("\n[阶段 5] 最终统计报告")
        print("=" * 120)
        print_stats_report()
        
        print("\n[阶段 6] 内存泄漏检查...")
        print("-" * 120)
        
        gc.collect()
        
        try:
            import psutil
            process = psutil.Process()
            mem_info = process.memory_info()
            
            print(f"\n当前内存使用:")
            print(f"  - RSS (物理内存): {mem_info.rss / 1024 / 1024:.2f} MB")
            print(f"  - VMS (虚拟内存): {mem_info.vms / 1024 / 1024:.2f} MB")
            
            if len(stats.memory_samples) > 1:
                first_mem = stats.memory_samples[0]
                last_mem = stats.memory_samples[-1]
                mem_growth = last_mem["rss_mb"] - first_mem["rss_mb"]
                
                print(f"\n内存增长分析:")
                print(f"  - 初始内存: {first_mem['rss_mb']:.2f} MB")
                print(f"  - 最终内存: {last_mem['rss_mb']:.2f} MB")
                print(f"  - 内存增长: {mem_growth:.2f} MB")
                
                duration_hours = (last_mem["timestamp"] - first_mem["timestamp"]) / 3600
                if duration_hours > 0:
                    growth_rate = mem_growth / duration_hours
                    print(f"  - 增长率: {growth_rate:.2f} MB/小时")
                    
                    if growth_rate > 10:
                        print("  ⚠️ 警告: 内存增长率较高，可能存在内存泄漏")
                    elif growth_rate > 0:
                        print("  ℹ️ 信息: 内存有轻微增长，建议监控")
                    else:
                        print("  ✓ 正常: 内存没有明显增长")
        except ImportError:
            print("\n[提示] psutil 未安装，无法进行详细内存分析")
            print("请运行: pip install psutil")
        
        print("\n[阶段 7] 文件句柄检查...")
        print("-" * 120)
        
        try:
            import psutil
            process = psutil.Process()
            
            if hasattr(process, 'num_handles'):
                num_handles = process.num_handles()
                print(f"\nWindows 句柄数量: {num_handles}")
            else:
                num_fds = process.num_fds()
                print(f"\n文件描述符数量: {num_fds}")
        except ImportError:
            print("\n[提示] psutil 未安装，无法进行文件句柄分析")
        
        print("\n" + "=" * 120)
        print("压力测试完成!")
        print("=" * 120)
        
        final_stats = stats.get_current_stats()
        total_errors = final_stats["data_report"]["errors"] + final_stats["commands"]["errors"] + final_stats["websocket"]["errors"]
        
        if total_errors == 0:
            print("\n✓ 测试通过: 所有操作均成功完成")
        else:
            print(f"\n⚠️ 测试完成，但存在错误:")
            print(f"   数据上报错误: {final_stats['data_report']['errors']}")
            print(f"   控制指令错误: {final_stats['commands']['errors']}")
            print(f"   WebSocket 错误: {final_stats['websocket']['errors']}")
        
        print("\n[提示] 建议检查:")
        print("  1. 服务器端日志查看是否有异常")
        print("  2. 数据库文件大小 (iot_devices.db)")
        print("  3. 服务器进程内存使用情况")
        print("  4. 如发现内存泄漏，可考虑:")
        print("     - 检查数据库连接是否正确关闭")
        print("     - 检查 WebSocket 连接是否正确释放")
        print("     - 检查定时任务是否有资源泄漏")

def main():
    try:
        asyncio.run(run_stress_test())
    except KeyboardInterrupt:
        print("\n\n[提示] 用户中断测试")
    except Exception as e:
        print(f"\n[错误] 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
