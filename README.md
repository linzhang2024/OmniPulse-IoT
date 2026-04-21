# OmniPulse-IoT

轻量级的工业/家居物联网设备管理平台，支持海量异构设备的状态监控与远程控制。

## ✨ 功能特性

### 📊 设备监控
- **实时状态追踪**：设备心跳机制，30秒无响应自动标记离线
- **数据上报**：统一 RESTful API 接收传感器数据
- **监控大屏**：Web 实时展示设备状态和传感器数据
- **自动告警**：温度超过 50°C 自动触发告警指令

### 🎮 远程控制
- **指令下发**：`POST /devices/control/{device_id}` 接口
- **异步响应**：指令存入队列，设备下次心跳时下发
- **Ack 确认机制**：设备可上报已执行指令，确保可靠执行
- **TTL 有效期**：指令 10 分钟自动过期，避免无效下发

### 🔧 指令生命周期

```
创建指令 → [PENDING] → 心跳下发 → [DELIVERED] → 设备确认 → [执行成功]
                    ↓
                超过 10 分钟
                    ↓
                [EXPIRED] (保留记录但不再下发)
```

## 🛠️ 技术栈

| 组件 | 技术 |
|------|------|
| 后端框架 | FastAPI |
| 数据库 | SQLite + SQLAlchemy |
| 定时任务 | APScheduler |
| 前端 | 原生 HTML + JavaScript |
| 数据格式 | JSON |

## 📦 安装部署

### 环境要求
- Python 3.8+
- pip

### 安装依赖
```bash
pip install -r requirements.txt
```

### 启动服务
```bash
python server.py
```

服务默认运行在 `http://localhost:8000`

### 访问地址
- 监控大屏：`http://localhost:8000`
- API 文档：`http://localhost:8000/docs`

## 🔐 设备认证机制

为了保障 IoT 平台的安全性，设备发送请求时需要进行签名认证。

### 认证流程

1. **设备注册**：设备首次注册时，服务器会返回一个 `secret_key`
2. **请求签名**：设备发送心跳或数据上报时，需要在 Header 中携带签名
3. **签名校验**：服务器校验签名和时间戳，确保请求的合法性

### 签名计算规则

```
签名 = MD5(device_id + timestamp + secret_key)
```

- `device_id`: 设备唯一标识
- `timestamp`: 当前 Unix 时间戳（秒）
- `secret_key`: 设备注册时获取的密钥

### 请求 Header 要求

| Header | 说明 |
|--------|------|
| `X-Signature` | 签名值（MD5 结果，小写） |
| `X-Timestamp` | Unix 时间戳（秒） |

### 安全机制

- **签名校验**：确保请求来自持有正确密钥的设备
- **时间戳校验**：时间戳与服务器时间差超过 60 秒的请求将被拒绝（防重放攻击）

### Python 签名示例

```python
import hashlib
import time

def compute_signature(device_id: str, secret_key: str) -> tuple[str, int]:
    timestamp = int(time.time())
    raw_string = f"{device_id}{timestamp}{secret_key}"
    signature = hashlib.md5(raw_string.encode('utf-8')).hexdigest().lower()
    return signature, timestamp

# 使用示例
device_id = "sensor_001"
secret_key = "abc123xyz789"
signature, timestamp = compute_signature(device_id, secret_key)

# 请求 Header
headers = {
    "Content-Type": "application/json",
    "X-Signature": signature,
    "X-Timestamp": str(timestamp)
}
```

## 📡 API 接口

### 设备管理

#### 注册设备
```http
POST /devices/register
Content-Type: application/json

{
  "device_id": "sensor_001",
  "model": "TemperatureSensor_v2"
}
```

**响应示例**：
```json
{
  "message": "Device registered successfully",
  "device_id": "sensor_001",
  "model": "TemperatureSensor_v2",
  "status": "offline",
  "secret_key": "aB3kLmN9pQrStUvWxYz1234567890Ab"
}
```

> **重要**：请妥善保存返回的 `secret_key`，后续所有请求都需要使用此密钥计算签名。

#### 发送心跳（支持 Ack 确认）
```http
POST /devices/heartbeat/{device_id}
Content-Type: application/json
X-Signature: 5f4dcc3b5aa765d61d8327deb882cf99
X-Timestamp: 1713685800

{
  "executed_commands": ["cmd-uuid-1", "cmd-uuid-2"]
}
```

**响应示例**：
```json
{
  "device_id": "sensor_001",
  "status": "online",
  "last_heartbeat": "2026-04-21T10:30:00",
  "pending_commands": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "command": "toggle_switch",
      "value": "on",
      "created_at": "2026-04-21T10:29:00"
    }
  ],
  "acknowledged_count": 2,
  "expired_count": 0
}
```

#### 上报数据
```http
POST /devices/data
Content-Type: application/json
X-Signature: 5f4dcc3b5aa765d61d8327deb882cf99
X-Timestamp: 1713685800

{
  "device_id": "sensor_001",
  "payload": {
    "temperature": 25.5,
    "humidity": 60,
    "battery": 85
  }
}
```

### 控制接口

#### 下发控制指令
```http
POST /devices/control/{device_id}
Content-Type: application/json

{
  "command": "toggle_switch",
  "value": "on"
}
```

**响应示例**：
```json
{
  "message": "Command queued successfully",
  "device_id": "device_001",
  "command_id": "550e8400-e29b-41d4-a716-446655440000",
  "command": "toggle_switch",
  "value": "on",
  "status": "pending",
  "queued_at": "2026-04-21T10:30:00",
  "ttl_seconds": 600
}
```

#### 获取设备列表
```http
GET /devices
```

#### 获取单个设备信息
```http
GET /devices/{device_id}
```

## 📋 指令状态说明

| 状态 | 值 | 说明 |
|------|-----|------|
| PENDING | `pending` | 指令已创建，等待下发 |
| DELIVERED | `delivered` | 指令已下发给设备，等待确认 |
| SUCCESS | `success` | 设备已确认执行 |
| EXPIRED | `expired` | 超过 10 分钟未下发，已过期 |

## 🎯 设备端集成示例

### Python 设备端示例
```python
import requests
import time
import hashlib

SERVER_URL = "http://localhost:8000"
DEVICE_ID = "my_device_001"
SECRET_KEY = "your_secret_key_here"  # 从注册响应中获取

executed_commands = []

def compute_signature(device_id: str, secret_key: str) -> dict:
    timestamp = int(time.time())
    raw_string = f"{device_id}{timestamp}{secret_key}"
    signature = hashlib.md5(raw_string.encode('utf-8')).hexdigest().lower()
    return {
        "X-Signature": signature,
        "X-Timestamp": str(timestamp),
        "Content-Type": "application/json"
    }

def register_device():
    global SECRET_KEY
    try:
        response = requests.post(
            f"{SERVER_URL}/devices/register",
            json={
                "device_id": DEVICE_ID,
                "model": "TemperatureSensor_v1"
            },
            headers={"Content-Type": "application/json"}
        )
        data = response.json()
        SECRET_KEY = data.get("secret_key")
        print(f"设备注册成功，Secret Key: {SECRET_KEY}")
        return SECRET_KEY
    except Exception as e:
        print(f"设备注册失败: {e}")
        return None

def send_heartbeat():
    global executed_commands
    if not SECRET_KEY:
        print("请先注册设备")
        return
    
    try:
        headers = compute_signature(DEVICE_ID, SECRET_KEY)
        response = requests.post(
            f"{SERVER_URL}/devices/heartbeat/{DEVICE_ID}",
            json={"executed_commands": executed_commands},
            headers=headers
        )
        
        if response.status_code != 200:
            print(f"心跳发送失败: {response.text}")
            return
        
        data = response.json()
        
        # 处理待执行指令
        for cmd in data["pending_commands"]:
            cmd_id = cmd["id"]
            command = cmd["command"]
            value = cmd["value"]
            
            print(f"执行指令: {command} = {value}")
            
            # 模拟执行指令
            execute_device_command(command, value)
            
            # 标记为已执行，下次心跳确认
            executed_commands.append(cmd_id)
        
        # 清空已确认的列表（服务器已收到）
        if data["acknowledged_count"] > 0:
            executed_commands = []
            
    except Exception as e:
        print(f"心跳发送失败: {e}")

def report_data():
    if not SECRET_KEY:
        print("请先注册设备")
        return
    
    try:
        headers = compute_signature(DEVICE_ID, SECRET_KEY)
        response = requests.post(
            f"{SERVER_URL}/devices/data",
            json={
                "device_id": DEVICE_ID,
                "payload": {
                    "temperature": 25.5,
                    "humidity": 60
                }
            },
            headers=headers
        )
        
        if response.status_code != 200:
            print(f"数据上报失败: {response.text}")
            
    except Exception as e:
        print(f"数据上报失败: {e}")

def execute_device_command(command: str, value: str):
    print(f"执行设备指令: {command} = {value}")

# 初始化：先注册设备
if not SECRET_KEY:
    register_device()

# 主循环
while True:
    send_heartbeat()
    report_data()
    time.sleep(5)  # 每 5 秒上报一次
```

## 📁 项目结构

```
OmniPulse-IoT/
├── server/
│   ├── __init__.py
│   ├── main.py           # FastAPI 主应用，包含所有 API 接口
│   └── models.py         # 数据库模型定义
├── templates/
│   └── index.html        # 监控大屏前端页面
├── logs/                 # 日志目录
├── iot_devices.db        # SQLite 数据库
├── requirements.txt      # Python 依赖
├── server.py             # 启动入口
├── migrate_add_pending_commands.py  # 数据库迁移脚本
├── migrate_commands_v2.py           # 指令格式迁移脚本
└── migrate_add_secret_key.py        # 设备认证密钥迁移脚本
```

## 🔧 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| HEARTBEAT_TIMEOUT | 30 秒 | 心跳超时时间 |
| CHECK_INTERVAL | 10 秒 | 设备状态检查间隔 |
| COMMAND_TTL_SECONDS | 600 秒 (10 分钟) | 指令有效期 |
| TEMPERATURE_THRESHOLD | 50°C | 温度告警阈值 |
| ALERT_CONSECUTIVE_THRESHOLD | 3 次 | 连续超标次数阈值（告警平滑引擎） |
| SIGNATURE_TIMESTAMP_TOLERANCE | 60 秒 | 签名时间戳容差（防重放攻击） |

## 🚨 告警机制

### 自动温度告警（告警平滑引擎）

为了防止传感器抖动造成的误报，平台采用了**告警平滑引擎**：

- **触发条件**：连续 3 次数据上报温度超过阈值（默认 50°C）
- **自动指令**：`alert_buzzer = on`
- **去重逻辑**：同一设备不会重复添加待处理的告警指令
- **复位逻辑**：温度恢复正常后，连续超标计数自动清零

### 工作流程

```
第1次超温 → 计数=1 → 不触发告警
第2次超温 → 计数=2 → 不触发告警
第3次超温 → 计数=3 → 触发 alert_buzzer 指令
温度恢复 → 计数=0 → 状态复位
```

### 日志示例

**连续超温但未触发（前2次）：**
```
[Alert] Device sensor_001: Temperature 52.0°C exceeds threshold, consecutive count: 1/3
[Alert] Device sensor_001: Temperature 53.5°C exceeds threshold, consecutive count: 2/3
```

**连续3次超温触发告警：**
```
[Alert] Device sensor_001: Temperature 55.5°C exceeds threshold, consecutive count: 3/3
[Alert] Device sensor_001: Triggering alert_buzzer after 3 consecutive alerts (id=xxx)
[Command] Command xxx delivered to device
[Command] Command xxx acknowledged by device
```

**温度恢复正常：**
```
[Alert] Device sensor_001: Temperature 45.0°C normalized, resetting consecutive alert count
```

## 📝 数据库迁移

首次部署或升级时需要执行迁移脚本：

```bash
# 1. 添加 pending_commands 字段（首次部署）
python migrate_add_pending_commands.py

# 2. 转换旧格式指令（从 v1 升级到 v2）
python migrate_commands_v2.py

# 3. 添加设备认证 secret_key 字段（新增设备认证功能）
python migrate_add_secret_key.py

# 4. 添加连续告警计数字段（新增告警平滑引擎）
python migrate_add_alert_count.py
```

## 🤝 贡献指南

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建 Pull Request


---

**OmniPulse-IoT** - 让物联网设备管理更简单、更可靠。
