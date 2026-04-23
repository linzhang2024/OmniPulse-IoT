import time
import hashlib
import uuid
import httpx
import logging
import sys
import os
from datetime import datetime
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server.models import Base, Device, DeviceDataHistory, DeviceProtocol, ProtocolType

LOG_FILE = "protocol_test.log"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ProtocolTest")

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
    logger.info(f"[Register] 注册设备: device_id={device_id}, model={model}")
    response = client.post(
        f"{BASE_URL}/devices/register",
        json={"device_id": device_id, "model": model},
        timeout=10.0
    )
    response.raise_for_status()
    result = response.json()
    logger.info(f"[Register] 注册成功: secret_key={result['secret_key'][:8]}...")
    return result

def report_raw_data(
    client: httpx.Client,
    device_id: str,
    secret_key: str,
    raw_payload: str = None,
    payload: dict = None
) -> dict:
    timestamp = str(int(time.time()))
    signature = compute_signature(device_id, timestamp, secret_key)
    
    request_data = {"device_id": device_id}
    if raw_payload is not None:
        request_data["raw_payload"] = raw_payload
        logger.debug(f"[Report] 发送 raw_payload: {raw_payload}")
    if payload is not None:
        request_data["payload"] = payload
        logger.debug(f"[Report] 发送 payload: {payload}")
    
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
        logger.error(f"[Report] 请求失败: {response.status_code} - {response.text}")
    else:
        logger.info(f"[Report] 请求成功: status_code={response.status_code}")
    
    response.raise_for_status()
    return response.json()

def create_hex_protocol_4bytes(db, device_id: str) -> DeviceProtocol:
    """
    创建 4 字节 HEX 协议配置
    格式: [温度(2字节)][湿度(2字节)]
    例如: 01A2 00FA -> 温度0x01A2=418, 湿度0x00FA=250
    """
    logger.info(f"[Protocol] 创建 4 字节 HEX 协议配置")
    logger.info(f"[Protocol] 格式: offset 0-1: 温度(uint), offset 2-3: 湿度(uint)")
    
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
        description="旧工业设备 4 字节 HEX 协议: [温度(2B)][湿度(2B)]"
    )
    db.add(protocol)
    db.commit()
    db.refresh(protocol)
    logger.info(f"[Protocol] 协议配置创建成功: id={protocol.id[:8]}...")
    return protocol

def create_hex_protocol_8bytes(db, device_id: str) -> DeviceProtocol:
    """
    创建 8 字节 HEX 协议配置（多字段测试）
    格式: [设备状态(1B)][温度(2B)][湿度(2B)][电压(2B)][保留(1B)]
    """
    logger.info(f"[Protocol] 创建 8 字节多字段 HEX 协议配置")
    
    protocol = DeviceProtocol(
        id=str(uuid.uuid4()),
        device_id=device_id,
        protocol_type=ProtocolType.HEX,
        is_active=True,
        parse_config={
            "byte_order": "big",
            "fields": [
                {"name": "status", "offset": 0, "length": 1, "type": "uint"},
                {"name": "temp_raw", "offset": 1, "length": 2, "type": "uint"},
                {"name": "hum_raw", "offset": 3, "length": 2, "type": "uint"},
                {"name": "voltage_raw", "offset": 5, "length": 2, "type": "uint"}
            ]
        },
        field_mappings={
            "temp_raw": "temperature",
            "hum_raw": "humidity"
        },
        transform_formulas={
            "temperature": {
                "formula": "val * 0.1 - 15.3",
                "input_range": [0, 1023]
            },
            "humidity": {
                "formula": "val * 0.0976",
                "input_range": [0, 1023]
            },
            "voltage_raw": {
                "formula": "val * 0.01",
                "input_range": [0, 500]
            }
        },
        description="多字段测试协议: [状态(1B)][温度(2B)][湿度(2B)][电压(2B)][保留(1B)]"
    )
    db.add(protocol)
    db.commit()
    db.refresh(protocol)
    return protocol

def get_history_records(db, device_id: str, limit: int = 10) -> list:
    records = db.query(DeviceDataHistory).filter(
        DeviceDataHistory.device_id == device_id
    ).order_by(desc(DeviceDataHistory.timestamp)).limit(limit).all()
    return records

def get_latest_history(db, device_id: str) -> DeviceDataHistory:
    return db.query(DeviceDataHistory).filter(
        DeviceDataHistory.device_id == device_id
    ).order_by(desc(DeviceDataHistory.timestamp)).first()

def generate_test_hex_data(index: int) -> tuple:
    """
    生成测试用的 HEX 数据
    返回: (hex_string, expected_temp, expected_humidity, raw_temp, raw_hum)
    """
    base_temp = 200 + (index % 300)
    base_hum = 100 + (index * 7 % 400)
    
    hex_temp = f"{base_temp:04X}"
    hex_hum = f"{base_hum:04X}"
    
    expected_temp = round(base_temp * 0.1 - 15.3, 2)
    expected_hum = round(base_hum * 0.0976, 2)
    
    return (hex_temp + hex_hum, expected_temp, expected_humidity, base_temp, base_hum)

def test_basic_hex_parsing():
    """
    测试1: 基础 HEX 解析测试（4字节，2字段）
    """
    logger.info("\n" + "=" * 70)
    logger.info("测试1: 基础 HEX 解析测试（4字节，2字段）")
    logger.info("=" * 70)
    
    device_id = f"test_hex_basic_{uuid.uuid4().hex[:8]}"
    model = "OldSensor-BASIC"
    
    with httpx.Client() as client:
        register_result = register_device(client, device_id, model)
        secret_key = register_result["secret_key"]
    
    db = SessionLocal()
    try:
        create_hex_protocol_4bytes(db, device_id)
        
        test_cases = [
            {
                "raw_payload": "01A200FA",
                "description": "0x01A2=418, 0x00FA=250",
                "calc_temp": lambda: 418 * 0.1 - 15.3,
                "calc_hum": lambda: 250 * 0.0976
            },
            {
                "raw_payload": "020001F4",
                "description": "0x0200=512, 0x01F4=500",
                "calc_temp": lambda: 512 * 0.1 - 15.3,
                "calc_hum": lambda: 500 * 0.0976
            },
            {
                "raw_payload": "00FF00C8",
                "description": "0x00FF=255, 0x00C8=200",
                "calc_temp": lambda: 255 * 0.1 - 15.3,
                "calc_hum": lambda: 200 * 0.0976
            },
            {
                "raw_payload": "03 FF 02 BC",
                "description": "带空格的格式: 0x03FF=1023, 0x02BC=700",
                "calc_temp": lambda: 1023 * 0.1 - 15.3,
                "calc_hum": lambda: 700 * 0.0976
            }
        ]
        
        all_passed = True
        
        for i, test_case in enumerate(test_cases, 1):
            logger.info(f"\n--- 测试用例 {i}: {test_case['raw_payload']} ---")
            logger.info(f"    描述: {test_case['description']}")
            
            expected_temp = test_case['calc_temp']()
            expected_hum = test_case['calc_hum']()
            
            logger.info(f"    计算期望值: temperature={expected_temp:.2f}°C, humidity={expected_hum:.2f}%")
            
            try:
                with httpx.Client() as client:
                    result = report_raw_data(
                        client, device_id, secret_key,
                        raw_payload=test_case['raw_payload']
                    )
                    logger.info(f"    API 响应成功")
                    
                    if "protocol_info" in result:
                        logger.info(f"    协议类型: {result['protocol_info']['protocol_type']}")
                    
                    time.sleep(0.1)
                
                record = get_latest_history(db, device_id)
                
                if record:
                    logger.info(f"    数据库记录:")
                    logger.info(f"        temperature: {record.temperature}")
                    logger.info(f"        humidity: {record.humidity}")
                    logger.info(f"        is_alert: {record.is_alert}")
                    logger.info(f"        timestamp: {record.timestamp}")
                    
                    temp_ok = record.temperature is not None and abs(record.temperature - expected_temp) < 0.5
                    hum_ok = record.humidity is not None and abs(record.humidity - expected_hum) < 0.5
                    
                    if temp_ok:
                        logger.info(f"    [PASSED] 温度解析正确")
                    else:
                        logger.error(f"    [FAILED] 温度解析错误: 期望 {expected_temp:.2f}，实际 {record.temperature}")
                        all_passed = False
                    
                    if hum_ok:
                        logger.info(f"    [PASSED] 湿度解析正确")
                    else:
                        logger.error(f"    [FAILED] 湿度解析错误: 期望 {expected_hum:.2f}，实际 {record.humidity}")
                        all_passed = False
                else:
                    logger.error(f"    [FAILED] 未找到历史记录")
                    all_passed = False
                    
            except Exception as e:
                logger.error(f"    [FAILED] 测试用例执行失败: {str(e)}")
                all_passed = False
        
        return all_passed
        
    finally:
        db.close()

def test_error_handling():
    """
    测试2: 错误处理和容错能力测试
    """
    logger.info("\n" + "=" * 70)
    logger.info("测试2: 错误处理和容错能力测试")
    logger.info("=" * 70)
    
    device_id = f"test_hex_error_{uuid.uuid4().hex[:8]}"
    model = "ErrorTestSensor"
    
    with httpx.Client() as client:
        register_result = register_device(client, device_id, model)
        secret_key = register_result["secret_key"]
    
    db = SessionLocal()
    try:
        create_hex_protocol_4bytes(db, device_id)
        
        error_cases = [
            {
                "raw_payload": "G1A200FA",
                "description": "包含非法字符 'G'",
                "expected_error_contains": ["非法字符", "Invalid", "invalid"]
            },
            {
                "raw_payload": "01A200F",
                "description": "奇数长度",
                "expected_error_contains": ["奇数", "odd length", "odd"]
            },
            {
                "raw_payload": "01A2",
                "description": "数据长度不足（只有2字节，协议需要4字节）",
                "expected_error_contains": ["长度不足", "offset", "offset"]
            },
            {
                "raw_payload": "",
                "description": "空字符串",
                "expected_error_contains": ["空", "empty", "Empty"]
            }
        ]
        
        all_handled = True
        
        for i, test_case in enumerate(error_cases, 1):
            logger.info(f"\n--- 错误测试 {i}: {test_case['raw_payload'] or '(empty)'} ---")
            logger.info(f"    描述: {test_case['description']}")
            
            try:
                with httpx.Client() as client:
                    result = report_raw_data(
                        client, device_id, secret_key,
                        raw_payload=test_case['raw_payload']
                    )
                    logger.error(f"    [FAILED] 期望返回 400 错误，但请求成功了")
                    all_handled = False
                    
            except httpx.HTTPStatusError as e:
                logger.info(f"    正确返回错误状态码: {e.response.status_code}")
                logger.info(f"    错误信息: {e.response.text}")
                
                if e.response.status_code == 400:
                    error_text = e.response.text.lower()
                    found_keyword = any(
                        keyword.lower() in error_text 
                        for keyword in test_case['expected_error_contains']
                    )
                    
                    if found_keyword:
                        logger.info(f"    [PASSED] 错误信息包含期望的关键字")
                    else:
                        logger.warning(f"    [WARNING] 错误信息可能不够明确")
                else:
                    logger.error(f"    [FAILED] 期望状态码 400，但实际是 {e.response.status_code}")
                    all_handled = False
                    
            except Exception as e:
                logger.error(f"    [FAILED] 发生未预期的异常: {str(e)}")
                all_handled = False
        
        return all_handled
        
    finally:
        db.close()

def test_multi_field_parsing():
    """
    测试3: 多字段解析测试（8字节，4字段）
    """
    logger.info("\n" + "=" * 70)
    logger.info("测试3: 多字段解析测试（8字节，4字段）")
    logger.info("=" * 70)
    
    device_id = f"test_hex_multi_{uuid.uuid4().hex[:8]}"
    model = "MultiFieldSensor"
    
    with httpx.Client() as client:
        register_result = register_device(client, device_id, model)
        secret_key = register_result["secret_key"]
    
    db = SessionLocal()
    try:
        create_hex_protocol_8bytes(db, device_id)
        
        logger.info(f"\n测试数据格式: [状态(1B)][温度(2B)][湿度(2B)][电压(2B)][保留(1B)]")
        
        test_cases = [
            {
                "raw_payload": "0101A200FA0064FF",
                "description": "状态=1, 温度=0x01A2=418, 湿度=0x00FA=250, 电压=0x0064=100",
                "fields": {
                    "status": 1,
                    "temp_raw": 418,
                    "hum_raw": 250,
                    "voltage_raw": 100
                }
            },
            {
                "raw_payload": "02020001F4012C00",
                "description": "状态=2, 温度=0x0200=512, 湿度=0x01F4=500, 电压=0x012C=300",
                "fields": {
                    "status": 2,
                    "temp_raw": 512,
                    "hum_raw": 500,
                    "voltage_raw": 300
                }
            }
        ]
        
        all_passed = True
        
        for i, test_case in enumerate(test_cases, 1):
            logger.info(f"\n--- 多字段测试 {i}: {test_case['raw_payload']} ---")
            logger.info(f"    描述: {test_case['description']}")
            
            try:
                with httpx.Client() as client:
                    result = report_raw_data(
                        client, device_id, secret_key,
                        raw_payload=test_case['raw_payload']
                    )
                    time.sleep(0.1)
                
                record = get_latest_history(db, device_id)
                
                if record and record.payload:
                    logger.info(f"    完整 payload: {record.payload}")
                    
                    raw_parsed = record.payload.get('_raw_parsed', {})
                    if raw_parsed:
                        parsed_data = raw_parsed.get('parsed_data', {})
                        logger.info(f"    解析出的原始值: {parsed_data}")
                        
                        for field_name, expected_value in test_case['fields'].items():
                            if field_name in parsed_data:
                                actual_value = parsed_data[field_name]
                                if actual_value == expected_value:
                                    logger.info(f"        {field_name}: {actual_value} [PASSED]")
                                else:
                                    logger.error(f"        {field_name}: {actual_value} [FAILED] 期望 {expected_value}")
                                    all_passed = False
                            else:
                                logger.warning(f"        {field_name}: 未找到该字段")
                else:
                    logger.error(f"    [FAILED] 未找到记录或 payload 为空")
                    all_passed = False
                    
            except Exception as e:
                logger.error(f"    [FAILED] 测试用例执行失败: {str(e)}")
                all_passed = False
        
        return all_passed
        
    finally:
        db.close()

def test_performance_100_records():
    """
    测试4: 性能测试 - 100条数据连续上报
    """
    logger.info("\n" + "=" * 70)
    logger.info("测试4: 性能测试 - 100条数据连续上报")
    logger.info("=" * 70)
    
    device_id = f"test_perf_100_{uuid.uuid4().hex[:8]}"
    model = "PerfTestSensor"
    
    with httpx.Client() as client:
        register_result = register_device(client, device_id, model)
        secret_key = register_result["secret_key"]
    
    db = SessionLocal()
    try:
        create_hex_protocol_4bytes(db, device_id)
        
        total_records = 100
        success_count = 0
        failed_count = 0
        
        logger.info(f"\n开始上报 {total_records} 条测试数据...")
        
        start_time = time.time()
        
        for i in range(total_records):
            hex_data, expected_temp, expected_hum, raw_temp, raw_hum = generate_test_hex_data(i)
            
            try:
                with httpx.Client() as client:
                    result = report_raw_data(
                        client, device_id, secret_key,
                        raw_payload=hex_data
                    )
                    success_count += 1
                    
                    if (i + 1) % 10 == 0:
                        logger.info(f"    已上报 {i + 1}/{total_records} 条数据")
                        
            except Exception as e:
                failed_count += 1
                logger.error(f"    第 {i + 1} 条上报失败: {str(e)}")
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        logger.info(f"\n--- 性能测试结果 ---")
        logger.info(f"    总耗时: {elapsed_time:.2f} 秒")
        logger.info(f"    成功: {success_count} 条")
        logger.info(f"    失败: {failed_count} 条")
        logger.info(f"    平均速度: {total_records / elapsed_time:.2f} 条/秒")
        logger.info(f"    单条平均耗时: {elapsed_time * 1000 / total_records:.2f} 毫秒")
        
        time.sleep(0.5)
        records = get_history_records(db, device_id, limit=100)
        actual_count = len(records)
        
        logger.info(f"\n--- 数据库验证 ---")
        logger.info(f"    数据库中实际记录数: {actual_count}")
        
        if actual_count == total_records:
            logger.info(f"    [PASSED] 所有记录都已正确写入数据库")
        else:
            logger.error(f"    [FAILED] 记录数量不匹配: 期望 {total_records}，实际 {actual_count}")
        
        if records:
            logger.info(f"\n--- 抽样验证 ---")
            sample_indices = [0, 49, 99]
            for idx in sample_indices:
                if idx < len(records):
                    record = records[len(records) - 1 - idx]
                    logger.info(f"\n    记录 {idx + 1}:")
                    logger.info(f"        temperature: {record.temperature}")
                    logger.info(f"        humidity: {record.humidity}")
                    logger.info(f"        timestamp: {record.timestamp}")
        
        return failed_count == 0 and actual_count == total_records
        
    finally:
        db.close()

def test_json_compatibility():
    """
    测试5: 传统 JSON 格式兼容性测试
    """
    logger.info("\n" + "=" * 70)
    logger.info("测试5: 传统 JSON 格式兼容性测试")
    logger.info("=" * 70)
    
    device_id = f"test_json_compat_{uuid.uuid4().hex[:8]}"
    model = "JsonCompatSensor"
    
    with httpx.Client() as client:
        register_result = register_device(client, device_id, model)
        secret_key = register_result["secret_key"]
        
        test_payloads = [
            {"temperature": 25.5, "humidity": 60.0},
            {"temperature": 28.3, "humidity": 55.5},
            {"temp": 30.0, "humidity": 70.0}
        ]
        
        all_passed = True
        
        for i, payload in enumerate(test_payloads, 1):
            logger.info(f"\n--- JSON 测试 {i}: {payload} ---")
            
            try:
                result = report_raw_data(
                    client, device_id, secret_key,
                    raw_payload=None, payload=payload
                )
                logger.info(f"    API 响应成功")
                
                time.sleep(0.1)
                
                db = SessionLocal()
                try:
                    record = get_latest_history(db, device_id)
                    
                    if record:
                        expected_temp = payload.get('temperature') or payload.get('temp')
                        expected_hum = payload.get('humidity') or payload.get('humidity')
                        
                        logger.info(f"    数据库记录:")
                        logger.info(f"        temperature: {record.temperature}")
                        logger.info(f"        humidity: {record.humidity}")
                        
                        if record.temperature == expected_temp:
                            logger.info(f"    [PASSED] 温度正确")
                        else:
                            logger.error(f"    [FAILED] 温度错误: 期望 {expected_temp}，实际 {record.temperature}")
                            all_passed = False
                    else:
                        logger.error(f"    [FAILED] 未找到记录")
                        all_passed = False
                finally:
                    db.close()
                    
            except Exception as e:
                logger.error(f"    [FAILED] 测试失败: {str(e)}")
                all_passed = False
        
        return all_passed

def main():
    logger.info("\n" + "#" * 70)
    logger.info("# 协议适配器深度重构测试 - Protocol Adapter Deep Refactor Test")
    logger.info(f"# 测试开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"# 日志文件: {LOG_FILE}")
    logger.info("#" * 70)
    
    results = {}
    
    logger.info("\n" + "*" * 70)
    logger.info("* 开始执行测试套件")
    logger.info("*" * 70)
    
    try:
        results["test_basic_hex"] = test_basic_hex_parsing()
    except Exception as e:
        logger.error(f"测试1执行失败: {str(e)}")
        results["test_basic_hex"] = False
    
    try:
        results["test_error_handling"] = test_error_handling()
    except Exception as e:
        logger.error(f"测试2执行失败: {str(e)}")
        results["test_error_handling"] = False
    
    try:
        results["test_multi_field"] = test_multi_field_parsing()
    except Exception as e:
        logger.error(f"测试3执行失败: {str(e)}")
        results["test_multi_field"] = False
    
    try:
        results["test_performance"] = test_performance_100_records()
    except Exception as e:
        logger.error(f"测试4执行失败: {str(e)}")
        results["test_performance"] = False
    
    try:
        results["test_json_compat"] = test_json_compatibility()
    except Exception as e:
        logger.error(f"测试5执行失败: {str(e)}")
        results["test_json_compat"] = False
    
    logger.info("\n" + "=" * 70)
    logger.info("测试结果汇总")
    logger.info("=" * 70)
    
    test_names = {
        "test_basic_hex": "测试1: 基础 HEX 解析",
        "test_error_handling": "测试2: 错误处理和容错",
        "test_multi_field": "测试3: 多字段解析",
        "test_performance": "测试4: 性能测试(100条)",
        "test_json_compat": "测试5: JSON 格式兼容"
    }
    
    all_passed = True
    for key, name in test_names.items():
        result = results.get(key, False)
        status = "[PASSED]" if result else "[FAILED]"
        logger.info(f"  {name}: {status}")
        if not result:
            all_passed = False
    
    logger.info("\n" + "=" * 70)
    if all_passed:
        logger.info("所有测试通过! 协议适配器功能正常工作。")
    else:
        logger.info("部分测试失败! 请查看日志文件了解详情。")
    logger.info("=" * 70)
    
    logger.info(f"\n测试结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"详细日志已保存到: {os.path.abspath(LOG_FILE)}")
    
    print(f"\n{'='*70}")
    print("按 Enter 键退出...")
    print("="*70)
    input('Press Enter to exit...')
    
    return all_passed

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
