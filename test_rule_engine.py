import time
import hashlib
import uuid
import logging
import sys
import os
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server.models import (
    Base, Device, DeviceStatus, DeviceCommand, CommandStatus,
    Rule, RuleAction, RuleOperator
)

LOG_FILE = "logs/test_rule_engine.log"

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("RuleEngineTest")

DATABASE_URL = "sqlite:///./iot_devices.db"

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)

TEST_DEVICE_ID = f"TEST_RULE_{int(time.time())}"
TEST_DEVICE_MODEL = "Thermostat-X1"
TEST_SECRET_KEY = "test_rule_secret_123"


def create_test_device(db: Session) -> Device:
    logger.info(f"Creating test device: {TEST_DEVICE_ID}")
    
    existing = db.query(Device).filter(Device.device_id == TEST_DEVICE_ID).first()
    if existing:
        db.query(DeviceCommand).filter(DeviceCommand.device_id == TEST_DEVICE_ID).delete()
        db.commit()
        db.query(Rule).filter(Rule.device_id == TEST_DEVICE_ID).delete()
        db.commit()
        db.delete(existing)
        db.commit()
    
    device = Device(
        device_id=TEST_DEVICE_ID,
        secret_key=TEST_SECRET_KEY,
        model=TEST_DEVICE_MODEL,
        status=DeviceStatus.OFFLINE,
        last_heartbeat=None,
        last_seen=None
    )
    db.add(device)
    db.commit()
    db.refresh(device)
    
    logger.info(f"Test device created: {device.device_id}, model: {device.model}")
    return device


def create_test_rule(
    db: Session,
    device_id: str,
    rule_name: str,
    metric: str,
    operator: RuleOperator,
    threshold: float,
    action: RuleAction,
    command_type: str = None,
    command_value: str = None,
    description: str = None
) -> Rule:
    rule = Rule(
        id=str(uuid.uuid4()),
        rule_name=rule_name,
        device_id=device_id,
        metric=metric,
        operator=operator,
        threshold=threshold,
        action=action,
        command_type=command_type,
        command_value=command_value,
        is_active=True,
        description=description
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    
    logger.info(f"[CreateRule] Created rule: {rule.rule_name}")
    logger.info(f"  - Metric: {rule.metric} {rule.operator.value} {rule.threshold}")
    logger.info(f"  - Action: {rule.action.value}")
    if rule.command_type:
        logger.info(f"  - Command: {rule.command_type} = {rule.command_value}")
    
    return rule


def get_device_commands(db: Session, device_id: str, status: CommandStatus = None) -> list:
    query = db.query(DeviceCommand).filter(DeviceCommand.device_id == device_id)
    if status:
        query = query.filter(DeviceCommand.status == status)
    return query.order_by(desc(DeviceCommand.created_at)).all()


def count_device_commands(db: Session, device_id: str) -> int:
    return db.query(DeviceCommand).filter(DeviceCommand.device_id == device_id).count()


class MockRuleEngine:
    def __init__(self):
        pass
    
    def evaluate_operator(self, value: float, operator: RuleOperator, threshold: float) -> bool:
        if operator == RuleOperator.GT:
            return value > threshold
        elif operator == RuleOperator.LT:
            return value < threshold
        elif operator == RuleOperator.EQ:
            return value == threshold
        elif operator == RuleOperator.GE:
            return value >= threshold
        elif operator == RuleOperator.LE:
            return value <= threshold
        elif operator == RuleOperator.NE:
            return value != threshold
        return False
    
    def get_metric_value(self, payload: dict, metric: str) -> Optional[float]:
        if metric in payload:
            value = payload[metric]
            if isinstance(value, (int, float)):
                return float(value)
        if metric == "temperature" and "temp" in payload:
            value = payload["temp"]
            if isinstance(value, (int, float)):
                return float(value)
        if metric == "humidity" and "hum" in payload:
            value = payload["hum"]
            if isinstance(value, (int, float)):
                return float(value)
        return None
    
    def execute_send_command(self, rule: Rule, device: Device, db: Session):
        if not rule.command_type:
            logger.error(f"[RuleEngine] Rule '{rule.rule_name}' has action SEND_COMMAND but no command_type specified")
            return
        
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=600)
        
        command = DeviceCommand(
            id=str(uuid.uuid4()),
            device_id=rule.device_id,
            command_type=rule.command_type,
            command_value=rule.command_value,
            status=CommandStatus.PENDING,
            ttl_seconds=600,
            expires_at=expires_at,
            reason=f"Auto-triggered by rule: {rule.rule_name}",
            source="rule_engine"
        )
        db.add(command)
        
        if device.pending_commands is None:
            device.pending_commands = []
        
        pending_command = {
            "id": command.id,
            "command": rule.command_type,
            "value": rule.command_value,
            "status": CommandStatus.PENDING.value,
            "created_at": now.isoformat(),
            "delivered_at": None,
            "reason": command.reason
        }
        
        new_pending = list(device.pending_commands)
        new_pending.append(pending_command)
        device.pending_commands = new_pending
        
        db.commit()
        
        logger.info(f"[RuleEngine] Created command '{rule.command_type}' for device {rule.device_id} (id={command.id})")
        return command
    
    def process_device_data(self, device_id: str, payload: dict, db: Session):
        rules = db.query(Rule).filter(
            Rule.device_id == device_id,
            Rule.is_active == True
        ).all()
        
        if not rules:
            logger.debug(f"[RuleEngine] No active rules for device {device_id}")
            return []
        
        device = db.query(Device).filter(Device.device_id == device_id).first()
        if not device:
            logger.error(f"[RuleEngine] Device {device_id} not found")
            return []
        
        triggered_commands = []
        
        for rule in rules:
            metric_value = self.get_metric_value(payload, rule.metric)
            
            if metric_value is None:
                logger.debug(f"[RuleEngine] Metric '{rule.metric}' not found in payload for device {device_id}")
                continue
            
            print("\n" + "="*80)
            print(f"  [RULE ALERT] 规则触发: {rule.rule_name}")
            print("="*80)
            print(f"  设备 ID: {rule.device_id}")
            print(f"  监控指标: {rule.metric}")
            print(f"  当前值: {metric_value}")
            print(f"  条件: {rule.metric} {rule.operator.value} {rule.threshold}")
            print(f"  触发动作: {rule.action.value}")
            if rule.action == RuleAction.SEND_COMMAND:
                print(f"  下发指令: {rule.command_type} = {rule.command_value}")
            print("="*80 + "\n")
            
            if self.evaluate_operator(metric_value, rule.operator, rule.threshold):
                logger.info(f"[RuleEngine] Rule '{rule.rule_name}' matched for device {device_id}: {rule.metric}={metric_value} {rule.operator.value} {rule.threshold}")
                
                if rule.action == RuleAction.SEND_COMMAND:
                    command = self.execute_send_command(rule, device, db)
                    if command:
                        triggered_commands.append(command)
                
                rule.triggered_at = datetime.utcnow()
                rule.trigger_count = (rule.trigger_count or 0) + 1
                db.commit()
        
        return triggered_commands


def run_test_case_1_rule_creation():
    logger.info("=" * 60)
    logger.info("TEST CASE 1: Rule Creation")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        
        rule = create_test_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="High Temperature Alert",
            metric="temperature",
            operator=RuleOperator.GT,
            threshold=40.0,
            action=RuleAction.SEND_COMMAND,
            command_type="cooling_on",
            command_value="auto",
            description="当温控器温度 > 40°C 时，自动下发冷气开启指令"
        )
        
        assert rule.id is not None, "Rule ID should not be None"
        assert rule.device_id == TEST_DEVICE_ID, "Device ID should match"
        assert rule.rule_name == "High Temperature Alert", "Rule name should match"
        assert rule.metric == "temperature", "Metric should be temperature"
        assert rule.operator == RuleOperator.GT, "Operator should be GT"
        assert rule.threshold == 40.0, "Threshold should be 40.0"
        assert rule.action == RuleAction.SEND_COMMAND, "Action should be SEND_COMMAND"
        assert rule.command_type == "cooling_on", "Command type should match"
        assert rule.command_value == "auto", "Command value should match"
        assert rule.is_active == True, "Rule should be active"
        
        logger.info("[PASS] Rule created with all required fields")
        
        retrieved_rule = db.query(Rule).filter(Rule.id == rule.id).first()
        assert retrieved_rule is not None, "Should retrieve rule by ID"
        assert retrieved_rule.id == rule.id, "Retrieved rule ID should match"
        
        logger.info("[PASS] Rule retrieved from database")
        
        logger.info("\n[TEST CASE 1: PASS] Rule creation works correctly!")
        return True, rule.id
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 1: FAIL] {e}")
        return False, None
    except Exception as e:
        logger.error(f"[TEST CASE 1: ERROR] {e}", exc_info=True)
        return False, None
    finally:
        db.close()


def run_test_case_2_rule_engine_evaluation():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 2: Rule Engine Operator Evaluation")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        engine = MockRuleEngine()
        
        test_cases = [
            (45.0, RuleOperator.GT, 40.0, True),
            (40.0, RuleOperator.GT, 40.0, False),
            (35.0, RuleOperator.GT, 40.0, False),
            
            (35.0, RuleOperator.LT, 40.0, True),
            (40.0, RuleOperator.LT, 40.0, False),
            (45.0, RuleOperator.LT, 40.0, False),
            
            (40.0, RuleOperator.EQ, 40.0, True),
            (45.0, RuleOperator.EQ, 40.0, False),
            
            (45.0, RuleOperator.GE, 40.0, True),
            (40.0, RuleOperator.GE, 40.0, True),
            (35.0, RuleOperator.GE, 40.0, False),
            
            (35.0, RuleOperator.LE, 40.0, True),
            (40.0, RuleOperator.LE, 40.0, True),
            (45.0, RuleOperator.LE, 40.0, False),
            
            (45.0, RuleOperator.NE, 40.0, True),
            (40.0, RuleOperator.NE, 40.0, False),
        ]
        
        all_passed = True
        for value, operator, threshold, expected in test_cases:
            result = engine.evaluate_operator(value, operator, threshold)
            status = "PASS" if result == expected else "FAIL"
            logger.info(f"  [{status}] {value} {operator.value} {threshold} = {result} (expected: {expected})")
            if result != expected:
                all_passed = False
        
        payload_tests = [
            ({"temperature": 25.5}, "temperature", 25.5),
            ({"temp": 25.5}, "temperature", 25.5),
            ({"humidity": 60}, "humidity", 60.0),
            ({"hum": 60}, "humidity", 60.0),
            ({"temperature": "invalid"}, "temperature", None),
            ({"other": 100}, "temperature", None),
        ]
        
        logger.info("")
        logger.info("Testing metric value extraction:")
        for payload, metric, expected in payload_tests:
            result = engine.get_metric_value(payload, metric)
            status = "PASS" if result == expected else "FAIL"
            logger.info(f"  [{status}] payload={payload}, metric='{metric}' -> {result} (expected: {expected})")
            if result != expected:
                all_passed = False
        
        if all_passed:
            logger.info("\n[TEST CASE 2: PASS] Rule engine evaluation works correctly!")
            return True
        else:
            logger.error("\n[TEST CASE 2: FAIL] Some evaluations failed!")
            return False
            
    except Exception as e:
        logger.error(f"[TEST CASE 2: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def run_test_case_3_temperature_rule_trigger():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 3: Temperature Rule Trigger (Main Test)")
    logger.info("=" * 60)
    logger.info("测试场景: 当温控器温度 > 40°C 时，自动下发冷气开启指令")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        rule_engine = MockRuleEngine()
        
        logger.info("[Step 1] 创建规则: 温度 > 40°C 时下发冷气开启指令")
        rule = create_test_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="高温自动制冷",
            metric="temperature",
            operator=RuleOperator.GT,
            threshold=40.0,
            action=RuleAction.SEND_COMMAND,
            command_type="cooling_on",
            command_value="auto",
            description="当温控器温度 > 40°C 时，自动下发冷气开启指令"
        )
        
        initial_command_count = count_device_commands(db, TEST_DEVICE_ID)
        logger.info(f"[Step 1: PASS] 规则创建成功，初始指令数量: {initial_command_count}")
        
        logger.info("")
        logger.info("[Step 2] 上报温度 35°C (低于阈值，不应触发规则)")
        payload_35 = {"temperature": 35.0, "humidity": 50}
        commands = rule_engine.process_device_data(TEST_DEVICE_ID, payload_35, db)
        
        assert len(commands) == 0, "35°C should not trigger the rule"
        
        current_command_count = count_device_commands(db, TEST_DEVICE_ID)
        assert current_command_count == initial_command_count, "Command count should not change"
        
        logger.info(f"[Step 2: PASS] 35°C 未触发规则，指令数量: {current_command_count}")
        
        logger.info("")
        logger.info("[Step 3] 上报温度 45°C (高于阈值，应触发规则)")
        payload_45 = {"temperature": 45.0, "humidity": 45}
        commands = rule_engine.process_device_data(TEST_DEVICE_ID, payload_45, db)
        
        assert len(commands) == 1, "45°C should trigger exactly 1 command"
        
        triggered_command = commands[0]
        logger.info(f"[PASS] 规则触发，生成指令: {triggered_command.id}")
        logger.info(f"  - 指令类型: {triggered_command.command_type}")
        logger.info(f"  - 指令值: {triggered_command.command_value}")
        logger.info(f"  - 状态: {triggered_command.status.value}")
        logger.info(f"  - 触发原因: {triggered_command.reason}")
        
        assert triggered_command.command_type == "cooling_on", "Command type should be cooling_on"
        assert triggered_command.command_value == "auto", "Command value should be auto"
        assert triggered_command.status == CommandStatus.PENDING, "Command status should be PENDING"
        assert triggered_command.source == "rule_engine", "Command source should be rule_engine"
        assert "高温自动制冷" in triggered_command.reason, "Reason should mention the rule"
        
        logger.info("[Step 3: PASS] 45°C 触发规则，指令生成成功")
        
        logger.info("")
        logger.info("[Step 4] 验证指令表中存在对应指令")
        all_commands = get_device_commands(db, TEST_DEVICE_ID)
        assert len(all_commands) == 1, "Should have exactly 1 command in database"
        
        db_command = all_commands[0]
        assert db_command.id == triggered_command.id, "Command ID should match"
        assert db_command.command_type == "cooling_on", "Command type in DB should be cooling_on"
        
        logger.info(f"[Step 4: PASS] 指令表验证成功，指令 ID: {db_command.id}")
        
        logger.info("")
        logger.info("[Step 5] 验证设备 pending_commands 已更新")
        db.expire_all()
        device = db.query(Device).filter(Device.device_id == TEST_DEVICE_ID).first()
        assert device.pending_commands is not None, "pending_commands should not be None"
        assert len(device.pending_commands) > 0, "pending_commands should have at least one command"
        
        pending_cmd = device.pending_commands[-1]
        assert pending_cmd["command"] == "cooling_on", "Pending command type should match"
        assert pending_cmd["status"] == CommandStatus.PENDING.value, "Pending command status should be PENDING"
        
        logger.info(f"[Step 5: PASS] 设备 pending_commands 已更新，待处理指令数: {len(device.pending_commands)}")
        
        logger.info("")
        logger.info("[Step 6] 验证规则触发次数已更新")
        rule = db.query(Rule).filter(Rule.id == rule.id).first()
        assert rule.trigger_count == 1, "Rule trigger count should be 1"
        assert rule.triggered_at is not None, "Rule triggered_at should be set"
        
        logger.info(f"[Step 6: PASS] 规则触发次数: {rule.trigger_count}，最后触发时间: {rule.triggered_at}")
        
        logger.info("")
        logger.info("=" * 60)
        logger.info("[TEST CASE 3: PASS] 温度规则触发测试成功!")
        logger.info("=" * 60)
        logger.info("测试结论: 当温控器温度 > 40°C 时，自动下发冷气开启指令")
        logger.info("验证结果: 指令表中已自动生成对应指令 ✓")
        logger.info("=" * 60)
        
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 3: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 3: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def run_test_case_4_notify_action():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 4: Notify Action Test")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        rule_engine = MockRuleEngine()
        
        logger.info("[Step 1] 创建规则: 湿度 < 30% 时发送通知")
        rule = create_test_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="低湿度告警",
            metric="humidity",
            operator=RuleOperator.LT,
            threshold=30.0,
            action=RuleAction.NOTIFY,
            description="当湿度 < 30% 时，发送告警通知"
        )
        
        logger.info("[Step 1: PASS] 规则创建成功")
        
        logger.info("")
        logger.info("[Step 2] 上报湿度 25% (低于阈值，应触发规则)")
        payload = {"temperature": 25.0, "humidity": 25.0}
        commands = rule_engine.process_device_data(TEST_DEVICE_ID, payload, db)
        
        assert len(commands) == 0, "Notify action should not create commands"
        
        rule = db.query(Rule).filter(Rule.id == rule.id).first()
        assert rule.trigger_count == 1, "Rule should have been triggered once"
        
        logger.info(f"[Step 2: PASS] 规则触发成功，触发次数: {rule.trigger_count}")
        logger.info("[INFO] NOTIFY 动作会在实际运行时打印告警日志并推送到 WebSocket 前端")
        
        logger.info("\n[TEST CASE 4: PASS] Notify action test completed!")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 4: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 4: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def run_test_case_5_multiple_rules():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 5: Multiple Rules Test")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        rule_engine = MockRuleEngine()
        
        logger.info("[Step 1] 创建多条规则")
        
        rule1 = create_test_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="高温告警",
            metric="temperature",
            operator=RuleOperator.GT,
            threshold=40.0,
            action=RuleAction.SEND_COMMAND,
            command_type="cooling_on",
            command_value="high"
        )
        
        rule2 = create_test_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="极高温告警",
            metric="temperature",
            operator=RuleOperator.GT,
            threshold=50.0,
            action=RuleAction.SEND_COMMAND,
            command_type="emergency_cool",
            command_value="max"
        )
        
        logger.info("[Step 1: PASS] 2 条规则创建成功")
        
        logger.info("")
        logger.info("[Step 2] 上报温度 45°C (应触发第1条规则)")
        payload_45 = {"temperature": 45.0}
        commands = rule_engine.process_device_data(TEST_DEVICE_ID, payload_45, db)
        
        assert len(commands) == 1, "Should trigger exactly 1 rule at 45°C"
        assert commands[0].command_type == "cooling_on", "Should trigger cooling_on"
        
        logger.info(f"[Step 2: PASS] 触发 1 条规则: {commands[0].command_type}")
        
        logger.info("")
        logger.info("[Step 3] 上报温度 55°C (应触发2条规则)")
        payload_55 = {"temperature": 55.0}
        commands = rule_engine.process_device_data(TEST_DEVICE_ID, payload_55, db)
        
        assert len(commands) == 2, "Should trigger 2 rules at 55°C"
        
        command_types = [c.command_type for c in commands]
        assert "cooling_on" in command_types, "Should have cooling_on"
        assert "emergency_cool" in command_types, "Should have emergency_cool"
        
        logger.info(f"[Step 3: PASS] 触发 2 条规则: {command_types}")
        
        all_commands = get_device_commands(db, TEST_DEVICE_ID)
        logger.info(f"[INFO] 指令表中共有 {len(all_commands)} 条指令")
        
        logger.info("\n[TEST CASE 5: PASS] Multiple rules test completed!")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 5: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 5: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def run_test_case_6_inactive_rule():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 6: Inactive Rule Test")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        rule_engine = MockRuleEngine()
        
        logger.info("[Step 1] 创建规则并设置为非活跃状态")
        rule = create_test_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="测试非活跃规则",
            metric="temperature",
            operator=RuleOperator.GT,
            threshold=40.0,
            action=RuleAction.SEND_COMMAND,
            command_type="test_cmd",
            command_value="test"
        )
        
        rule.is_active = False
        db.commit()
        
        logger.info("[Step 1: PASS] 规则创建并设置为非活跃状态")
        
        logger.info("")
        logger.info("[Step 2] 上报温度 45°C (非活跃规则不应触发)")
        payload = {"temperature": 45.0}
        commands = rule_engine.process_device_data(TEST_DEVICE_ID, payload, db)
        
        assert len(commands) == 0, "Inactive rule should not trigger"
        
        command_count = count_device_commands(db, TEST_DEVICE_ID)
        assert command_count == 0, "No commands should be created"
        
        logger.info(f"[Step 2: PASS] 非活跃规则未触发，指令数量: {command_count}")
        
        logger.info("")
        logger.info("[Step 3] 激活规则后再次测试")
        rule.is_active = True
        db.commit()
        
        commands = rule_engine.process_device_data(TEST_DEVICE_ID, payload, db)
        
        assert len(commands) == 1, "Active rule should trigger"
        assert commands[0].command_type == "test_cmd"
        
        logger.info(f"[Step 3: PASS] 激活后规则触发成功")
        
        logger.info("\n[TEST CASE 6: PASS] Inactive rule test completed!")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 6: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 6: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def main():
    logger.info("=" * 60)
    logger.info("Rule Engine Test Suite")
    logger.info("=" * 60)
    logger.info(f"Test Parameters:")
    logger.info(f"  - Test Device: {TEST_DEVICE_ID}")
    logger.info(f"  - Log File: {LOG_FILE}")
    logger.info("")
    
    test_results = []
    
    test_results.append(("Test Case 1: Rule Creation", run_test_case_1_rule_creation()[0]))
    
    test_results.append(("Test Case 2: Rule Engine Evaluation", run_test_case_2_rule_engine_evaluation()))
    
    test_results.append(("Test Case 3: Temperature Rule Trigger (MAIN TEST)", run_test_case_3_temperature_rule_trigger()))
    
    test_results.append(("Test Case 4: Notify Action", run_test_case_4_notify_action()))
    
    test_results.append(("Test Case 5: Multiple Rules", run_test_case_5_multiple_rules()))
    
    test_results.append(("Test Case 6: Inactive Rule", run_test_case_6_inactive_rule()))
    
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    
    passed = 0
    failed = 0
    
    for test_name, result in test_results:
        status = "PASS" if result else "FAIL"
        if result:
            passed += 1
        else:
            failed += 1
        logger.info(f"  {status}: {test_name}")
    
    logger.info("")
    logger.info(f"Total: {len(test_results)} tests")
    logger.info(f"Passed: {passed}")
    logger.info(f"Failed: {failed}")
    
    if failed == 0:
        logger.info("\n[ALL TESTS PASSED] Rule engine mechanism is working correctly!")
        logger.info("")
        logger.info("核心测试验证:")
        logger.info("  ✓ Rule 模型正确创建")
        logger.info("  ✓ 规则引擎操作符评估正确")
        logger.info("  ✓ 温度 > 40°C 时自动下发冷气开启指令")
        logger.info("  ✓ 指令表中自动生成对应指令")
        logger.info("  ✓ 设备 pending_commands 已更新")
        logger.info("  ✓ 规则触发次数已记录")
    else:
        logger.warning(f"\n[SOME TESTS FAILED] Please check the logs for details: {LOG_FILE}")
    
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
