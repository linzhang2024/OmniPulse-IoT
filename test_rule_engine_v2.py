import time
import uuid
import logging
import sys
import os
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from collections import deque
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server.models import (
    Base, Device, DeviceStatus, DeviceCommand, CommandStatus,
    Rule, RuleAction, RuleOperator, ConditionType
)

LOG_FILE = "logs/test_rule_engine_v2.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("RuleEngineV2Test")

DATABASE_URL = "sqlite:///./iot_devices_test_v2.db"

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)

TEST_DEVICE_ID = f"TEST_V2_{int(time.time())}"
TEST_DEVICE_MODEL = "MultiSensor-X1"
TEST_SECRET_KEY = "test_v2_secret_123"

_operator_map = {
    ">": RuleOperator.GT,
    "<": RuleOperator.LT,
    "==": RuleOperator.EQ,
    ">=": RuleOperator.GE,
    "<=": RuleOperator.LE,
    "!=": RuleOperator.NE,
    "gt": RuleOperator.GT,
    "lt": RuleOperator.LT,
    "eq": RuleOperator.EQ,
    "ge": RuleOperator.GE,
    "le": RuleOperator.LE,
    "ne": RuleOperator.NE,
}
_operator_funcs = {
    RuleOperator.GT: lambda v, t: v > t,
    RuleOperator.LT: lambda v, t: v < t,
    RuleOperator.EQ: lambda v, t: v == t,
    RuleOperator.GE: lambda v, t: v >= t,
    RuleOperator.LE: lambda v, t: v <= t,
    RuleOperator.NE: lambda v, t: v != t,
}
_metric_aliases = {
    "temperature": ["temp"],
    "humidity": ["hum"],
    "power": ["pwr"],
    "voltage": ["volt", "v"],
    "current": ["amp", "i"],
}

_alert_history = deque(maxlen=100)


def _get_metric(payload: dict, metric: str) -> Optional[float]:
    if metric in payload:
        val = payload[metric]
        if isinstance(val, (int, float)):
            return float(val)
    aliases = _metric_aliases.get(metric, [])
    for alias in aliases:
        if alias in payload:
            val = payload[alias]
            if isinstance(val, (int, float)):
                return float(val)
    return None


def _eval_simple(payload: dict, metric: str, op: RuleOperator, threshold: float) -> tuple:
    val = _get_metric(payload, metric)
    if val is None:
        return False, None
    func = _operator_funcs.get(op)
    if not func:
        return False, val
    return func(val, threshold), val


def _eval_compound(payload: dict, conditions: list) -> tuple:
    if not conditions:
        return False, {}
    
    matched_values = {}
    result = True
    logic_op = conditions[0].get("logic", "and") if len(conditions) > 0 else "and"
    
    for cond in conditions:
        metric = cond.get("metric")
        op_str = cond.get("operator")
        threshold = cond.get("threshold")
        
        if not metric or op_str is None or threshold is None:
            continue
        
        op = _operator_map.get(op_str.lower()) if isinstance(op_str, str) else op_str
        if not op:
            continue
        
        val = _get_metric(payload, metric)
        if val is None:
            matched_values[metric] = None
            if logic_op == "and":
                result = False
            continue
        
        func = _operator_funcs.get(op)
        if not func:
            matched_values[metric] = val
            if logic_op == "and":
                result = False
            continue
        
        cond_result = func(val, threshold)
        matched_values[metric] = val
        
        if logic_op == "and":
            result = result and cond_result
        elif logic_op == "or":
            result = result or cond_result
    
    return result, matched_values


def _check_silence(rule: Rule, now: datetime) -> bool:
    if rule.last_triggered_at is None:
        return False
    silence_seconds = rule.silence_seconds or 60
    elapsed = (now - rule.last_triggered_at).total_seconds()
    return elapsed < silence_seconds


def create_test_device(db: Session) -> Device:
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


def create_simple_rule(
    db: Session,
    device_id: str,
    rule_name: str,
    metric: str,
    operator: RuleOperator,
    threshold: float,
    action: RuleAction,
    command_type: str = None,
    command_value: str = None,
    silence_seconds: int = 60,
    webhook_url: str = None,
    is_active: bool = True
) -> Rule:
    rule = Rule(
        id=str(uuid.uuid4()),
        rule_name=rule_name,
        device_id=device_id,
        condition_type=ConditionType.SIMPLE,
        metric=metric,
        operator=operator,
        threshold=threshold,
        action=action,
        command_type=command_type,
        command_value=command_value,
        silence_seconds=silence_seconds,
        webhook_url=webhook_url,
        is_active=is_active
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    
    logger.info(f"[CreateRule] Created simple rule: {rule_name}")
    logger.info(f"  - Condition: {metric} {operator.value} {threshold}")
    logger.info(f"  - Action: {action.value}")
    if command_type:
        logger.info(f"  - Command: {command_type} = {command_value}")
    
    return rule


def create_compound_rule(
    db: Session,
    device_id: str,
    rule_name: str,
    conditions: List[dict],
    action: RuleAction,
    command_type: str = None,
    command_value: str = None,
    silence_seconds: int = 60,
    webhook_url: str = None,
    is_active: bool = True
) -> Rule:
    rule = Rule(
        id=str(uuid.uuid4()),
        rule_name=rule_name,
        device_id=device_id,
        condition_type=ConditionType.COMPOUND,
        conditions=conditions,
        action=action,
        command_type=command_type,
        command_value=command_value,
        silence_seconds=silence_seconds,
        webhook_url=webhook_url,
        is_active=is_active
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    
    logger.info(f"[CreateRule] Created compound rule: {rule_name}")
    logger.info(f"  - Conditions: {conditions}")
    logger.info(f"  - Action: {action.value}")
    
    return rule


def count_device_commands(db: Session, device_id: str) -> int:
    return db.query(DeviceCommand).filter(DeviceCommand.device_id == device_id).count()


def get_device_commands(db: Session, device_id: str) -> list:
    return db.query(DeviceCommand).filter(
        DeviceCommand.device_id == device_id
    ).order_by(desc(DeviceCommand.created_at)).all()


class EnhancedRuleEngine:
    def __init__(self):
        self.triggered_alerts = []
    
    def process_device_data(self, device_id: str, payload: dict, db: Session):
        rules = db.query(Rule).filter(
            Rule.device_id == device_id,
            Rule.is_active == True
        ).all()
        
        if not rules:
            logger.debug(f"No active rules for device {device_id}")
            return []
        
        device = db.query(Device).filter(Device.device_id == device_id).first()
        if not device:
            logger.error(f"Device {device_id} not found")
            return []
        
        now = datetime.utcnow()
        triggered_commands = []
        
        for rule in rules:
            if _check_silence(rule, now):
                logger.debug(f"Rule {rule.rule_name} is in silence period, skipped")
                continue
            
            is_triggered = False
            matched_values = {}
            
            if rule.condition_type == ConditionType.COMPOUND and rule.conditions:
                is_triggered, matched_values = _eval_compound(payload, rule.conditions)
            elif rule.metric and rule.operator and rule.threshold is not None:
                is_triggered, val = _eval_simple(payload, rule.metric, rule.operator, rule.threshold)
                if val is not None:
                    matched_values[rule.metric] = val
            
            if not is_triggered:
                continue
            
            print("\n" + "=" * 80)
            print(f"  [RULE ALERT] {rule.rule_name}")
            print("=" * 80)
            print(f"  Device: {rule.device_id}")
            print(f"  Values: {matched_values}")
            print(f"  Action: {rule.action.value}")
            if rule.action == RuleAction.SEND_COMMAND:
                print(f"  Command: {rule.command_type} = {rule.command_value}")
            print("=" * 80 + "\n")
            
            logger.warning(f"[RuleEngine] Rule triggered: {rule.rule_name} (device={device_id}, values={matched_values})")
            
            if rule.action == RuleAction.SEND_COMMAND and rule.command_type:
                cmd_result = self._send_command(rule, device, db, now)
                if cmd_result.get("success"):
                    triggered_commands.append(cmd_result)
            
            rule.last_triggered_at = now
            rule.triggered_at = now
            rule.trigger_count = (rule.trigger_count or 0) + 1
            db.commit()
            
            self.triggered_alerts.append({
                "rule_id": rule.id,
                "rule_name": rule.rule_name,
                "device_id": device_id,
                "matched_values": matched_values,
                "triggered_at": now.isoformat()
            })
        
        return triggered_commands
    
    def _send_command(self, rule: Rule, device: Device, db: Session, now: datetime) -> dict:
        if not rule.command_type:
            return {"success": False, "error": "missing_command_type"}
        
        expires_at = now + timedelta(seconds=600)
        
        command = DeviceCommand(
            id=str(uuid.uuid4()),
            device_id=rule.device_id,
            command_type=rule.command_type,
            command_value=rule.command_value,
            status=CommandStatus.PENDING,
            ttl_seconds=600,
            expires_at=expires_at,
            reason=f"Rule: {rule.rule_name}",
            source="rule_engine"
        )
        db.add(command)
        
        pending_list = device.pending_commands or []
        pending_list.append({
            "id": command.id,
            "command": rule.command_type,
            "value": rule.command_value,
            "status": CommandStatus.PENDING.value,
            "created_at": now.isoformat(),
            "reason": command.reason
        })
        device.pending_commands = pending_list
        
        db.commit()
        
        logger.info(f"Command created: {command.id} ({rule.command_type}={rule.command_value})")
        
        return {
            "success": True,
            "command_id": command.id,
            "command_type": rule.command_type,
            "command_value": rule.command_value
        }


def test_case_1_simple_condition():
    logger.info("=" * 60)
    logger.info("TEST CASE 1: Simple Condition (Backward Compatible)")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        engine = EnhancedRuleEngine()
        
        rule = create_simple_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="High Temperature Alert",
            metric="temperature",
            operator=RuleOperator.GT,
            threshold=40.0,
            action=RuleAction.SEND_COMMAND,
            command_type="cooling_on",
            command_value="auto",
            silence_seconds=60
        )
        
        assert rule.condition_type == ConditionType.SIMPLE
        assert rule.metric == "temperature"
        assert rule.operator == RuleOperator.GT
        assert rule.threshold == 40.0
        
        logger.info("[Step 1] Testing 35°C (should NOT trigger)")
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temperature": 35.0}, db)
        assert len(commands) == 0
        logger.info("[Step 1: PASS] 35°C did not trigger")
        
        logger.info("[Step 2] Testing 45°C (should trigger)")
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temperature": 45.0}, db)
        assert len(commands) == 1
        assert commands[0]["command_type"] == "cooling_on"
        assert commands[0]["command_value"] == "auto"
        logger.info(f"[Step 2: PASS] 45°C triggered command: {commands[0]['command_id']}")
        
        db.refresh(rule)
        assert rule.trigger_count == 1
        assert rule.last_triggered_at is not None
        logger.info("[Step 3: PASS] Rule trigger_count updated")
        
        logger.info("\n[TEST CASE 1: PASS]")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 1: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 1: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def test_case_2_compound_condition_and():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 2: Compound Condition (AND Logic)")
    logger.info("  Scenario: temperature > 40 AND humidity < 30")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        engine = EnhancedRuleEngine()
        
        conditions = [
            {"metric": "temperature", "operator": ">", "threshold": 40.0, "logic": "and"},
            {"metric": "humidity", "operator": "<", "threshold": 30.0, "logic": "and"},
        ]
        
        rule = create_compound_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="High Temp + Low Humidity Alert",
            conditions=conditions,
            action=RuleAction.SEND_COMMAND,
            command_type="emergency_cool",
            command_value="high",
            silence_seconds=60
        )
        
        assert rule.condition_type == ConditionType.COMPOUND
        assert len(rule.conditions) == 2
        
        logger.info("[Step 1] Testing temp=45, humidity=50 (AND: humidity fails)")
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temperature": 45.0, "humidity": 50.0}, db)
        assert len(commands) == 0
        logger.info("[Step 1: PASS] Did not trigger (humidity too high)")
        
        logger.info("[Step 2] Testing temp=35, humidity=25 (AND: temp fails)")
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temperature": 35.0, "humidity": 25.0}, db)
        assert len(commands) == 0
        logger.info("[Step 2: PASS] Did not trigger (temp too low)")
        
        logger.info("[Step 3] Testing temp=45, humidity=25 (AND: both pass)")
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temperature": 45.0, "humidity": 25.0}, db)
        assert len(commands) == 1
        assert commands[0]["command_type"] == "emergency_cool"
        logger.info(f"[Step 3: PASS] Triggered command: {commands[0]['command_id']}")
        
        db.refresh(rule)
        assert rule.trigger_count == 1
        logger.info("[Step 4: PASS] Rule trigger_count updated")
        
        logger.info("\n[TEST CASE 2: PASS]")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 2: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 2: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def test_case_3_silence_mechanism():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 3: Silence Period / Storm Suppression")
    logger.info("  Scenario: Rule triggers, then silence_seconds prevents re-trigger")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        engine = EnhancedRuleEngine()
        
        rule = create_simple_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="Silence Test Rule",
            metric="temperature",
            operator=RuleOperator.GT,
            threshold=40.0,
            action=RuleAction.SEND_COMMAND,
            command_type="alert",
            command_value="on",
            silence_seconds=10
        )
        
        logger.info("[Step 1] First trigger (45°C)")
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temperature": 45.0}, db)
        assert len(commands) == 1
        first_cmd_id = commands[0]["command_id"]
        logger.info(f"[Step 1: PASS] First trigger created command: {first_cmd_id}")
        
        db.refresh(rule)
        assert rule.trigger_count == 1
        last_triggered = rule.last_triggered_at
        
        logger.info("[Step 2] Immediate re-trigger (should be silenced)")
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temperature": 50.0}, db)
        assert len(commands) == 0
        logger.info("[Step 2: PASS] Silence period prevented re-trigger")
        
        db.refresh(rule)
        assert rule.trigger_count == 1
        assert rule.last_triggered_at == last_triggered
        logger.info("[Step 3: PASS] trigger_count unchanged during silence")
        
        logger.info("[Step 4] Simulating silence period expired...")
        rule.last_triggered_at = datetime.utcnow() - timedelta(seconds=15)
        db.commit()
        db.refresh(rule)
        
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temperature": 55.0}, db)
        assert len(commands) == 1
        second_cmd_id = commands[0]["command_id"]
        assert second_cmd_id != first_cmd_id
        logger.info(f"[Step 4: PASS] After silence expired, triggered new command: {second_cmd_id}")
        
        db.refresh(rule)
        assert rule.trigger_count == 2
        logger.info("[Step 5: PASS] trigger_count now = 2")
        
        logger.info("\n[TEST CASE 3: PASS]")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 3: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 3: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def test_case_4_metric_aliases():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 4: Metric Aliases")
    logger.info("  Scenario: temp -> temperature, hum -> humidity")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        engine = EnhancedRuleEngine()
        
        rule = create_simple_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="Alias Test Rule",
            metric="temperature",
            operator=RuleOperator.GT,
            threshold=40.0,
            action=RuleAction.SEND_COMMAND,
            command_type="cooling",
            command_value="on",
            silence_seconds=60
        )
        
        logger.info("[Step 1] Testing with 'temp' alias (should work)")
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temp": 45.0}, db)
        assert len(commands) == 1
        logger.info("[Step 1: PASS] 'temp' alias matched 'temperature'")
        
        logger.info("[Step 2] Testing with 'temperature' direct (should work)")
        rule.last_triggered_at = None
        rule.trigger_count = 0
        db.commit()
        
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temperature": 45.0}, db)
        assert len(commands) == 1
        logger.info("[Step 2: PASS] 'temperature' direct matched")
        
        logger.info("[Step 3] Testing with unrelated metric (should NOT work)")
        rule.last_triggered_at = None
        rule.trigger_count = 0
        db.commit()
        
        commands = engine.process_device_data(TEST_DEVICE_ID, {"humidity": 45.0}, db)
        assert len(commands) == 0
        logger.info("[Step 3: PASS] 'humidity' did not match 'temperature'")
        
        logger.info("\n[TEST CASE 4: PASS]")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 4: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 4: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def test_case_5_multiple_rules():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 5: Multiple Rules on Same Device")
    logger.info("  Scenario: Two rules with different thresholds")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        engine = EnhancedRuleEngine()
        
        rule1 = create_simple_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="Rule 1: > 40",
            metric="temperature",
            operator=RuleOperator.GT,
            threshold=40.0,
            action=RuleAction.SEND_COMMAND,
            command_type="cool_low",
            command_value="on",
            silence_seconds=60
        )
        
        rule2 = create_simple_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="Rule 2: > 50",
            metric="temperature",
            operator=RuleOperator.GT,
            threshold=50.0,
            action=RuleAction.SEND_COMMAND,
            command_type="cool_high",
            command_value="on",
            silence_seconds=60
        )
        
        logger.info("[Step 1] Testing 45°C (should trigger only Rule 1)")
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temperature": 45.0}, db)
        assert len(commands) == 1
        assert commands[0]["command_type"] == "cool_low"
        logger.info("[Step 1: PASS] Only Rule 1 triggered")
        
        logger.info("[Step 2] Testing 55°C (should trigger both rules)")
        rule1.last_triggered_at = None
        rule2.last_triggered_at = None
        db.commit()
        
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temperature": 55.0}, db)
        assert len(commands) == 2
        cmd_types = {c["command_type"] for c in commands}
        assert "cool_low" in cmd_types
        assert "cool_high" in cmd_types
        logger.info(f"[Step 2: PASS] Both rules triggered: {cmd_types}")
        
        logger.info("\n[TEST CASE 5: PASS]")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 5: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 5: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def test_case_6_inactive_rule():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 6: Inactive Rule")
    logger.info("  Scenario: Inactive rule should NOT trigger")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        engine = EnhancedRuleEngine()
        
        rule = create_simple_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="Inactive Test Rule",
            metric="temperature",
            operator=RuleOperator.GT,
            threshold=40.0,
            action=RuleAction.SEND_COMMAND,
            command_type="test",
            command_value="on",
            is_active=False
        )
        
        logger.info("[Step 1] Inactive rule should NOT trigger")
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temperature": 45.0}, db)
        assert len(commands) == 0
        logger.info("[Step 1: PASS] Inactive rule did not trigger")
        
        logger.info("[Step 2] Activate rule and test again")
        rule.is_active = True
        db.commit()
        
        commands = engine.process_device_data(TEST_DEVICE_ID, {"temperature": 45.0}, db)
        assert len(commands) == 1
        logger.info("[Step 2: PASS] Activated rule triggered")
        
        logger.info("\n[TEST CASE 6: PASS]")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 6: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 6: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def test_case_7_webhook_config():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 7: Webhook Configuration")
    logger.info("  Scenario: Rule with webhook URL configured")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        
        webhook_url = "https://example.com/webhook/alert"
        webhook_headers = {"Authorization": "Bearer test-token", "X-Custom-Header": "value"}
        
        rule = create_simple_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="Webhook Test Rule",
            metric="temperature",
            operator=RuleOperator.GT,
            threshold=40.0,
            action=RuleAction.NOTIFY,
            silence_seconds=60,
            webhook_url=webhook_url,
            is_active=True
        )
        
        rule.webhook_headers = webhook_headers
        rule.webhook_method = "POST"
        rule.webhook_body_template = {
            "message": "Temperature alert: {temperature}°C",
            "device": "{device_id}"
        }
        db.commit()
        db.refresh(rule)
        
        assert rule.webhook_url == webhook_url
        assert rule.webhook_method == "POST"
        assert rule.webhook_headers == webhook_headers
        assert rule.action == RuleAction.NOTIFY
        logger.info("[Step 1: PASS] Webhook config stored correctly")
        
        retrieved = db.query(Rule).filter(Rule.id == rule.id).first()
        assert retrieved.webhook_url == webhook_url
        assert retrieved.webhook_headers["Authorization"] == "Bearer test-token"
        logger.info("[Step 2: PASS] Webhook config retrieved from DB")
        
        logger.info("\n[TEST CASE 7: PASS]")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 7: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 7: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def main():
    logger.info("\n" + "=" * 60)
    logger.info("RULE ENGINE V2 TEST SUITE")
    logger.info("=" * 60)
    logger.info("")
    
    tests = [
        ("Test 1: Simple Condition", test_case_1_simple_condition),
        ("Test 2: Compound Condition (AND)", test_case_2_compound_condition_and),
        ("Test 3: Silence Mechanism", test_case_3_silence_mechanism),
        ("Test 4: Metric Aliases", test_case_4_metric_aliases),
        ("Test 5: Multiple Rules", test_case_5_multiple_rules),
        ("Test 6: Inactive Rule", test_case_6_inactive_rule),
        ("Test 7: Webhook Config", test_case_7_webhook_config),
    ]
    
    results = []
    
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            logger.error(f"Test '{name}' crashed: {e}", exc_info=True)
            results.append((name, False))
    
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    
    passed = sum(1 for _, r in results if r)
    failed = len(results) - passed
    
    for name, result in results:
        status = "PASS" if result else "FAIL"
        logger.info(f"  [{status}] {name}")
    
    logger.info("")
    logger.info(f"Total: {len(results)} tests")
    logger.info(f"Passed: {passed}")
    logger.info(f"Failed: {failed}")
    logger.info("")
    
    if failed == 0:
        logger.info("[ALL TESTS PASSED]")
        return True
    else:
        logger.error("[SOME TESTS FAILED]")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
