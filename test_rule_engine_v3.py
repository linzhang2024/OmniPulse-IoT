import time
import uuid
import logging
import sys
import os
from datetime import datetime, timedelta
from collections import deque
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server.models import (
    Base, Device, DeviceStatus, DeviceCommand, CommandStatus,
    Rule, RuleAction, RuleOperator, ConditionType,
    AlertSeverity, AlertStatus, AlertHistory
)

LOG_FILE = "logs/test_rule_engine_v3.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("RuleEngineV3Test")

DATABASE_URL = "sqlite:///./iot_devices_test_v3.db"

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)

TEST_DEVICE_ID = f"TEST_V3_{int(time.time())}"
TEST_DEVICE_MODEL = "MultiSensor-X2"
TEST_SECRET_KEY = "test_v3_secret_123"

_operator_map = {
    ">": RuleOperator.GT, "<": RuleOperator.LT, "==": RuleOperator.EQ,
    ">=": RuleOperator.GE, "<=": RuleOperator.LE, "!=": RuleOperator.NE,
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
    "temperature": ["temp"], "humidity": ["hum"],
    "power": ["pwr"], "voltage": ["volt", "v"], "current": ["amp", "i"],
}


def _get_metric(payload: dict, metric: str):
    if metric in payload:
        val = payload[metric]
        if isinstance(val, (int, float)):
            return float(val)
    for alias in _metric_aliases.get(metric, []):
        if alias in payload:
            val = payload[alias]
            if isinstance(val, (int, float)):
                return float(val)
    return None


def _eval_simple(payload: dict, metric: str, op, threshold: float):
    val = _get_metric(payload, metric)
    if val is None:
        return False, None
    func = _operator_funcs.get(op)
    if not func:
        return False, val
    return func(val, threshold), val


def _eval_compound(payload: dict, conditions: list):
    if not conditions:
        return False, {}
    
    matched_values = {}
    logic_op = conditions[0].get("logic", "and") if conditions else "and"
    result = True if logic_op == "and" else False
    
    for cond in conditions:
        metric = cond.get("metric")
        op_str = cond.get("operator")
        threshold = cond.get("threshold")
        
        if not metric or op_str is None or threshold is None:
            if logic_op == "and":
                result = False
            continue
        
        op = _operator_map.get(op_str)
        if not op:
            if logic_op == "and":
                result = False
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
        else:
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
        db.query(AlertHistory).filter(AlertHistory.device_id == TEST_DEVICE_ID).delete()
        db.commit()
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


def create_rule(
    db: Session,
    device_id: str,
    rule_name: str,
    action: RuleAction,
    metric: str = None,
    operator: str = None,
    threshold: float = None,
    conditions: list = None,
    severity: AlertSeverity = AlertSeverity.WARNING,
    silence_seconds: int = 60,
    command_type: str = None,
    command_value: str = None,
    webhook_url: str = None,
) -> Rule:
    condition_type = ConditionType.COMPOUND if conditions else ConditionType.SIMPLE
    
    op_enum = _operator_map.get(operator) if operator else None
    
    rule = Rule(
        id=str(uuid.uuid4()),
        rule_name=rule_name,
        device_id=device_id,
        condition_type=condition_type,
        metric=metric,
        operator=op_enum,
        threshold=threshold,
        conditions=conditions,
        action=action,
        severity=severity,
        command_type=command_type,
        command_value=command_value,
        silence_seconds=silence_seconds,
        webhook_url=webhook_url,
        is_active=True
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    
    logger.info(f"[CreateRule] Created: {rule_name}")
    logger.info(f"  - Type: {condition_type.value}, Severity: {severity.value}")
    if conditions:
        logger.info(f"  - Conditions: {conditions}")
    else:
        logger.info(f"  - Condition: {metric} {operator} {threshold}")
    
    return rule


def process_device_data(db: Session, device_id: str, payload: dict):
    rules = db.query(Rule).filter(
        Rule.device_id == device_id,
        Rule.is_active == True
    ).all()
    
    if not rules:
        return []
    
    device = db.query(Device).filter(Device.device_id == device_id).first()
    if not device:
        logger.error(f"Device {device_id} not found")
        return []
    
    now = datetime.utcnow()
    triggered_alerts = []
    
    for rule in rules:
        if _check_silence(rule, now):
            logger.debug(f"Rule {rule.rule_name} in silence period, skipped")
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
        
        action_result = {"success": True}
        
        if rule.action == RuleAction.SEND_COMMAND and rule.command_type:
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
            action_result["command"] = {
                "success": True,
                "command_id": command.id,
                "command_type": rule.command_type,
                "command_value": rule.command_value
            }
        
        alert = AlertHistory(
            id=str(uuid.uuid4()),
            rule_id=rule.id,
            device_id=rule.device_id,
            rule_name=rule.rule_name,
            severity=rule.severity or AlertSeverity.WARNING,
            status=AlertStatus.OPEN,
            matched_values=matched_values,
            action=rule.action.value,
            action_result=action_result,
            created_at=now,
            updated_at=now
        )
        db.add(alert)
        
        rule.last_triggered_at = now
        rule.triggered_at = now
        rule.trigger_count = (rule.trigger_count or 0) + 1
        
        db.commit()
        db.refresh(alert)
        
        print("\n" + "=" * 80)
        print(f"  [RULE ALERT] {rule.rule_name}")
        print("=" * 80)
        print(f"  Device: {rule.device_id}")
        print(f"  Severity: {rule.severity.value if rule.severity else 'warning'}")
        print(f"  Values: {matched_values}")
        print("=" * 80 + "\n")
        
        triggered_alerts.append(alert)
        logger.warning(f"Rule triggered: {rule.rule_name} (device={device_id}, severity={rule.severity.value if rule.severity else 'warning'})")
    
    return triggered_alerts


def test_case_1_alert_severity():
    logger.info("=" * 60)
    logger.info("TEST CASE 1: Alert Severity Levels")
    logger.info("  critical(紧急), warning(重要), info(提示)")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        
        logger.info("[Step 1] Creating rules with different severities")
        
        rule_critical = create_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="Critical: High Temperature",
            action=RuleAction.NOTIFY,
            metric="temperature",
            operator=">",
            threshold=80.0,
            severity=AlertSeverity.CRITICAL
        )
        
        rule_warning = create_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="Warning: Medium Temperature",
            action=RuleAction.NOTIFY,
            metric="temperature",
            operator=">",
            threshold=50.0,
            severity=AlertSeverity.WARNING
        )
        
        rule_info = create_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="Info: Normal Temperature",
            action=RuleAction.NOTIFY,
            metric="temperature",
            operator=">",
            threshold=30.0,
            severity=AlertSeverity.INFO
        )
        
        logger.info("[Step 2] Testing 35°C (should trigger only INFO)")
        alerts = process_device_data(db, TEST_DEVICE_ID, {"temperature": 35.0})
        assert len(alerts) == 1, f"Expected 1 alert, got {len(alerts)}"
        assert alerts[0].severity == AlertSeverity.INFO, f"Expected INFO severity"
        logger.info(f"[Step 2: PASS] Triggered INFO alert: {alerts[0].id}")
        
        logger.info("[Step 3] Testing 60°C (should trigger INFO + WARNING)")
        rule_info.last_triggered_at = None
        db.commit()
        alerts = process_device_data(db, TEST_DEVICE_ID, {"temperature": 60.0})
        assert len(alerts) == 2, f"Expected 2 alerts, got {len(alerts)}"
        severities = {a.severity for a in alerts}
        assert AlertSeverity.INFO in severities
        assert AlertSeverity.WARNING in severities
        logger.info(f"[Step 3: PASS] Triggered {len(alerts)} alerts with severities: {[s.value for s in severities]}")
        
        logger.info("[Step 4] Testing 90°C (should trigger all 3)")
        rule_info.last_triggered_at = None
        rule_warning.last_triggered_at = None
        db.commit()
        alerts = process_device_data(db, TEST_DEVICE_ID, {"temperature": 90.0})
        assert len(alerts) == 3, f"Expected 3 alerts, got {len(alerts)}"
        severities = {a.severity for a in alerts}
        assert AlertSeverity.CRITICAL in severities
        assert AlertSeverity.WARNING in severities
        assert AlertSeverity.INFO in severities
        logger.info(f"[Step 4: PASS] Triggered {len(alerts)} alerts with all severities")
        
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


def test_case_2_alert_status_flow():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 2: Alert Status Flow")
    logger.info("  OPEN -> ACKNOWLEDGED -> RESOLVED")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        
        rule = create_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="Test Alert Flow",
            action=RuleAction.NOTIFY,
            metric="temperature",
            operator=">",
            threshold=50.0,
            severity=AlertSeverity.WARNING
        )
        
        logger.info("[Step 1] Trigger alert and check initial status")
        alerts = process_device_data(db, TEST_DEVICE_ID, {"temperature": 55.0})
        assert len(alerts) == 1
        alert = alerts[0]
        
        assert alert.status == AlertStatus.OPEN, f"Expected OPEN, got {alert.status}"
        assert alert.acknowledged_by is None
        assert alert.resolved_by is None
        logger.info(f"[Step 1: PASS] Alert status is OPEN: {alert.id}")
        
        logger.info("[Step 2] Acknowledge alert")
        now = datetime.utcnow()
        alert.status = AlertStatus.ACKNOWLEDGED
        alert.acknowledged_by = "user_123"
        alert.acknowledged_at = now
        alert.updated_at = now
        db.commit()
        db.refresh(alert)
        
        assert alert.status == AlertStatus.ACKNOWLEDGED
        assert alert.acknowledged_by == "user_123"
        assert alert.acknowledged_at is not None
        logger.info(f"[Step 2: PASS] Alert acknowledged by user_123")
        
        logger.info("[Step 3] Resolve alert")
        now = datetime.utcnow()
        alert.status = AlertStatus.RESOLVED
        alert.resolved_by = "user_456"
        alert.resolved_at = now
        alert.updated_at = now
        db.commit()
        db.refresh(alert)
        
        assert alert.status == AlertStatus.RESOLVED
        assert alert.resolved_by == "user_456"
        assert alert.resolved_at is not None
        logger.info(f"[Step 3: PASS] Alert resolved by user_456")
        
        logger.info("[Step 4] Query from database")
        queried = db.query(AlertHistory).filter(AlertHistory.id == alert.id).first()
        assert queried.status == AlertStatus.RESOLVED
        assert queried.severity == AlertSeverity.WARNING
        logger.info(f"[Step 4: PASS] Alert retrieved from DB correctly")
        
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
    logger.info("TEST CASE 3: Silence / Storm Suppression")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        
        rule = create_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="Silence Test Rule",
            action=RuleAction.SEND_COMMAND,
            metric="temperature",
            operator=">",
            threshold=50.0,
            severity=AlertSeverity.WARNING,
            silence_seconds=10,
            command_type="cooling",
            command_value="on"
        )
        
        logger.info("[Step 1] First trigger")
        alerts = process_device_data(db, TEST_DEVICE_ID, {"temperature": 55.0})
        assert len(alerts) == 1
        first_alert_id = alerts[0].id
        logger.info(f"[Step 1: PASS] First alert created: {first_alert_id}")
        
        db.refresh(rule)
        assert rule.trigger_count == 1
        assert rule.last_triggered_at is not None
        
        logger.info("[Step 2] Immediate re-trigger (should be silenced)")
        alerts = process_device_data(db, TEST_DEVICE_ID, {"temperature": 60.0})
        assert len(alerts) == 0, f"Expected 0 alerts during silence, got {len(alerts)}"
        
        db.refresh(rule)
        assert rule.trigger_count == 1, "trigger_count should not change during silence"
        logger.info(f"[Step 2: PASS] Silence period prevented re-trigger")
        
        logger.info("[Step 3] Simulate silence expired")
        rule.last_triggered_at = datetime.utcnow() - timedelta(seconds=15)
        db.commit()
        db.refresh(rule)
        
        alerts = process_device_data(db, TEST_DEVICE_ID, {"temperature": 65.0})
        assert len(alerts) == 1
        second_alert_id = alerts[0].id
        assert second_alert_id != first_alert_id
        
        db.refresh(rule)
        assert rule.trigger_count == 2, "trigger_count should be 2 after silence expired"
        logger.info(f"[Step 3: PASS] After silence expired, new alert created: {second_alert_id}")
        
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


def test_case_4_compound_conditions():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 4: Compound Conditions (A > 10 AND B < 20)")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        
        conditions_and = [
            {"metric": "temperature", "operator": ">", "threshold": 40.0, "logic": "and"},
            {"metric": "humidity", "operator": "<", "threshold": 30.0, "logic": "and"},
        ]
        
        rule_and = create_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="Compound AND: High Temp + Low Humidity",
            action=RuleAction.SEND_COMMAND,
            conditions=conditions_and,
            severity=AlertSeverity.WARNING,
            command_type="emergency_cool",
            command_value="high"
        )
        
        logger.info("[Step 1] temp=45, humidity=50 (AND: humidity fails)")
        alerts = process_device_data(db, TEST_DEVICE_ID, {"temperature": 45.0, "humidity": 50.0})
        assert len(alerts) == 0
        logger.info("[Step 1: PASS] Did not trigger (humidity too high)")
        
        logger.info("[Step 2] temp=35, humidity=25 (AND: temp fails)")
        rule_and.last_triggered_at = None
        db.commit()
        alerts = process_device_data(db, TEST_DEVICE_ID, {"temperature": 35.0, "humidity": 25.0})
        assert len(alerts) == 0
        logger.info("[Step 2: PASS] Did not trigger (temp too low)")
        
        logger.info("[Step 3] temp=45, humidity=25 (AND: both pass)")
        rule_and.last_triggered_at = None
        db.commit()
        alerts = process_device_data(db, TEST_DEVICE_ID, {"temperature": 45.0, "humidity": 25.0})
        assert len(alerts) == 1
        
        alert = alerts[0]
        assert alert.matched_values == {"temperature": 45.0, "humidity": 25.0}
        logger.info(f"[Step 3: PASS] Triggered with matched_values: {alert.matched_values}")
        
        logger.info("[Step 4] Verify AlertHistory stored in DB")
        db_alerts = db.query(AlertHistory).filter(
            AlertHistory.device_id == TEST_DEVICE_ID
        ).all()
        assert len(db_alerts) == 1
        assert db_alerts[0].status == AlertStatus.OPEN
        assert db_alerts[0].matched_values == {"temperature": 45.0, "humidity": 25.0}
        logger.info("[Step 4: PASS] AlertHistory stored correctly in DB")
        
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


def test_case_5_webhook_config():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 5: Webhook Configuration")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        
        webhook_url = "https://example.com/webhook/test"
        webhook_headers = {
            "Authorization": "Bearer test-token-123",
            "X-Custom-Header": "custom-value"
        }
        
        rule = create_rule(
            db=db,
            device_id=TEST_DEVICE_ID,
            rule_name="Webhook Test Rule",
            action=RuleAction.NOTIFY,
            metric="temperature",
            operator=">",
            threshold=50.0,
            severity=AlertSeverity.CRITICAL,
            webhook_url=webhook_url
        )
        
        rule.webhook_headers = webhook_headers
        rule.webhook_method = "POST"
        db.commit()
        db.refresh(rule)
        
        assert rule.webhook_url == webhook_url
        assert rule.webhook_method == "POST"
        assert rule.webhook_headers == webhook_headers
        logger.info("[Step 1: PASS] Webhook config stored in rule")
        
        logger.info("[Step 2] Trigger alert and verify webhook is associated")
        alerts = process_device_data(db, TEST_DEVICE_ID, {"temperature": 55.0})
        assert len(alerts) == 1
        
        alert = alerts[0]
        assert alert.severity == AlertSeverity.CRITICAL
        assert alert.action == "notify"
        logger.info(f"[Step 2: PASS] Alert created with severity=CRITICAL, action=notify")
        
        db.refresh(rule)
        assert rule.trigger_count == 1
        assert rule.last_triggered_at is not None
        logger.info("[Step 3: PASS] Rule trigger_count updated")
        
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


def test_case_6_alert_stats():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 6: Alert Stats & Filtering")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        
        logger.info("[Step 1] Creating multiple alerts with different statuses")
        
        now = datetime.utcnow()
        
        alert1 = AlertHistory(
            id=str(uuid.uuid4()),
            rule_id=str(uuid.uuid4()),
            device_id=TEST_DEVICE_ID,
            rule_name="Open Critical Alert",
            severity=AlertSeverity.CRITICAL,
            status=AlertStatus.OPEN,
            matched_values={"temperature": 90.0},
            action="notify",
            created_at=now - timedelta(hours=2),
            updated_at=now - timedelta(hours=2)
        )
        db.add(alert1)
        
        alert2 = AlertHistory(
            id=str(uuid.uuid4()),
            rule_id=str(uuid.uuid4()),
            device_id=TEST_DEVICE_ID,
            rule_name="Acknowledged Warning",
            severity=AlertSeverity.WARNING,
            status=AlertStatus.ACKNOWLEDGED,
            matched_values={"temperature": 60.0},
            action="send_command",
            acknowledged_by="user_1",
            acknowledged_at=now - timedelta(hours=1),
            created_at=now - timedelta(hours=3),
            updated_at=now - timedelta(hours=1)
        )
        db.add(alert2)
        
        alert3 = AlertHistory(
            id=str(uuid.uuid4()),
            rule_id=str(uuid.uuid4()),
            device_id=TEST_DEVICE_ID,
            rule_name="Resolved Info",
            severity=AlertSeverity.INFO,
            status=AlertStatus.RESOLVED,
            matched_values={"temperature": 35.0},
            action="notify",
            resolved_by="user_2",
            resolved_at=now - timedelta(minutes=30),
            created_at=now - timedelta(hours=4),
            updated_at=now - timedelta(minutes=30)
        )
        db.add(alert3)
        
        db.commit()
        
        logger.info("[Step 2] Query different statuses")
        
        open_alerts = db.query(AlertHistory).filter(
            AlertHistory.status == AlertStatus.OPEN
        ).all()
        assert len(open_alerts) == 1
        assert open_alerts[0].severity == AlertSeverity.CRITICAL
        logger.info(f"[Step 2a: PASS] Found 1 OPEN alert (CRITICAL)")
        
        ack_alerts = db.query(AlertHistory).filter(
            AlertHistory.status == AlertStatus.ACKNOWLEDGED
        ).all()
        assert len(ack_alerts) == 1
        assert ack_alerts[0].severity == AlertSeverity.WARNING
        logger.info(f"[Step 2b: PASS] Found 1 ACKNOWLEDGED alert (WARNING)")
        
        resolved_alerts = db.query(AlertHistory).filter(
            AlertHistory.status == AlertStatus.RESOLVED
        ).all()
        assert len(resolved_alerts) == 1
        assert resolved_alerts[0].severity == AlertSeverity.INFO
        logger.info(f"[Step 2c: PASS] Found 1 RESOLVED alert (INFO)")
        
        logger.info("[Step 3] Query by severity")
        
        critical_alerts = db.query(AlertHistory).filter(
            AlertHistory.severity == AlertSeverity.CRITICAL
        ).all()
        assert len(critical_alerts) == 1
        logger.info(f"[Step 3a: PASS] Found 1 CRITICAL alert")
        
        all_alerts = db.query(AlertHistory).order_by(
            AlertHistory.created_at.desc()
        ).all()
        assert len(all_alerts) == 3
        logger.info(f"[Step 3b: PASS] Found total 3 alerts")
        
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


def main():
    logger.info("\n" + "=" * 60)
    logger.info("RULE ENGINE V3 TEST SUITE")
    logger.info("=" * 60)
    logger.info("")
    
    tests = [
        ("Test 1: Alert Severity Levels", test_case_1_alert_severity),
        ("Test 2: Alert Status Flow", test_case_2_alert_status_flow),
        ("Test 3: Silence Mechanism", test_case_3_silence_mechanism),
        ("Test 4: Compound Conditions", test_case_4_compound_conditions),
        ("Test 5: Webhook Config", test_case_5_webhook_config),
        ("Test 6: Alert Stats & Filtering", test_case_6_alert_stats),
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
