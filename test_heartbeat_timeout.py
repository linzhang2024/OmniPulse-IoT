import time
import hashlib
import uuid
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server.models import (
    Base, Device, DeviceStatus, DeviceStatusEvent,
    DeviceDataHistory
)

LOG_FILE = "logs/test_heartbeat_timeout.log"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("HeartbeatTimeoutTest")

DATABASE_URL = "sqlite:///./iot_devices.db"

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

HEARTBEAT_TIMEOUT = 120
CHECK_INTERVAL = 30

TEST_DEVICE_ID = f"TEST_TIMEOUT_{int(time.time())}"
TEST_DEVICE_MODEL = "TimeoutTestModel"
TEST_SECRET_KEY = "test_secret_key_123"


def generate_signature(device_id: str, timestamp: str, secret_key: str) -> str:
    raw_string = f"{device_id}{timestamp}{secret_key}"
    return hashlib.md5(raw_string.encode('utf-8')).hexdigest().lower()


def create_test_device(db: Session) -> Device:
    logger.info(f"Creating test device: {TEST_DEVICE_ID}")
    
    existing = db.query(Device).filter(Device.device_id == TEST_DEVICE_ID).first()
    if existing:
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
    
    logger.info(f"Test device created: {device.device_id}, status: {device.status.value}")
    return device


def simulate_heartbeat(device: Device, db: Session) -> Dict[str, Any]:
    logger.info(f"Simulating heartbeat for device: {device.device_id}")
    
    now = datetime.utcnow()
    old_status = device.status.value
    
    device.last_heartbeat = now
    device.last_seen = now
    
    if device.status != DeviceStatus.ONLINE:
        device.status = DeviceStatus.ONLINE
        new_status = device.status.value
        
        event = DeviceStatusEvent(
            id=str(uuid.uuid4()),
            device_id=device.device_id,
            event_type="online",
            old_status=old_status,
            new_status=new_status,
            reason="Device heartbeat received (test simulation)",
            details={"timestamp": now.isoformat()}
        )
        db.add(event)
        logger.info(f"[StatusEvent] Device {device.device_id} went ONLINE")
    
    db.commit()
    db.refresh(device)
    
    return {
        "device_id": device.device_id,
        "status": device.status.value,
        "last_heartbeat": device.last_heartbeat.isoformat() if device.last_heartbeat else None,
        "last_seen": device.last_seen.isoformat() if device.last_seen else None
    }


def simulate_timeout(device: Device, db: Session, timeout_seconds: int = 130) -> Dict[str, Any]:
    logger.info(f"Simulating timeout: setting last_seen to {timeout_seconds} seconds ago")
    
    now = datetime.utcnow()
    past_time = now - timedelta(seconds=timeout_seconds)
    
    device.last_heartbeat = past_time
    device.last_seen = past_time
    
    db.commit()
    db.refresh(device)
    
    logger.info(f"Device {device.device_id} last_seen set to: {past_time} ({timeout_seconds}s ago)")
    
    return {
        "device_id": device.device_id,
        "last_heartbeat": device.last_heartbeat.isoformat() if device.last_heartbeat else None,
        "last_seen": device.last_seen.isoformat() if device.last_seen else None
    }


def check_device_timeout(device: Device, db: Session, timeout_seconds: int = HEARTBEAT_TIMEOUT) -> Dict[str, Any]:
    logger.info(f"Checking device timeout for {device.device_id}")
    
    now = datetime.utcnow()
    last_active_time = device.last_seen if device.last_seen else device.last_heartbeat
    
    if last_active_time is None:
        return {
            "device_id": device.device_id,
            "status": device.status.value,
            "was_offlined": False,
            "reason": "No activity recorded"
        }
    
    time_since_active = (now - last_active_time).total_seconds()
    old_status = device.status.value
    
    was_offlined = False
    reason = None
    
    if device.status == DeviceStatus.ONLINE and time_since_active > timeout_seconds:
        device.status = DeviceStatus.OFFLINE
        new_status = device.status.value
        was_offlined = True
        
        reason = f"No heartbeat for {time_since_active:.1f} seconds (timeout: {timeout_seconds}s)"
        event = DeviceStatusEvent(
            id=str(uuid.uuid4()),
            device_id=device.device_id,
            event_type="offline",
            old_status=old_status,
            new_status=new_status,
            reason=reason,
            details={
                "last_active_time": last_active_time.isoformat() if last_active_time else None,
                "timeout_seconds": timeout_seconds,
                "time_since_active": time_since_active
            }
        )
        db.add(event)
        
        logger.warning(f"[DeviceOffline] Device {device.device_id} marked as OFFLINE. Reason: {reason}")
    
    db.commit()
    db.refresh(device)
    
    return {
        "device_id": device.device_id,
        "status": device.status.value,
        "was_offlined": was_offlined,
        "reason": reason,
        "time_since_active": time_since_active,
        "timeout_seconds": timeout_seconds
    }


def get_status_events(db: Session, device_id: str = None, event_type: str = None) -> list:
    query = db.query(DeviceStatusEvent)
    
    if device_id:
        query = query.filter(DeviceStatusEvent.device_id == device_id)
    if event_type:
        query = query.filter(DeviceStatusEvent.event_type == event_type)
    
    events = query.order_by(desc(DeviceStatusEvent.created_at)).all()
    
    result = []
    for event in events:
        result.append({
            "id": event.id,
            "device_id": event.device_id,
            "event_type": event.event_type,
            "old_status": event.old_status,
            "new_status": event.new_status,
            "reason": event.reason,
            "details": event.details,
            "created_at": event.created_at.isoformat() if event.created_at else None
        })
    
    return result


def run_test_case_1_basic_timeout():
    logger.info("=" * 60)
    logger.info("TEST CASE 1: Basic Heartbeat Timeout")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        assert device.status == DeviceStatus.OFFLINE, "Initial status should be OFFLINE"
        logger.info("[PASS] Initial status is OFFLINE")
        
        heartbeat_result = simulate_heartbeat(device, db)
        assert heartbeat_result["status"] == "online", "Status should be ONLINE after heartbeat"
        logger.info("[PASS] Status changed to ONLINE after heartbeat")
        
        events = get_status_events(db, device_id=TEST_DEVICE_ID, event_type="online")
        assert len(events) >= 1, "Should have at least one ONLINE event"
        logger.info(f"[PASS] Found {len(events)} ONLINE event(s)")
        
        simulate_timeout(device, db, timeout_seconds=130)
        
        timeout_result = check_device_timeout(device, db, timeout_seconds=HEARTBEAT_TIMEOUT)
        assert timeout_result["was_offlined"] == True, "Device should be offlined"
        assert timeout_result["status"] == "offline", "Status should be OFFLINE"
        logger.info("[PASS] Device was offlined due to timeout")
        
        offline_events = get_status_events(db, device_id=TEST_DEVICE_ID, event_type="offline")
        assert len(offline_events) >= 1, "Should have at least one OFFLINE event"
        logger.info(f"[PASS] Found {len(offline_events)} OFFLINE event(s)")
        
        offline_event = offline_events[0]
        logger.info(f"Offline event details:")
        logger.info(f"  - event_type: {offline_event['event_type']}")
        logger.info(f"  - old_status: {offline_event['old_status']}")
        logger.info(f"  - new_status: {offline_event['new_status']}")
        logger.info(f"  - reason: {offline_event['reason']}")
        logger.info(f"  - details: {offline_event['details']}")
        
        logger.info("\n[TEST CASE 1: PASS] Basic heartbeat timeout works correctly!")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 1: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 1: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def run_test_case_2_no_timeout():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 2: No Timeout (within threshold)")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device_id = f"TEST_NO_TIMEOUT_{int(time.time())}"
        device = Device(
            device_id=device_id,
            secret_key=TEST_SECRET_KEY,
            model=TEST_DEVICE_MODEL,
            status=DeviceStatus.OFFLINE,
            last_heartbeat=None,
            last_seen=None
        )
        db.add(device)
        db.commit()
        db.refresh(device)
        
        heartbeat_result = simulate_heartbeat(device, db)
        assert heartbeat_result["status"] == "online", "Status should be ONLINE after heartbeat"
        logger.info("[PASS] Status changed to ONLINE after heartbeat")
        
        simulate_timeout(device, db, timeout_seconds=60)
        
        timeout_result = check_device_timeout(device, db, timeout_seconds=HEARTBEAT_TIMEOUT)
        assert timeout_result["was_offlined"] == False, "Device should NOT be offlined"
        assert timeout_result["status"] == "online", "Status should remain ONLINE"
        logger.info(f"[PASS] Device remained ONLINE (only {timeout_result['time_since_active']:.1f}s since last active)")
        
        offline_events = get_status_events(db, device_id=device_id, event_type="offline")
        assert len(offline_events) == 0, "Should have NO OFFLINE events"
        logger.info("[PASS] No OFFLINE events recorded")
        
        logger.info("\n[TEST CASE 2: PASS] No false positives for timeout!")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 2: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 2: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def run_test_case_3_device_without_activity():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 3: Device Without Any Activity")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device_id = f"TEST_NO_ACTIVITY_{int(time.time())}"
        device = Device(
            device_id=device_id,
            secret_key=TEST_SECRET_KEY,
            model=TEST_DEVICE_MODEL,
            status=DeviceStatus.OFFLINE,
            last_heartbeat=None,
            last_seen=None
        )
        db.add(device)
        db.commit()
        db.refresh(device)
        
        assert device.status == DeviceStatus.OFFLINE, "Initial status should be OFFLINE"
        logger.info("[PASS] Initial status is OFFLINE")
        
        timeout_result = check_device_timeout(device, db, timeout_seconds=HEARTBEAT_TIMEOUT)
        assert timeout_result["was_offlined"] == False, "Should not offline device without activity"
        assert timeout_result["status"] == "offline", "Status should remain OFFLINE"
        logger.info("[PASS] Device without activity remains OFFLINE")
        
        events = get_status_events(db, device_id=device_id)
        assert len(events) == 0, "Should have NO events for inactive device"
        logger.info("[PASS] No events recorded for inactive device")
        
        logger.info("\n[TEST CASE 3: PASS] Inactive devices handled correctly!")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 3: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 3: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def run_test_case_4_status_events_query():
    logger.info("\n" + "=" * 60)
    logger.info("TEST CASE 4: Status Events Query Interface")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        device_id1 = f"TEST_QUERY_1_{int(time.time())}"
        device_id2 = f"TEST_QUERY_2_{int(time.time())}"
        
        for dev_id in [device_id1, device_id2]:
            device = Device(
                device_id=dev_id,
                secret_key=TEST_SECRET_KEY,
                model=TEST_DEVICE_MODEL,
                status=DeviceStatus.OFFLINE,
                last_heartbeat=None,
                last_seen=None
            )
            db.add(device)
        db.commit()
        
        for dev_id in [device_id1, device_id2]:
            device = db.query(Device).filter(Device.device_id == dev_id).first()
            simulate_heartbeat(device, db)
        
        for dev_id in [device_id1, device_id2]:
            device = db.query(Device).filter(Device.device_id == dev_id).first()
            simulate_timeout(device, db, timeout_seconds=130)
            check_device_timeout(device, db, timeout_seconds=HEARTBEAT_TIMEOUT)
        
        all_events = get_status_events(db)
        assert len(all_events) >= 4, f"Should have at least 4 events, got {len(all_events)}"
        logger.info(f"[PASS] Found {len(all_events)} total events")
        
        device1_events = get_status_events(db, device_id=device_id1)
        assert len(device1_events) >= 2, f"Should have at least 2 events for device1, got {len(device1_events)}"
        logger.info(f"[PASS] Found {len(device1_events)} events for device1")
        
        online_events = get_status_events(db, event_type="online")
        assert len(online_events) >= 2, f"Should have at least 2 ONLINE events, got {len(online_events)}"
        logger.info(f"[PASS] Found {len(online_events)} ONLINE events")
        
        offline_events = get_status_events(db, event_type="offline")
        assert len(offline_events) >= 2, f"Should have at least 2 OFFLINE events, got {len(offline_events)}"
        logger.info(f"[PASS] Found {len(offline_events)} OFFLINE events")
        
        logger.info("\n[TEST CASE 4: PASS] Status events query works correctly!")
        return True
        
    except AssertionError as e:
        logger.error(f"[TEST CASE 4: FAIL] {e}")
        return False
    except Exception as e:
        logger.error(f"[TEST CASE 4: ERROR] {e}", exc_info=True)
        return False
    finally:
        db.close()


def main():
    logger.info("=" * 60)
    logger.info("Heartbeat Timeout Test Suite")
    logger.info("=" * 60)
    logger.info(f"Test Parameters:")
    logger.info(f"  - HEARTBEAT_TIMEOUT: {HEARTBEAT_TIMEOUT} seconds (2 minutes)")
    logger.info(f"  - CHECK_INTERVAL: {CHECK_INTERVAL} seconds")
    logger.info("")
    
    test_results = []
    
    test_results.append(("Test Case 1: Basic Timeout", run_test_case_1_basic_timeout()))
    test_results.append(("Test Case 2: No Timeout", run_test_case_2_no_timeout()))
    test_results.append(("Test Case 3: Device Without Activity", run_test_case_3_device_without_activity()))
    test_results.append(("Test Case 4: Status Events Query", run_test_case_4_status_events_query()))
    
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
        logger.info("\n[ALL TESTS PASSED] Heartbeat timeout mechanism is working correctly!")
    else:
        logger.warning(f"\n[SOME TESTS FAILED] Please check the logs for details: {LOG_FILE}")
    
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
