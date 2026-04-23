import time
import hashlib
import uuid
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server.models import (
    Base, Device, DeviceStatus, DeviceStatusEvent,
    DeviceDataHistory
)

LOG_FILE = "logs/test_heartbeat_timeout_v2.log"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("HeartbeatTimeoutTestV2")

DATABASE_URL = "sqlite:///./iot_devices.db"

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

PENDING_OFFLINE_THRESHOLD = 60
OFFLINE_THRESHOLD = 120


def print_test_banner(test_name: str):
    print("\n" + "=" * 70)
    print(f"  TEST: {test_name}")
    print("=" * 70)


def print_test_result(success: bool, message: str = ""):
    status = "[PASS]" if success else "[FAIL]"
    print(f"\n  {status}: {message}")
    print("-" * 70)


def create_test_device(db: Session, device_id: str = None) -> Device:
    if device_id is None:
        device_id = f"TEST_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    
    existing = db.query(Device).filter(Device.device_id == device_id).first()
    if existing:
        db.delete(existing)
        db.commit()
    
    device = Device(
        device_id=device_id,
        secret_key="test_secret_key_123",
        model="TestModel",
        status=DeviceStatus.OFFLINE,
        last_heartbeat=None,
        last_seen=None
    )
    db.add(device)
    db.commit()
    db.refresh(device)
    
    logger.info(f"Created test device: {device_id}, status: {device.status.value}")
    return device


def simulate_heartbeat(db: Session, device: Device) -> Dict[str, Any]:
    now = datetime.utcnow()
    old_status = device.status.value
    
    device.last_heartbeat = now
    device.last_seen = now
    
    status_changed = False
    if device.status != DeviceStatus.ONLINE:
        device.status = DeviceStatus.ONLINE
        new_status = device.status.value
        status_changed = True
        
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
        logger.info(f"[DeviceOnline] Device {device.device_id} changed: {old_status} -> {new_status}")
    
    db.commit()
    db.refresh(device)
    
    return {
        "device_id": device.device_id,
        "status": device.status.value,
        "status_changed": status_changed,
        "old_status": old_status,
        "new_status": device.status.value,
        "last_heartbeat": device.last_heartbeat.isoformat() if device.last_heartbeat else None,
        "last_seen": device.last_seen.isoformat() if device.last_seen else None
    }


def simulate_inactivity(db: Session, device: Device, seconds_ago: int) -> Dict[str, Any]:
    now = datetime.utcnow()
    past_time = now - timedelta(seconds=seconds_ago)
    
    device.last_heartbeat = past_time
    device.last_seen = past_time
    
    db.commit()
    db.refresh(device)
    
    logger.info(f"Set device {device.device_id} last_seen to {seconds_ago}s ago (current status: {device.status.value})")
    
    return {
        "device_id": device.device_id,
        "last_heartbeat": device.last_heartbeat.isoformat() if device.last_heartbeat else None,
        "last_seen": device.last_seen.isoformat() if device.last_seen else None,
        "current_status": device.status.value
    }


def check_device_status_machine(db: Session, device: Device) -> Dict[str, Any]:
    last_active_time = device.last_seen if device.last_seen else device.last_heartbeat
    
    if last_active_time is None:
        return {
            "device_id": device.device_id,
            "status": device.status.value,
            "status_changed": False,
            "reason": "No activity recorded"
        }
    
    now = datetime.utcnow()
    time_since_active = (now - last_active_time).total_seconds()
    old_status = device.status.value
    status_changed = False
    new_status = old_status
    reason = None
    
    if device.status == DeviceStatus.ONLINE:
        if time_since_active > PENDING_OFFLINE_THRESHOLD and time_since_active <= OFFLINE_THRESHOLD:
            device.status = DeviceStatus.PENDING_OFFLINE
            new_status = device.status.value
            status_changed = True
            reason = f"No heartbeat for {time_since_active:.1f}s (pending threshold: {PENDING_OFFLINE_THRESHOLD}s)"
            
            event = DeviceStatusEvent(
                id=str(uuid.uuid4()),
                device_id=device.device_id,
                event_type="pending_offline",
                old_status=old_status,
                new_status=new_status,
                reason=reason,
                details={
                    "last_active_time": last_active_time.isoformat(),
                    "time_since_active": time_since_active,
                    "pending_threshold": PENDING_OFFLINE_THRESHOLD,
                    "offline_threshold": OFFLINE_THRESHOLD
                }
            )
            db.add(event)
            logger.warning(f"[DevicePendingOffline] Device {device.device_id}: {old_status} -> {new_status}")
        
        elif time_since_active > OFFLINE_THRESHOLD:
            device.status = DeviceStatus.OFFLINE
            new_status = device.status.value
            status_changed = True
            reason = f"No heartbeat for {time_since_active:.1f}s (timeout: {OFFLINE_THRESHOLD}s)"
            
            event = DeviceStatusEvent(
                id=str(uuid.uuid4()),
                device_id=device.device_id,
                event_type="offline",
                old_status=old_status,
                new_status=new_status,
                reason=reason,
                details={
                    "last_active_time": last_active_time.isoformat(),
                    "time_since_active": time_since_active,
                    "timeout_seconds": OFFLINE_THRESHOLD
                }
            )
            db.add(event)
            logger.critical(f"[DeviceOffline] Device {device.device_id}: {old_status} -> {new_status}")
    
    elif device.status == DeviceStatus.PENDING_OFFLINE:
        if time_since_active > OFFLINE_THRESHOLD:
            device.status = DeviceStatus.OFFLINE
            new_status = device.status.value
            status_changed = True
            reason = f"No heartbeat for {time_since_active:.1f}s (timeout: {OFFLINE_THRESHOLD}s, was pending)"
            
            event = DeviceStatusEvent(
                id=str(uuid.uuid4()),
                device_id=device.device_id,
                event_type="offline",
                old_status=old_status,
                new_status=new_status,
                reason=reason,
                details={
                    "last_active_time": last_active_time.isoformat(),
                    "time_since_active": time_since_active,
                    "timeout_seconds": OFFLINE_THRESHOLD,
                    "was_pending": True
                }
            )
            db.add(event)
            logger.critical(f"[DeviceOffline] Device {device.device_id}: {old_status} -> {new_status} (from pending)")
    
    if status_changed:
        db.commit()
        db.refresh(device)
    
    return {
        "device_id": device.device_id,
        "status": device.status.value,
        "status_changed": status_changed,
        "old_status": old_status,
        "new_status": new_status,
        "reason": reason,
        "time_since_active": time_since_active,
        "pending_threshold": PENDING_OFFLINE_THRESHOLD,
        "offline_threshold": OFFLINE_THRESHOLD
    }


def get_events(db: Session, device_id: str = None, event_type: str = None) -> List[Dict]:
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


def test_1_basic_offline_timeout():
    """
    Test Case 1: Basic Offline Timeout
    - Device goes ONLINE
    - Simulate > 2 minutes of inactivity
    - Verify: ONLINE -> OFFLINE (directly, without passing through PENDING)
    """
    print_test_banner("Test 1: Basic Offline Timeout (> 2 minutes)")
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        assert device.status == DeviceStatus.OFFLINE, "Initial status should be OFFLINE"
        print(f"  [1] Initial status: {device.status.value} OK")
        
        result = simulate_heartbeat(db, device)
        assert result["status"] == "online", f"Status should be ONLINE after heartbeat, got {result['status']}"
        print(f"  [2] After heartbeat: {result['status']} OK")
        
        simulate_inactivity(db, device, seconds_ago=130)
        
        check_result = check_device_status_machine(db, device)
        print(f"  [3] After 130s inactivity check:")
        print(f"      - Time since active: {check_result['time_since_active']:.1f}s")
        print(f"      - Status changed: {check_result['status_changed']}")
        print(f"      - Old status: {check_result['old_status']}")
        print(f"      - New status: {check_result['new_status']}")
        print(f"      - Reason: {check_result['reason']}")
        
        assert check_result["status_changed"] == True, "Status should have changed"
        assert check_result["new_status"] == "offline", f"Should be OFFLINE, got {check_result['new_status']}"
        
        offline_events = get_events(db, device_id=device.device_id, event_type="offline")
        assert len(offline_events) >= 1, f"Should have OFFLINE event, got {len(offline_events)}"
        print(f"  [4] OFFLINE event recorded OK")
        
        print_test_result(True, f"Device correctly transitioned: ONLINE -> OFFLINE after 130s")
        return True
        
    except AssertionError as e:
        logger.error(f"Test failed: {e}")
        print_test_result(False, str(e))
        return False
    except Exception as e:
        logger.error(f"Test error: {e}", exc_info=True)
        print_test_result(False, str(e))
        return False
    finally:
        db.close()


def test_2_pending_offline_state():
    """
    Test Case 2: Pending Offline State
    - Device goes ONLINE
    - Simulate 1-2 minutes of inactivity (e.g., 70 seconds)
    - Verify: ONLINE -> PENDING_OFFLINE
    """
    print_test_banner("Test 2: Pending Offline State (1-2 minutes)")
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        
        simulate_heartbeat(db, device)
        assert device.status == DeviceStatus.ONLINE, "Should be ONLINE after heartbeat"
        print(f"  [1] After heartbeat: {device.status.value} OK")
        
        simulate_inactivity(db, device, seconds_ago=70)
        
        check_result = check_device_status_machine(db, device)
        print(f"  [2] After 70s inactivity check:")
        print(f"      - Time since active: {check_result['time_since_active']:.1f}s")
        print(f"      - Status changed: {check_result['status_changed']}")
        print(f"      - Old status: {check_result['old_status']}")
        print(f"      - New status: {check_result['new_status']}")
        
        assert check_result["status_changed"] == True, "Status should have changed"
        assert check_result["new_status"] == "pending_offline", f"Should be PENDING_OFFLINE, got {check_result['new_status']}"
        
        pending_events = get_events(db, device_id=device.device_id, event_type="pending_offline")
        assert len(pending_events) >= 1, f"Should have PENDING_OFFLINE event, got {len(pending_events)}"
        print(f"  [3] PENDING_OFFLINE event recorded OK")
        
        print_test_result(True, f"Device correctly transitioned: ONLINE -> PENDING_OFFLINE after 70s")
        return True
        
    except AssertionError as e:
        logger.error(f"Test failed: {e}")
        print_test_result(False, str(e))
        return False
    except Exception as e:
        logger.error(f"Test error: {e}", exc_info=True)
        print_test_result(False, str(e))
        return False
    finally:
        db.close()


def test_3_critical_point_recovery():
    """
    Test Case 3: Critical Point Recovery (Network Jitter Simulation)
    - Device goes ONLINE
    - Simulate 55 seconds of inactivity (just BEFORE PENDING_OFFLINE threshold)
    - Device sends heartbeat
    - Verify: Status remains ONLINE (no PENDING_OFFLINE)
    """
    print_test_banner("Test 3: Critical Point Recovery (55s -> heartbeat)")
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        
        simulate_heartbeat(db, device)
        assert device.status == DeviceStatus.ONLINE, "Should be ONLINE after heartbeat"
        print(f"  [1] After heartbeat: {device.status.value} OK")
        
        simulate_inactivity(db, device, seconds_ago=55)
        
        check_result = check_device_status_machine(db, device)
        print(f"  [2] After 55s inactivity check:")
        print(f"      - Time since active: {check_result['time_since_active']:.1f}s")
        print(f"      - Status changed: {check_result['status_changed']}")
        print(f"      - Current status: {check_result['status']}")
        
        assert check_result["status_changed"] == False, "Status should NOT have changed at 55s"
        assert check_result["status"] == "online", f"Should still be ONLINE, got {check_result['status']}"
        print(f"  [3] Status remains ONLINE at 55s OK")
        
        heartbeat_result = simulate_heartbeat(db, device)
        print(f"  [4] Heartbeat sent at 55s:")
        print(f"      - Status changed: {heartbeat_result['status_changed']}")
        print(f"      - New status: {heartbeat_result['new_status']}")
        
        assert heartbeat_result["status"] == "online", "Should remain ONLINE"
        
        pending_events = get_events(db, device_id=device.device_id, event_type="pending_offline")
        offline_events = get_events(db, device_id=device.device_id, event_type="offline")
        print(f"  [5] Events check:")
        print(f"      - PENDING_OFFLINE events: {len(pending_events)}")
        print(f"      - OFFLINE events: {len(offline_events)}")
        
        assert len(pending_events) == 0, f"Should have NO PENDING_OFFLINE events, got {len(pending_events)}"
        assert len(offline_events) == 0, f"Should have NO OFFLINE events, got {len(offline_events)}"
        
        print_test_result(True, f"Device recovered at 55s (before 60s threshold), status remains ONLINE")
        return True
        
    except AssertionError as e:
        logger.error(f"Test failed: {e}")
        print_test_result(False, str(e))
        return False
    except Exception as e:
        logger.error(f"Test error: {e}", exc_info=True)
        print_test_result(False, str(e))
        return False
    finally:
        db.close()


def test_4_pending_offline_recovery():
    """
    Test Case 4: Pending Offline Recovery
    - Device goes ONLINE
    - Simulate 70 seconds (PENDING_OFFLINE)
    - Device sends heartbeat
    - Verify: PENDING_OFFLINE -> ONLINE (recovery)
    """
    print_test_banner("Test 4: Pending Offline Recovery (70s -> heartbeat)")
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        
        simulate_heartbeat(db, device)
        assert device.status == DeviceStatus.ONLINE, "Should be ONLINE after heartbeat"
        print(f"  [1] After heartbeat: {device.status.value} OK")
        
        simulate_inactivity(db, device, seconds_ago=70)
        
        check_result_1 = check_device_status_machine(db, device)
        print(f"  [2] After 70s inactivity:")
        print(f"      - Status: {check_result_1['new_status']}")
        assert check_result_1["new_status"] == "pending_offline", f"Should be PENDING_OFFLINE, got {check_result_1['new_status']}"
        print(f"      OK Entered PENDING_OFFLINE state")
        
        heartbeat_result = simulate_heartbeat(db, device)
        print(f"  [3] Heartbeat sent while PENDING_OFFLINE:")
        print(f"      - Status changed: {heartbeat_result['status_changed']}")
        print(f"      - Old status: {heartbeat_result['old_status']}")
        print(f"      - New status: {heartbeat_result['new_status']}")
        
        assert heartbeat_result["status_changed"] == True, "Status should have changed"
        assert heartbeat_result["old_status"] == "pending_offline", f"Old status should be pending_offline, got {heartbeat_result['old_status']}"
        assert heartbeat_result["new_status"] == "online", f"New status should be online, got {heartbeat_result['new_status']}"
        
        online_events = get_events(db, device_id=device.device_id, event_type="online")
        print(f"  [4] ONLINE events: {len(online_events)}")
        assert len(online_events) >= 2, f"Should have at least 2 ONLINE events (initial + recovery), got {len(online_events)}"
        
        pending_events = get_events(db, device_id=device.device_id, event_type="pending_offline")
        print(f"  [5] PENDING_OFFLINE events: {len(pending_events)}")
        assert len(pending_events) == 1, f"Should have exactly 1 PENDING_OFFLINE event, got {len(pending_events)}"
        
        offline_events = get_events(db, device_id=device.device_id, event_type="offline")
        print(f"  [6] OFFLINE events: {len(offline_events)}")
        assert len(offline_events) == 0, f"Should have NO OFFLINE events, got {len(offline_events)}"
        
        print_test_result(True, f"Device recovered from PENDING_OFFLINE: PENDING_OFFLINE -> ONLINE")
        return True
        
    except AssertionError as e:
        logger.error(f"Test failed: {e}")
        print_test_result(False, str(e))
        return False
    except Exception as e:
        logger.error(f"Test error: {e}", exc_info=True)
        print_test_result(False, str(e))
        return False
    finally:
        db.close()


def test_5_pending_to_offline_timeout():
    """
    Test Case 5: Pending to Offline Timeout
    - Device goes ONLINE
    - Simulate 70 seconds (PENDING_OFFLINE)
    - Simulate total 130 seconds of inactivity
    - Verify: PENDING_OFFLINE -> OFFLINE
    """
    print_test_banner("Test 5: Pending -> Offline Timeout (130s total)")
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        
        simulate_heartbeat(db, device)
        assert device.status == DeviceStatus.ONLINE, "Should be ONLINE after heartbeat"
        print(f"  [1] After heartbeat: {device.status.value} OK")
        
        simulate_inactivity(db, device, seconds_ago=70)
        check_result_1 = check_device_status_machine(db, device)
        assert check_result_1["new_status"] == "pending_offline", f"Should be PENDING_OFFLINE at 70s"
        print(f"  [2] At 70s: PENDING_OFFLINE OK")
        
        simulate_inactivity(db, device, seconds_ago=130)
        check_result_2 = check_device_status_machine(db, device)
        print(f"  [3] After 130s inactivity check:")
        print(f"      - Time since active: {check_result_2['time_since_active']:.1f}s")
        print(f"      - Status changed: {check_result_2['status_changed']}")
        print(f"      - Old status: {check_result_2['old_status']}")
        print(f"      - New status: {check_result_2['new_status']}")
        
        assert check_result_2["status_changed"] == True, "Status should have changed"
        assert check_result_2["old_status"] == "pending_offline", f"Old status should be pending_offline, got {check_result_2['old_status']}"
        assert check_result_2["new_status"] == "offline", f"New status should be offline, got {check_result_2['new_status']}"
        
        pending_events = get_events(db, device_id=device.device_id, event_type="pending_offline")
        offline_events = get_events(db, device_id=device.device_id, event_type="offline")
        print(f"  [4] Events:")
        print(f"      - PENDING_OFFLINE: {len(pending_events)}")
        print(f"      - OFFLINE: {len(offline_events)}")
        
        assert len(pending_events) == 1, f"Should have 1 PENDING_OFFLINE event, got {len(pending_events)}"
        assert len(offline_events) == 1, f"Should have 1 OFFLINE event, got {len(offline_events)}"
        
        offline_event = offline_events[0]
        assert offline_event["details"].get("was_pending") == True, "OFFLINE event should have was_pending=True"
        print(f"  [5] OFFLINE event has was_pending=True OK")
        
        print_test_result(True, f"Device transitioned: ONLINE -> PENDING_OFFLINE -> OFFLINE")
        return True
        
    except AssertionError as e:
        logger.error(f"Test failed: {e}")
        print_test_result(False, str(e))
        return False
    except Exception as e:
        logger.error(f"Test error: {e}", exc_info=True)
        print_test_result(False, str(e))
        return False
    finally:
        db.close()


def test_6_no_activity_device():
    """
    Test Case 6: Device Without Any Activity
    - Device created with no heartbeat
    - Verify: Remains OFFLINE, no state transitions
    """
    print_test_banner("Test 6: Device Without Any Activity")
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        assert device.status == DeviceStatus.OFFLINE, "Initial status should be OFFLINE"
        print(f"  [1] Initial status: {device.status.value} OK")
        
        check_result = check_device_status_machine(db, device)
        print(f"  [2] Status check result:")
        print(f"      - Status changed: {check_result['status_changed']}")
        print(f"      - Current status: {check_result['status']}")
        print(f"      - Reason: {check_result['reason']}")
        
        assert check_result["status_changed"] == False, "Status should NOT change for device without activity"
        assert check_result["status"] == "offline", f"Should remain OFFLINE, got {check_result['status']}"
        
        all_events = get_events(db, device_id=device.device_id)
        print(f"  [3] Events recorded: {len(all_events)}")
        assert len(all_events) == 0, f"Should have NO events, got {len(all_events)}"
        
        print_test_result(True, f"Inactive device remains OFFLINE with no state transitions")
        return True
        
    except AssertionError as e:
        logger.error(f"Test failed: {e}")
        print_test_result(False, str(e))
        return False
    except Exception as e:
        logger.error(f"Test error: {e}", exc_info=True)
        print_test_result(False, str(e))
        return False
    finally:
        db.close()


def test_7_within_pending_threshold():
    """
    Test Case 7: Within Pending Threshold (30s)
    - Device goes ONLINE
    - Simulate 30 seconds of inactivity (within PENDING_OFFLINE threshold)
    - Verify: Remains ONLINE
    """
    print_test_banner("Test 7: Within Pending Threshold (30s)")
    
    db = SessionLocal()
    try:
        device = create_test_device(db)
        
        simulate_heartbeat(db, device)
        assert device.status == DeviceStatus.ONLINE, "Should be ONLINE after heartbeat"
        print(f"  [1] After heartbeat: {device.status.value} OK")
        
        simulate_inactivity(db, device, seconds_ago=30)
        
        check_result = check_device_status_machine(db, device)
        print(f"  [2] After 30s inactivity:")
        print(f"      - Time since active: {check_result['time_since_active']:.1f}s")
        print(f"      - Status changed: {check_result['status_changed']}")
        print(f"      - Current status: {check_result['status']}")
        
        assert check_result["status_changed"] == False, "Status should NOT change at 30s"
        assert check_result["status"] == "online", f"Should remain ONLINE, got {check_result['status']}"
        
        pending_events = get_events(db, device_id=device.device_id, event_type="pending_offline")
        offline_events = get_events(db, device_id=device.device_id, event_type="offline")
        print(f"  [3] Events:")
        print(f"      - PENDING_OFFLINE: {len(pending_events)}")
        print(f"      - OFFLINE: {len(offline_events)}")
        
        assert len(pending_events) == 0, f"Should have NO PENDING_OFFLINE events, got {len(pending_events)}"
        assert len(offline_events) == 0, f"Should have NO OFFLINE events, got {len(offline_events)}"
        
        print_test_result(True, f"Device remains ONLINE at 30s (within pending threshold)")
        return True
        
    except AssertionError as e:
        logger.error(f"Test failed: {e}")
        print_test_result(False, str(e))
        return False
    except Exception as e:
        logger.error(f"Test error: {e}", exc_info=True)
        print_test_result(False, str(e))
        return False
    finally:
        db.close()


def main():
    print("\n" + "=" * 70)
    print("  HEARTBEAT TIMEOUT STATE MACHINE TEST SUITE v2")
    print("=" * 70)
    print(f"\nTest Parameters:")
    print(f"  - PENDING_OFFLINE_THRESHOLD: {PENDING_OFFLINE_THRESHOLD} seconds (1 minute)")
    print(f"  - OFFLINE_THRESHOLD: {OFFLINE_THRESHOLD} seconds (2 minutes)")
    print()
    print("State Machine:")
    print("  ONLINE + 60s no heartbeat -> PENDING_OFFLINE")
    print("  PENDING_OFFLINE + heartbeat -> ONLINE (recovery)")
    print("  PENDING_OFFLINE + 120s total no heartbeat -> OFFLINE")
    print("  ONLINE + 120s no heartbeat -> OFFLINE (direct)")
    print()
    
    test_cases = [
        ("Test 1: Basic Offline Timeout (> 2 minutes)", test_1_basic_offline_timeout),
        ("Test 2: Pending Offline State (1-2 minutes)", test_2_pending_offline_state),
        ("Test 3: Critical Point Recovery (55s -> heartbeat)", test_3_critical_point_recovery),
        ("Test 4: Pending Offline Recovery (70s -> heartbeat)", test_4_pending_offline_recovery),
        ("Test 5: Pending -> Offline Timeout (130s total)", test_5_pending_to_offline_timeout),
        ("Test 6: Device Without Any Activity", test_6_no_activity_device),
        ("Test 7: Within Pending Threshold (30s)", test_7_within_pending_threshold),
    ]
    
    results = []
    
    for test_name, test_func in test_cases:
        success = test_func()
        results.append((test_name, success))
        time.sleep(0.1)
    
    print("\n" + "=" * 70)
    print("  TEST SUMMARY")
    print("=" * 70)
    
    passed = 0
    failed = 0
    
    for test_name, success in results:
        status = "[PASS]" if success else "[FAIL]"
        if success:
            passed += 1
        else:
            failed += 1
        print(f"  {status}: {test_name}")
    
    print()
    print(f"  Total: {len(results)} tests")
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")
    print()
    
    if failed == 0:
        print("  [SUCCESS] ALL TESTS PASSED!")
        print("  The state machine correctly handles:")
        print("    - Network jitter recovery at critical points")
        print("    - Pending offline intermediate state")
        print("    - Recovery from pending offline state")
        print("    - Proper timeout to offline state")
    else:
        print(f"  [WARNING] Some tests failed. Check logs for details: {LOG_FILE}")
    
    print("=" * 70)
    
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
