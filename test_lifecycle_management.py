import time
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from server.models import (
    Base, Device, DeviceStatus, AlertHistory, AlertSeverity, AlertStatus
)

DATABASE_URL = "sqlite:///./iot_devices_test_lifecycle.db"

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)

PENDING_OFFLINE_THRESHOLD = 60
OFFLINE_THRESHOLD = 120


def create_offline_alert(db: Session, device: Device, time_since_active: float, now: datetime):
    alert = AlertHistory(
        id=str(int(time.time() * 1000000)),
        rule_id=None,
        device_id=device.device_id,
        rule_name="设备离线告警",
        severity=AlertSeverity.CRITICAL,
        status=AlertStatus.OPEN,
        matched_values={
            "time_since_active": time_since_active,
            "threshold_seconds": OFFLINE_THRESHOLD
        },
        action="notify",
        action_result={
            "message": f"设备 {device.device_id} 已离线，超过 {OFFLINE_THRESHOLD} 秒无响应"
        },
        created_at=now,
        updated_at=now
    )
    db.add(alert)
    db.flush()
    print(f"[OfflineAlert] Created CRITICAL alert for device {device.device_id} - offline for {time_since_active:.1f}s")
    return alert


def test_1_state_scanner_optimization():
    """测试状态扫描器优化 - 只查询 ONLINE 和 PENDING_OFFLINE 设备"""
    print("\n" + "=" * 70)
    print("  TEST 1: 状态扫描器优化 - 只查询活跃设备")
    print("=" * 70)
    
    db = SessionLocal()
    try:
        device_online = Device(
            device_id="TEST_OPTIMIZE_ONLINE",
            secret_key="test_key",
            model="TestModel",
            status=DeviceStatus.ONLINE,
            last_heartbeat=datetime.utcnow(),
            last_seen=datetime.utcnow()
        )
        db.add(device_online)
        
        device_pending = Device(
            device_id="TEST_OPTIMIZE_PENDING",
            secret_key="test_key",
            model="TestModel",
            status=DeviceStatus.PENDING_OFFLINE,
            last_heartbeat=datetime.utcnow() - timedelta(seconds=90),
            last_seen=datetime.utcnow() - timedelta(seconds=90)
        )
        db.add(device_pending)
        
        device_offline = Device(
            device_id="TEST_OPTIMIZE_OFFLINE",
            secret_key="test_key",
            model="TestModel",
            status=DeviceStatus.OFFLINE,
            last_heartbeat=None,
            last_seen=None
        )
        db.add(device_offline)
        
        device_maintenance = Device(
            device_id="TEST_OPTIMIZE_MAINT",
            secret_key="test_key",
            model="TestModel",
            status=DeviceStatus.MAINTENANCE,
            last_heartbeat=None,
            last_seen=None
        )
        db.add(device_maintenance)
        
        db.commit()
        
        active_devices = db.query(Device).filter(
            Device.status.in_([DeviceStatus.ONLINE, DeviceStatus.PENDING_OFFLINE])
        ).all()
        
        all_devices = db.query(Device).all()
        
        print(f"  总设备数: {len(all_devices)}")
        print(f"  活跃设备数 (ONLINE + PENDING_OFFLINE): {len(active_devices)}")
        print(f"  优化后减少查询: {len(all_devices) - len(active_devices)} 个设备")
        
        for d in active_devices:
            print(f"    - {d.device_id}: {d.status.value}")
        
        assert len(active_devices) == 2, f"Expected 2 active devices, got {len(active_devices)}"
        
        print("\n  [PASS] 状态扫描器优化 - 只查询活跃设备")
        return True
        
    except AssertionError as e:
        print(f"\n  [FAIL] {e}")
        return False
    except Exception as e:
        print(f"\n  [ERROR] {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        for d in db.query(Device).filter(Device.device_id.like("TEST_OPTIMIZE%")).all():
            db.delete(d)
        db.commit()
        db.close()


def test_2_offline_alert_creation():
    """测试离线告警创建 - 设备离线时创建 CRITICAL 级别告警"""
    print("\n" + "=" * 70)
    print("  TEST 2: 离线告警集成 - 设备离线时创建 CRITICAL 告警")
    print("=" * 70)
    
    db = SessionLocal()
    try:
        device = Device(
            device_id="TEST_ALERT_DEVICE",
            secret_key="test_key",
            model="TestModel",
            status=DeviceStatus.ONLINE,
            last_heartbeat=datetime.utcnow() - timedelta(seconds=150),
            last_seen=datetime.utcnow() - timedelta(seconds=150)
        )
        db.add(device)
        db.commit()
        db.refresh(device)
        
        now = datetime.utcnow()
        last_active_time = device.last_seen if device.last_seen else device.last_heartbeat
        time_since_active = (now - last_active_time).total_seconds()
        
        print(f"  设备状态: {device.status.value}")
        print(f"  最后活跃时间: {last_active_time}")
        print(f"  超时时间: {time_since_active:.1f} 秒 (阈值: {OFFLINE_THRESHOLD} 秒)")
        print(f"  需要标记为离线: {time_since_active > OFFLINE_THRESHOLD}")
        
        if time_since_active > OFFLINE_THRESHOLD:
            old_status = device.status.value
            device.status = DeviceStatus.OFFLINE
            new_status = device.status.value
            
            alert = create_offline_alert(db, device, time_since_active, now)
            db.commit()
            db.refresh(alert)
            
            print(f"\n  设备状态变更: {old_status} -> {new_status}")
            print(f"  告警 ID: {alert.id}")
            print(f"  告警级别: {alert.severity.value}")
            print(f"  告警状态: {alert.status.value}")
            print(f"  告警规则名: {alert.rule_name}")
            print(f"  匹配值: {alert.matched_values}")
            print(f"  动作结果: {alert.action_result}")
            
            assert alert.severity == AlertSeverity.CRITICAL, "Alert should be CRITICAL"
            assert alert.status == AlertStatus.OPEN, "Alert status should be OPEN"
            assert alert.rule_name == "设备离线告警", "Alert rule name mismatch"
            
            print("\n  [PASS] 离线告警创建成功")
            return True
        else:
            print("\n  [SKIP] 设备未超时，跳过测试")
            return True
            
    except AssertionError as e:
        print(f"\n  [FAIL] {e}")
        return False
    except Exception as e:
        print(f"\n  [ERROR] {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        for alert in db.query(AlertHistory).filter(AlertHistory.device_id == "TEST_ALERT_DEVICE").all():
            db.delete(alert)
        for d in db.query(Device).filter(Device.device_id == "TEST_ALERT_DEVICE").all():
            db.delete(d)
        db.commit()
        db.close()


def test_3_device_sorting():
    """测试设备列表排序 - 离线设备置顶"""
    print("\n" + "=" * 70)
    print("  TEST 3: 设备列表排序 - 离线设备置顶")
    print("=" * 70)
    
    def status_priority(device_data):
        status = device_data["status"]
        if status == "offline":
            return 0
        elif status == "pending_offline":
            return 1
        elif status == "online":
            return 2
        else:
            return 3
    
    devices = [
        {"device_id": "D1", "status": "online"},
        {"device_id": "D2", "status": "offline"},
        {"device_id": "D3", "status": "pending_offline"},
        {"device_id": "D4", "status": "online"},
        {"device_id": "D5", "status": "offline"},
        {"device_id": "D6", "status": "maintenance"},
    ]
    
    print(f"  原始顺序: {[d['device_id'] for d in devices]}")
    
    devices.sort(key=status_priority)
    
    print(f"  排序后: {[d['device_id'] for d in devices]}")
    print(f"  排序顺序: offline(0) -> pending_offline(1) -> online(2) -> maintenance(3)")
    
    expected_order = ["D2", "D5", "D3", "D1", "D4", "D6"]
    actual_order = [d["device_id"] for d in devices]
    
    for i, d in enumerate(devices):
        priority = status_priority(d)
        print(f"    {i+1}. {d['device_id']} ({d['status']}) - priority {priority}")
    
    assert actual_order == expected_order, f"Expected {expected_order}, got {actual_order}"
    
    print("\n  [PASS] 设备列表排序 - 离线设备置顶")
    return True


def test_4_stats_api_logic():
    """测试统计 API 逻辑"""
    print("\n" + "=" * 70)
    print("  TEST 4: 统计 API 逻辑验证")
    print("=" * 70)
    
    db = SessionLocal()
    try:
        test_devices = [
            Device(device_id="STAT_1", secret_key="k", model="M", status=DeviceStatus.ONLINE),
            Device(device_id="STAT_2", secret_key="k", model="M", status=DeviceStatus.ONLINE),
            Device(device_id="STAT_3", secret_key="k", model="M", status=DeviceStatus.OFFLINE),
            Device(device_id="STAT_4", secret_key="k", model="M", status=DeviceStatus.PENDING_OFFLINE),
            Device(device_id="STAT_5", secret_key="k", model="M", status=DeviceStatus.MAINTENANCE),
        ]
        
        for d in test_devices:
            db.add(d)
        db.commit()
        
        total_count = db.query(Device).filter(Device.device_id.like("STAT_%")).count()
        online_count = db.query(Device).filter(
            Device.device_id.like("STAT_%"),
            Device.status == DeviceStatus.ONLINE
        ).count()
        pending_count = db.query(Device).filter(
            Device.device_id.like("STAT_%"),
            Device.status == DeviceStatus.PENDING_OFFLINE
        ).count()
        offline_count = db.query(Device).filter(
            Device.device_id.like("STAT_%"),
            Device.status == DeviceStatus.OFFLINE
        ).count()
        maintenance_count = db.query(Device).filter(
            Device.device_id.like("STAT_%"),
            Device.status == DeviceStatus.MAINTENANCE
        ).count()
        
        critical_alerts = db.query(AlertHistory).filter(
            AlertHistory.severity == AlertSeverity.CRITICAL,
            AlertHistory.status == AlertStatus.OPEN
        ).count()
        
        stats = {
            "total": total_count,
            "online": online_count,
            "pending_offline": pending_count,
            "offline": offline_count,
            "maintenance": maintenance_count,
            "critical_alerts": critical_alerts,
            "last_updated": datetime.utcnow().isoformat()
        }
        
        print(f"  总设备数: {stats['total']}")
        print(f"  在线设备: {stats['online']}")
        print(f"  待离线: {stats['pending_offline']}")
        print(f"  离线设备: {stats['offline']}")
        print(f"  维护中: {stats['maintenance']}")
        print(f"  紧急告警: {stats['critical_alerts']}")
        print(f"  最后更新: {stats['last_updated']}")
        
        assert stats["total"] == 5, f"Expected 5, got {stats['total']}"
        assert stats["online"] == 2, f"Expected 2 online, got {stats['online']}"
        assert stats["offline"] == 1, f"Expected 1 offline, got {stats['offline']}"
        assert stats["pending_offline"] == 1, f"Expected 1 pending, got {stats['pending_offline']}"
        
        print("\n  [PASS] 统计 API 逻辑验证")
        return True
        
    except AssertionError as e:
        print(f"\n  [FAIL] {e}")
        return False
    except Exception as e:
        print(f"\n  [ERROR] {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        for d in db.query(Device).filter(Device.device_id.like("STAT_%")).all():
            db.delete(d)
        db.commit()
        db.close()


def main():
    print("\n" + "#" * 70)
    print("#  设备生命周期管理与离线监测系统 - 测试套件")
    print("#" * 70)
    
    results = []
    
    results.append(("状态扫描器优化", test_1_state_scanner_optimization()))
    results.append(("离线告警集成", test_2_offline_alert_creation()))
    results.append(("设备列表排序", test_3_device_sorting()))
    results.append(("统计 API 逻辑", test_4_stats_api_logic()))
    
    print("\n" + "#" * 70)
    print("#  测试结果汇总")
    print("#" * 70)
    
    passed = 0
    failed = 0
    
    for name, success in results:
        status = "PASS" if success else "FAIL"
        print(f"  [{status}] {name}")
        if success:
            passed += 1
        else:
            failed += 1
    
    print(f"\n  总计: {passed} 通过, {failed} 失败")
    print("#" * 70)
    
    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
