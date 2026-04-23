import sys
import os
import time
import uuid
import csv
import tracemalloc
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session
from server.models import (
    Base, Device, DeviceStatus, DeviceDataHistory, ReportTask, ReportTaskStatus,
    ScheduledReportConfig, ScheduledReportType
)
from server.report_engine import (
    report_engine, ScheduledReportManager, REPORTS_DIR, EXPORT_BATCH_SIZE
)

TEST_RECORDS_COUNT = 10000
TEST_DEVICE_ID = f"test_quick_{uuid.uuid4().hex[:8]}"
TEST_BATCH_SIZE = 1000

DATABASE_URL = "sqlite:///./test_quick_report.db"

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_test_database():
    Base.metadata.create_all(bind=engine)
    print(f"[QuickTest] Test database initialized: {DATABASE_URL}")

def create_test_device(db: Session):
    device = Device(
        device_id=TEST_DEVICE_ID,
        secret_key=uuid.uuid4().hex,
        model="Test-Sensor-Quick",
        status=DeviceStatus.ONLINE
    )
    db.add(device)
    db.commit()
    db.refresh(device)
    print(f"[QuickTest] Created test device: {TEST_DEVICE_ID}")
    return device

def generate_test_history_records(
    db: Session, 
    device_id: str, 
    count: int,
    batch_size: int = TEST_BATCH_SIZE
):
    print(f"\n[QuickTest] Generating {count} test history records...")
    
    base_time = datetime.utcnow() - timedelta(hours=count // 100)
    records_generated = 0
    
    for batch_start in range(0, count, batch_size):
        batch_records = []
        for i in range(batch_size):
            if records_generated >= count:
                break
            
            record_time = base_time + timedelta(seconds=records_generated)
            temperature = 20.0 + (records_generated % 100) * 0.5
            humidity = 40.0 + (records_generated % 80) * 0.75
            is_alert = temperature > 50.0
            
            history_record = DeviceDataHistory(
                id=str(uuid.uuid4()),
                device_id=device_id,
                payload={
                    "temperature": temperature,
                    "humidity": humidity,
                    "record_index": records_generated
                },
                timestamp=record_time,
                temperature=temperature,
                humidity=humidity,
                is_alert=is_alert
            )
            batch_records.append(history_record)
            records_generated += 1
        
        db.add_all(batch_records)
        db.commit()
        
        if records_generated % 2000 == 0:
            print(f"[QuickTest]   Generated {records_generated}/{count} records...")
    
    print(f"[QuickTest] Completed generating {records_generated} records")
    return records_generated

def test_streaming_export():
    print("\n" + "=" * 60)
    print("TEST 1: Streaming Export with Yield Generator")
    print("=" * 60)
    
    db = SessionLocal()
    try:
        tracemalloc.start()
        
        output_path = REPORTS_DIR / f"quick_test_{uuid.uuid4().hex[:8]}.csv"
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        
        print(f"\n[QuickTest] Exporting to: {output_path}")
        print(f"[QuickTest] Batch size: {EXPORT_BATCH_SIZE}")
        
        initial_memory, _ = tracemalloc.get_traced_memory()
        start_time = time.time()
        
        memory_samples = []
        
        def progress_callback(current: int, total: int, progress: int):
            current_mem, _ = tracemalloc.get_traced_memory()
            memory_samples.append(current_mem)
            if progress % 25 == 0:
                mem_mb = current_mem / (1024 * 1024)
                print(f"[QuickTest]   Progress: {progress}% ({current}/{total}), Memory: {mem_mb:.2f}MB")
        
        total_records, file_size = report_engine.export_to_csv_file(
            db=db,
            device_id=TEST_DEVICE_ID,
            output_path=output_path,
            include_payload=True,
            progress_callback=progress_callback
        )
        
        elapsed_time = time.time() - start_time
        final_memory, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        initial_mem_mb = initial_memory / (1024 * 1024)
        final_mem_mb = final_memory / (1024 * 1024)
        peak_mem_mb = peak_memory / (1024 * 1024)
        memory_increase = final_mem_mb - initial_mem_mb
        
        print(f"\n[QuickTest] Export Results:")
        print(f"[QuickTest]   Total records: {total_records}")
        print(f"[QuickTest]   File size: {file_size / 1024:.2f} KB")
        print(f"[QuickTest]   Time: {elapsed_time:.2f}s")
        print(f"[QuickTest]   Rate: {total_records / elapsed_time:.0f} records/sec")
        print(f"\n[QuickTest] Memory Analysis:")
        print(f"[QuickTest]   Initial: {initial_mem_mb:.2f} MB")
        print(f"[QuickTest]   Final: {final_mem_mb:.2f} MB")
        print(f"[QuickTest]   Peak: {peak_mem_mb:.2f} MB")
        print(f"[QuickTest]   Increase: {memory_increase:.2f} MB")
        
        if memory_samples:
            avg_memory = sum(memory_samples) / len(memory_samples)
            max_memory = max(memory_samples)
            min_memory = min(memory_samples)
            variance = max_memory - min_memory
            print(f"\n[QuickTest] During Export:")
            print(f"[QuickTest]   Average: {avg_memory / (1024 * 1024):.2f} MB")
            print(f"[QuickTest]   Variance: {variance / (1024 * 1024):.2f} MB")
        
        passed = True
        if total_records != TEST_RECORDS_COUNT:
            print(f"\n[FAILED] Expected {TEST_RECORDS_COUNT}, got {total_records}")
            passed = False
        else:
            print(f"\n[PASSED] Record count: {total_records}")
        
        if peak_mem_mb > 50:
            print(f"[WARNING] Peak memory ({peak_mem_mb:.2f}MB) exceeds 50MB")
        else:
            print(f"[PASSED] Peak memory ({peak_mem_mb:.2f}MB) within acceptable range")
        
        return passed, output_path, total_records
        
    finally:
        db.close()

def test_csv_integrity(output_path: Path, expected_records: int):
    print("\n" + "=" * 60)
    print("TEST 2: CSV File Integrity")
    print("=" * 60)
    
    if not output_path.exists():
        print(f"[FAILED] File not found: {output_path}")
        return False
    
    row_count = 0
    with open(output_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            row_count += 1
    
    data_rows = row_count - 1
    
    print(f"\n[QuickTest] Total rows: {row_count}")
    print(f"[QuickTest] Data rows: {data_rows}")
    print(f"[QuickTest] Expected: {expected_records}")
    
    if data_rows == expected_records:
        print(f"\n[PASSED] Row count matches")
        return True
    else:
        print(f"\n[FAILED] Row count mismatch: expected {expected_records}, got {data_rows}")
        return False

def test_report_task_crud():
    print("\n" + "=" * 60)
    print("TEST 3: ReportTask Model CRUD")
    print("=" * 60)
    
    db = SessionLocal()
    try:
        task = ReportTask(
            id=str(uuid.uuid4()),
            task_name="Quick Test Task",
            device_id=TEST_DEVICE_ID,
            status=ReportTaskStatus.PENDING,
            export_format="csv",
            expire_at=datetime.utcnow() + timedelta(hours=24)
        )
        
        db.add(task)
        db.commit()
        db.refresh(task)
        
        print(f"[QuickTest] Created task: {task.id}")
        
        task.status = ReportTaskStatus.PROCESSING
        task.progress = 50
        db.commit()
        db.refresh(task)
        
        print(f"[QuickTest] Updated task: status={task.status.value}, progress={task.progress}%")
        
        queried = db.query(ReportTask).filter(ReportTask.id == task.id).first()
        assert queried.id == task.id
        print(f"[PASSED] Query verification")
        
        db.delete(task)
        db.commit()
        
        deleted = db.query(ReportTask).filter(ReportTask.id == task.id).first()
        assert deleted is None
        print(f"[PASSED] Delete verification")
        
        return True
        
    except Exception as e:
        print(f"[FAILED] Error: {e}")
        return False
    finally:
        db.close()

def test_scheduled_config():
    print("\n" + "=" * 60)
    print("TEST 4: ScheduledReportConfig (Daily Briefing)")
    print("=" * 60)
    
    db = SessionLocal()
    try:
        config = ScheduledReportConfig(
            id=str(uuid.uuid4()),
            name="每日运行简报",
            description="每日 8 点自动生成昨日设备运行简报",
            report_type=ScheduledReportType.DAILY_BRIEFING,
            cron_expression="0 8 * * *",
            output_directory="./reports/daily",
            retention_days=7,
            is_active=True
        )
        
        db.add(config)
        db.commit()
        db.refresh(config)
        
        print(f"[QuickTest] Created config: {config.name}")
        print(f"[QuickTest]   Report Type: {config.report_type.value}")
        print(f"[QuickTest]   Cron: {config.cron_expression}")
        print(f"[QuickTest]   Retention: {config.retention_days} days")
        print(f"[QuickTest]   Active: {config.is_active}")
        
        print(f"\n[PASSED] ScheduledReportConfig created successfully")
        return True
        
    except Exception as e:
        print(f"[FAILED] Error: {e}")
        return False
    finally:
        db.close()

def main():
    print("\n" + "#" * 60)
    print("#  Quick Report Engine Test")
    print("#" * 60)
    print(f"\n[QuickTest] Starting at: {datetime.utcnow()}")
    print(f"[QuickTest] Test records: {TEST_RECORDS_COUNT}")
    
    try:
        init_test_database()
        
        db = SessionLocal()
        try:
            create_test_device(db)
            generate_test_history_records(db, TEST_DEVICE_ID, TEST_RECORDS_COUNT)
            
            count = db.query(DeviceDataHistory).filter(
                DeviceDataHistory.device_id == TEST_DEVICE_ID
            ).count()
            print(f"\n[QuickTest] Database has {count} records")
        finally:
            db.close()
        
        results = []
        
        export_passed, output_path, total_records = test_streaming_export()
        results.append(("Streaming Export", export_passed))
        
        csv_passed = test_csv_integrity(output_path, TEST_RECORDS_COUNT)
        results.append(("CSV Integrity", csv_passed))
        
        crud_passed = test_report_task_crud()
        results.append(("ReportTask CRUD", crud_passed))
        
        scheduled_passed = test_scheduled_config()
        results.append(("Scheduled Config", scheduled_passed))
        
        print("\n" + "#" * 60)
        print("#  TEST SUMMARY")
        print("#" * 60)
        
        all_passed = True
        for test_name, passed in results:
            status = "[PASSED]" if passed else "[FAILED]"
            print(f"\n{status} {test_name}")
            if not passed:
                all_passed = False
        
        if all_passed:
            print("\n" + "=" * 60)
            print("ALL QUICK TESTS PASSED!")
            print("=" * 60)
            print(f"\n[QuickTest] Full test (100K records) available in test_report_engine.py")
        else:
            print("\n" + "=" * 60)
            print("SOME TESTS FAILED")
            print("=" * 60)
        
        if output_path.exists():
            print(f"\n[QuickTest] Test output: {output_path}")
        
        return all_passed
        
    finally:
        db_path = DATABASE_URL.replace("sqlite:///", "")
        if os.path.exists(db_path):
            try:
                os.unlink(db_path)
                print(f"\n[QuickTest] Cleaned up test database")
            except:
                pass

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
