import sys
import os
import time
import uuid
import csv
import tracemalloc
import threading
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker, Session
from server.models import (
    Base, Device, DeviceDataHistory, ReportTask, ReportTaskStatus,
    ScheduledReportConfig, ScheduledReportType
)
from server.report_engine import (
    report_engine, ScheduledReportManager, REPORTS_DIR, EXPORT_BATCH_SIZE
)

TEST_RECORDS_COUNT = 100000
TEST_DEVICE_ID = f"test_export_{uuid.uuid4().hex[:8]}"
TEST_BATCH_SIZE = 1000

DATABASE_URL = "sqlite:///./test_report_db.db"

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_test_database():
    Base.metadata.create_all(bind=engine)
    print(f"[Test] Test database initialized: {DATABASE_URL}")

def cleanup_test_database():
    db_path = DATABASE_URL.replace("sqlite:///", "")
    if os.path.exists(db_path):
        os.unlink(db_path)
        print(f"[Test] Cleaned up test database: {db_path}")

def create_test_device(db: Session):
    device = Device(
        device_id=TEST_DEVICE_ID,
        secret_key=uuid.uuid4().hex,
        model="Test-Sensor-Export",
        status="online"
    )
    db.add(device)
    db.commit()
    db.refresh(device)
    print(f"[Test] Created test device: {TEST_DEVICE_ID}")
    return device

def generate_test_history_records(
    db: Session, 
    device_id: str, 
    count: int,
    batch_size: int = TEST_BATCH_SIZE
):
    print(f"\n[Test] Generating {count} test history records...")
    print(f"[Test] Using batch size: {batch_size}")
    
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
                    "record_index": records_generated,
                    "source": "test_export"
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
        
        if records_generated % 10000 == 0:
            print(f"[Test]   Generated {records_generated}/{count} records...")
    
    print(f"[Test] Completed generating {records_generated} records")
    return records_generated

def get_memory_usage():
    if tracemalloc.is_tracing():
        current, peak = tracemalloc.get_traced_memory()
        return current, peak
    return 0, 0

def test_streaming_export_memory_stability():
    print("\n" + "=" * 70)
    print("TEST 1: Streaming Export Memory Stability (yield generator pattern)")
    print("=" * 70)
    
    db = SessionLocal()
    try:
        tracemalloc.start()
        memory_samples = []
        
        output_path = REPORTS_DIR / f"test_streaming_{uuid.uuid4().hex[:8]}.csv"
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        
        print(f"\n[Test] Starting streaming export to: {output_path}")
        print(f"[Test] Records to export: {TEST_RECORDS_COUNT}")
        print(f"[Test] Batch size: {EXPORT_BATCH_SIZE}")
        
        start_time = time.time()
        initial_memory, _ = get_memory_usage()
        
        total_records = 0
        batch_count = 0
        
        def progress_callback(current: int, total: int, progress: int):
            nonlocal batch_count, memory_samples
            batch_count += 1
            current_mem, peak_mem = get_memory_usage()
            memory_samples.append(current_mem)
            
            if batch_count % 10 == 0:
                mem_mb = current_mem / (1024 * 1024)
                peak_mb = peak_mem / (1024 * 1024)
                print(f"[Test]   Progress: {progress}% ({current}/{total}), "
                      f"Memory: {mem_mb:.2f}MB (peak: {peak_mb:.2f}MB)")
        
        export_start_time = time.time()
        
        total_records, file_size = report_engine.export_to_csv_file(
            db=db,
            device_id=TEST_DEVICE_ID,
            output_path=output_path,
            include_payload=True,
            progress_callback=progress_callback
        )
        
        export_end_time = time.time()
        final_memory, peak_memory = get_memory_usage()
        
        tracemalloc.stop()
        
        elapsed_time = export_end_time - export_start_time
        initial_mem_mb = initial_memory / (1024 * 1024)
        final_mem_mb = final_memory / (1024 * 1024)
        peak_mem_mb = peak_memory / (1024 * 1024)
        memory_increase_mb = final_mem_mb - initial_mem_mb
        
        print(f"\n[Test] Export completed!")
        print(f"[Test] Total records exported: {total_records}")
        print(f"[Test] File size: {file_size / (1024 * 1024):.2f} MB")
        print(f"[Test] Time elapsed: {elapsed_time:.2f} seconds")
        print(f"[Test] Export rate: {total_records / elapsed_time:.0f} records/second")
        print(f"\n[Test] Memory Analysis:")
        print(f"[Test]   Initial memory: {initial_mem_mb:.2f} MB")
        print(f"[Test]   Final memory: {final_mem_mb:.2f} MB")
        print(f"[Test]   Peak memory: {peak_mem_mb:.2f} MB")
        print(f"[Test]   Memory increase: {memory_increase_mb:.2f} MB")
        
        if memory_samples:
            avg_memory = sum(memory_samples) / len(memory_samples)
            min_memory = min(memory_samples)
            max_memory = max(memory_samples)
            variance = max_memory - min_memory
            
            print(f"\n[Test] During Export Memory Samples:")
            print(f"[Test]   Average: {avg_memory / (1024 * 1024):.2f} MB")
            print(f"[Test]   Min: {min_memory / (1024 * 1024):.2f} MB")
            print(f"[Test]   Max: {max_memory / (1024 * 1024):.2f} MB")
            print(f"[Test]   Variance: {variance / (1024 * 1024):.2f} MB")
        
        passed = True
        if total_records != TEST_RECORDS_COUNT:
            print(f"\n[FAILED] Expected {TEST_RECORDS_COUNT} records, but got {total_records}")
            passed = False
        else:
            print(f"\n[PASSED] Record count verification: {total_records} records")
        
        if peak_mem_mb > 100:
            print(f"[WARNING] Peak memory usage ({peak_mem_mb:.2f}MB) exceeds 100MB threshold")
        else:
            print(f"[PASSED] Peak memory ({peak_mem_mb:.2f}MB) within acceptable range (< 100MB)")
        
        if variance > 20 * 1024 * 1024:
            print(f"[WARNING] Memory variance ({variance / (1024 * 1024):.2f}MB) exceeds 20MB")
        else:
            print(f"[PASSED] Memory stable, variance: {variance / (1024 * 1024):.2f}MB")
        
        return {
            "passed": passed,
            "total_records": total_records,
            "file_size": file_size,
            "elapsed_time": elapsed_time,
            "initial_memory_mb": initial_mem_mb,
            "final_memory_mb": final_mem_mb,
            "peak_memory_mb": peak_mem_mb,
            "output_path": output_path
        }
        
    finally:
        db.close()

def test_csv_file_integrity(output_path: Path, expected_records: int):
    print("\n" + "=" * 70)
    print("TEST 2: CSV File Integrity Verification")
    print("=" * 70)
    
    print(f"\n[Test] Verifying CSV file: {output_path}")
    
    if not output_path.exists():
        print(f"[FAILED] File does not exist: {output_path}")
        return False
    
    file_size = output_path.stat().st_size
    print(f"[Test] File size: {file_size / (1024 * 1024):.2f} MB")
    
    row_count = 0
    header_row = None
    sample_rows = []
    
    with open(output_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row in reader:
            if row_count == 0:
                header_row = row
                print(f"[Test] CSV Headers: {header_row}")
            else:
                if len(sample_rows) < 3:
                    sample_rows.append(row)
            row_count += 1
    
    data_row_count = row_count - 1
    
    print(f"\n[Test] Row count analysis:")
    print(f"[Test]   Total rows (including header): {row_count}")
    print(f"[Test]   Data rows (excluding header): {data_row_count}")
    print(f"[Test]   Expected data rows: {expected_records}")
    
    if data_row_count == expected_records:
        print(f"\n[PASSED] Data row count matches expected: {data_row_count}")
    else:
        print(f"\n[FAILED] Data row count mismatch! Expected {expected_records}, got {data_row_count}")
        return False
    
    print(f"\n[Test] Sample data rows (first 3):")
    for i, row in enumerate(sample_rows, 1):
        print(f"[Test]   Row {i}: {row[:5]}...")
    
    expected_headers = ["id", "device_id", "timestamp", "temperature", "humidity", "is_alert", "payload_json"]
    if header_row == expected_headers:
        print(f"\n[PASSED] CSV headers are correct")
    else:
        print(f"\n[WARNING] Headers differ from expected")
        print(f"[Test]   Expected: {expected_headers}")
        print(f"[Test]   Actual:   {header_row}")
    
    return True

def test_async_export_task():
    print("\n" + "=" * 70)
    print("TEST 3: Async Export Task (ReportTask Model)")
    print("=" * 70)
    
    db = SessionLocal()
    try:
        task_name = f"Async_Test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        print(f"\n[Test] Creating export task: {task_name}")
        
        task = report_engine.create_export_task(
            db=db,
            device_id=TEST_DEVICE_ID,
            task_name=task_name,
            include_payload=True,
            retention_hours=1
        )
        
        print(f"[Test] Task created: {task.id}")
        print(f"[Test] Initial status: {task.status.value}")
        
        task_status = report_engine.get_task_status(db, task.id)
        print(f"[Test] Task status: {task_status}")
        
        success = report_engine.start_export_task_async(db, task.id, SessionLocal)
        
        if success:
            print(f"[Test] Async task started successfully")
        else:
            print(f"[FAILED] Failed to start async task")
            return False
        
        print(f"\n[Test] Waiting for async task to complete...")
        max_wait = 120
        wait_interval = 2
        elapsed = 0
        
        while elapsed < max_wait:
            time.sleep(wait_interval)
            elapsed += wait_interval
            
            db.refresh(task)
            status = task.status
            
            progress_pct = f" ({task.progress}%)" if task.progress else ""
            print(f"[Test]   Waiting... {elapsed}s, Status: {status.value}{progress_pct}")
            
            if status in [ReportTaskStatus.COMPLETED, ReportTaskStatus.FAILED]:
                break
        
        db.refresh(task)
        
        if task.status == ReportTaskStatus.COMPLETED:
            print(f"\n[PASSED] Async task completed successfully!")
            print(f"[Test]   Record count: {task.record_count}")
            print(f"[Test]   File size: {task.file_size_bytes / 1024:.2f} KB")
            print(f"[Test]   File name: {task.file_name}")
            print(f"[Test]   Download URL: {task.download_url}")
            print(f"[Test]   Completed at: {task.completed_at}")
            
            if task.record_count == TEST_RECORDS_COUNT:
                print(f"[PASSED] Record count matches: {task.record_count}")
            else:
                print(f"[FAILED] Record count mismatch: expected {TEST_RECORDS_COUNT}, got {task.record_count}")
                return False
            
            return True
        else:
            print(f"\n[FAILED] Async task did not complete successfully")
            print(f"[Test]   Final status: {task.status.value}")
            if task.error_message:
                print(f"[Test]   Error: {task.error_message}")
            return False
            
    finally:
        db.close()

def test_report_task_model():
    print("\n" + "=" * 70)
    print("TEST 4: ReportTask Model CRUD Operations")
    print("=" * 70)
    
    db = SessionLocal()
    try:
        task = ReportTask(
            id=str(uuid.uuid4()),
            task_name="Model Test Task",
            device_id=TEST_DEVICE_ID,
            status=ReportTaskStatus.PENDING,
            progress=0,
            file_path="/test/path/test.csv",
            file_name="test.csv",
            export_format="csv",
            filters={"include_payload": True},
            expire_at=datetime.utcnow() + timedelta(hours=24)
        )
        
        db.add(task)
        db.commit()
        db.refresh(task)
        
        print(f"[Test] Created ReportTask: {task.id}")
        print(f"[Test]   Status: {task.status.value}")
        print(f"[Test]   Created at: {task.created_at}")
        
        task.status = ReportTaskStatus.PROCESSING
        task.progress = 50
        db.commit()
        db.refresh(task)
        
        print(f"[Test] Updated ReportTask:")
        print(f"[Test]   Status: {task.status.value}")
        print(f"[Test]   Progress: {task.progress}%")
        
        queried = db.query(ReportTask).filter(ReportTask.id == task.id).first()
        assert queried.id == task.id
        assert queried.status == ReportTaskStatus.PROCESSING
        assert queried.progress == 50
        print(f"[PASSED] Query verification passed")
        
        db.delete(task)
        db.commit()
        
        deleted = db.query(ReportTask).filter(ReportTask.id == task.id).first()
        assert deleted is None
        print(f"[PASSED] Delete verification passed")
        
        return True
        
    except Exception as e:
        print(f"[FAILED] Error: {e}")
        return False
    finally:
        db.close()

def test_scheduled_report_config():
    print("\n" + "=" * 70)
    print("TEST 5: ScheduledReportConfig Model (Daily Briefing)")
    print("=" * 70)
    
    db = SessionLocal()
    try:
        existing = db.query(ScheduledReportConfig).filter(
            ScheduledReportConfig.report_type == ScheduledReportType.DAILY_BRIEFING
        ).first()
        
        if existing:
            print(f"[Test] Found existing daily briefing config: {existing.name}")
        else:
            config = ScheduledReportConfig(
                id=str(uuid.uuid4()),
                name="每日运行简报",
                description="每日 8 点自动生成昨日设备运行简报",
                report_type=ScheduledReportType.DAILY_BRIEFING,
                cron_expression="0 8 * * *",
                output_directory="./reports/daily",
                retention_days=7,
                file_name_template="daily_briefing_{date}.csv",
                is_active=True
            )
            
            db.add(config)
            db.commit()
            db.refresh(config)
            
            print(f"[Test] Created ScheduledReportConfig: {config.id}")
            print(f"[Test]   Name: {config.name}")
            print(f"[Test]   Report Type: {config.report_type.value}")
            print(f"[Test]   Cron Expression: {config.cron_expression}")
            print(f"[Test]   Retention Days: {config.retention_days}")
            print(f"[Test]   Is Active: {config.is_active}")
        
        all_configs = db.query(ScheduledReportConfig).all()
        print(f"\n[Test] Total scheduled report configs: {len(all_configs)}")
        
        for config in all_configs:
            print(f"[Test]   - {config.name} ({config.report_type.value}): cron='{config.cron_expression}'")
        
        print(f"\n[PASSED] ScheduledReportConfig model test passed")
        return True
        
    except Exception as e:
        print(f"[FAILED] Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

def main():
    print("\n" + "#" * 70)
    print("#  Report Engine Test Suite")
    print("#" * 70)
    print(f"\n[Test] Starting at: {datetime.utcnow()}")
    print(f"[Test] Test records count: {TEST_RECORDS_COUNT}")
    print(f"[Test] Test device ID: {TEST_DEVICE_ID}")
    
    try:
        init_test_database()
        
        db = SessionLocal()
        try:
            create_test_device(db)
            generate_test_history_records(db, TEST_DEVICE_ID, TEST_RECORDS_COUNT)
            
            record_count = db.query(DeviceDataHistory).filter(
                DeviceDataHistory.device_id == TEST_DEVICE_ID
            ).count()
            
            print(f"\n[Test] Verification: {record_count} history records in database")
            
            if record_count != TEST_RECORDS_COUNT:
                print(f"[WARNING] Record count mismatch: expected {TEST_RECORDS_COUNT}, got {record_count}")
        finally:
            db.close()
        
        results = []
        
        export_result = test_streaming_export_memory_stability()
        results.append(("Streaming Export Memory", export_result["passed"]))
        
        csv_passed = test_csv_file_integrity(
            export_result["output_path"], 
            TEST_RECORDS_COUNT
        )
        results.append(("CSV File Integrity", csv_passed))
        
        async_passed = test_async_export_task()
        results.append(("Async Export Task", async_passed))
        
        model_passed = test_report_task_model()
        results.append(("ReportTask Model CRUD", model_passed))
        
        scheduled_passed = test_scheduled_report_config()
        results.append(("ScheduledReportConfig Model", scheduled_passed))
        
        print("\n" + "#" * 70)
        print("#  TEST SUMMARY")
        print("#" * 70)
        
        all_passed = True
        for test_name, passed in results:
            status = "[PASSED]" if passed else "[FAILED]"
            print(f"\n{status} {test_name}")
            if not passed:
                all_passed = False
        
        if all_passed:
            print("\n" + "=" * 70)
            print("ALL TESTS PASSED!")
            print("=" * 70)
            print(f"\n[Test] Performance Summary:")
            print(f"[Test]   Records exported: {TEST_RECORDS_COUNT}")
            print(f"[Test]   Time elapsed: {export_result['elapsed_time']:.2f}s")
            print(f"[Test]   Export rate: {TEST_RECORDS_COUNT / export_result['elapsed_time']:.0f} records/sec")
            print(f"[Test]   Peak memory: {export_result['peak_memory_mb']:.2f} MB")
            print(f"[Test]   File size: {export_result['file_size'] / (1024*1024):.2f} MB")
        else:
            print("\n" + "=" * 70)
            print("SOME TESTS FAILED!")
            print("=" * 70)
        
        if export_result["output_path"].exists():
            print(f"\n[Test] Test output file kept for inspection: {export_result['output_path']}")
        
        return all_passed
        
    finally:
        pass

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
