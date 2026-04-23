import sqlite3
import json
from datetime import datetime, UTC

DB_PATH = "./iot_devices.db"

def migrate_audit_logs_table():
    print("=" * 60)
    print("  Migrating audit_logs table to new schema")
    print("=" * 60)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='audit_logs'
        """)
        if not cursor.fetchone():
            print("[INFO] audit_logs table does not exist, no migration needed")
            conn.close()
            return True
        
        cursor.execute("PRAGMA table_info(audit_logs)")
        columns = [row[1] for row in cursor.fetchall()]
        print(f"[INFO] Current columns: {columns}")
        
        if 'changed_fields' in columns:
            print("[INFO] changed_fields column already exists, no migration needed")
            conn.close()
            return True
        
        print("\n[INFO] Starting migration...")
        
        print("[INFO] Renaming old table to audit_logs_old...")
        cursor.execute("ALTER TABLE audit_logs RENAME TO audit_logs_old")
        
        print("[INFO] Creating new audit_logs table with correct schema...")
        cursor.execute("""
            CREATE TABLE audit_logs (
                id VARCHAR(36) NOT NULL,
                operation_type VARCHAR(64) NOT NULL,
                operation_desc VARCHAR(500),
                user_id VARCHAR(36),
                device_id VARCHAR(64),
                ip_address VARCHAR(64),
                risk_level VARCHAR(16),
                changed_fields JSON,
                status VARCHAR(32),
                error_message VARCHAR(1000),
                created_at DATETIME,
                PRIMARY KEY (id)
            )
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_audit_logs_operation_type ON audit_logs (operation_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_audit_logs_user_id ON audit_logs (user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_audit_logs_device_id ON audit_logs (device_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_audit_logs_risk_level ON audit_logs (risk_level)")
        cursor.execute("CREATE INDEX IF NOT EXISTS ix_audit_logs_created_at ON audit_logs (created_at)")
        
        print("[INFO] Migrating existing data...")
        cursor.execute("""
            SELECT id, operation_type, operation_desc, user_id, device_id, 
                   ip_address, risk_level, old_values, new_values, changes,
                   status, error_message, created_at
            FROM audit_logs_old
        """)
        
        rows = cursor.fetchall()
        print(f"[INFO] Found {len(rows)} existing audit logs to migrate")
        
        migrated_count = 0
        for row in rows:
            (log_id, op_type, op_desc, user_id, device_id, 
             ip_addr, risk_level, old_values_str, new_values_str, changes_str,
             status, error_msg, created_at) = row
            
            changed_fields = {}
            
            if changes_str:
                try:
                    changes = json.loads(changes_str)
                    
                    if changes.get("modified"):
                        for key, diff in changes["modified"].items():
                            changed_fields[key] = [diff.get("old"), diff.get("new")]
                    
                    if changes.get("added"):
                        for key, value in changes["added"].items():
                            changed_fields[key] = [None, value]
                    
                    if changes.get("removed"):
                        for key, value in changes["removed"].items():
                            changed_fields[key] = [value, None]
                            
                except json.JSONDecodeError:
                    pass
            
            if not changed_fields:
                old_vals = None
                new_vals = None
                
                if old_values_str:
                    try:
                        old_vals = json.loads(old_values_str)
                    except json.JSONDecodeError:
                        pass
                
                if new_values_str:
                    try:
                        new_vals = json.loads(new_values_str)
                    except json.JSONDecodeError:
                        pass
                
                if old_vals or new_vals:
                    old_vals = old_vals or {}
                    new_vals = new_vals or {}
                    
                    all_keys = set(old_vals.keys()) | set(new_vals.keys())
                    for key in all_keys:
                        if key not in old_vals:
                            changed_fields[key] = [None, new_vals[key]]
                        elif key not in new_vals:
                            changed_fields[key] = [old_vals[key], None]
                        elif old_vals[key] != new_vals[key]:
                            changed_fields[key] = [old_vals[key], new_vals[key]]
            
            changed_fields_str = json.dumps(changed_fields) if changed_fields else None
            
            cursor.execute("""
                INSERT INTO audit_logs (
                    id, operation_type, operation_desc, user_id, device_id,
                    ip_address, risk_level, changed_fields, status, error_message, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                log_id, op_type, op_desc, user_id, device_id,
                ip_addr, risk_level, changed_fields_str, status, error_msg, created_at
            ))
            migrated_count += 1
        
        print(f"[INFO] Migrated {migrated_count} records")
        
        print("[INFO] Dropping old table...")
        cursor.execute("DROP TABLE audit_logs_old")
        
        conn.commit()
        print("\n[SUCCESS] Migration completed successfully!")
        print(f"  - Migrated {migrated_count} audit log records")
        print("  - Old columns (old_values, new_values, changes) replaced with changed_fields")
        print("  - changed_fields format: {\"field_name\": [old_value, new_value]}")
        
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        conn.close()
    
    return True

def verify_migration():
    print("\n" + "=" * 60)
    print("  Verifying migration")
    print("=" * 60)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("PRAGMA table_info(audit_logs)")
        columns = [row[1] for row in cursor.fetchall()]
        print(f"Columns: {columns}")
        
        assert 'changed_fields' in columns, "changed_fields column should exist"
        assert 'old_values' not in columns, "old_values column should be removed"
        assert 'new_values' not in columns, "new_values column should be removed"
        assert 'changes' not in columns, "changes column should be removed"
        
        cursor.execute("SELECT COUNT(*) FROM audit_logs")
        count = cursor.fetchone()[0]
        print(f"Total audit logs: {count}")
        
        print("\n[PASS] Migration verification passed!")
        return True
        
    except AssertionError as e:
        print(f"[FAIL] Verification failed: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Verification error: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    print(f"Migration started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Database: {DB_PATH}")
    
    if migrate_audit_logs_table():
        verify_migration()
    
    print("\nMigration completed.")
