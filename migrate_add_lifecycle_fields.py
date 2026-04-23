import sqlite3
from datetime import datetime

DB_PATH = "./iot_devices.db"

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        print("=" * 60)
        print("Device Lifecycle Fields Migration")
        print("=" * 60)
        print()
        
        print("[Step 1] Checking devices table columns...")
        cursor.execute("PRAGMA table_info(devices)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        print(f"         Existing columns: {', '.join(column_names)}")
        
        if 'last_seen' not in column_names:
            print("[Step 1a] Adding last_seen column to devices table...")
            cursor.execute("""
                ALTER TABLE devices ADD COLUMN last_seen DATETIME
            """)
            conn.commit()
            print("         Successfully added last_seen column")
        else:
            print("         last_seen column already exists")
        
        print()
        print("[Step 1b] Initializing last_seen from last_heartbeat...")
        cursor.execute("""
            UPDATE devices 
            SET last_seen = last_heartbeat 
            WHERE last_seen IS NULL AND last_heartbeat IS NOT NULL
        """)
        updated_count = cursor.rowcount
        conn.commit()
        print(f"         Initialized last_seen for {updated_count} devices")
        
        print()
        print("[Step 2] Checking if device_status_events table exists...")
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='device_status_events'
        """)
        
        if not cursor.fetchone():
            print("[Step 2a] Creating device_status_events table...")
            cursor.execute("""
                CREATE TABLE device_status_events (
                    id TEXT(36) PRIMARY KEY,
                    device_id TEXT(64) NOT NULL,
                    event_type TEXT(32) NOT NULL,
                    old_status TEXT(32),
                    new_status TEXT(32),
                    reason TEXT(500),
                    details TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (device_id) REFERENCES devices (device_id)
                )
            """)
            conn.commit()
            print("         Successfully created device_status_events table")
            
            print("[Step 2b] Creating indexes for device_status_events...")
            cursor.execute("""
                CREATE INDEX ix_device_status_events_device_id 
                ON device_status_events (device_id)
            """)
            cursor.execute("""
                CREATE INDEX ix_device_status_events_event_type 
                ON device_status_events (event_type)
            """)
            cursor.execute("""
                CREATE INDEX ix_device_status_events_created_at 
                ON device_status_events (created_at)
            """)
            conn.commit()
            print("         Successfully created indexes")
        else:
            print("         device_status_events table already exists")
        
        print()
        print("[Step 3] Verifying migration...")
        
        cursor.execute("PRAGMA table_info(devices)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        if 'last_seen' in column_names:
            print("         [OK] last_seen column exists in devices table")
        else:
            raise Exception("last_seen column missing from devices table")
        
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='device_status_events'
        """)
        if cursor.fetchone():
            print("         [OK] device_status_events table exists")
        else:
            raise Exception("device_status_events table missing")
        
        print()
        print("=" * 60)
        print("[Migration] Completed successfully!")
        print("=" * 60)
        print()
        print("Summary of changes:")
        print("  - Added 'last_seen' column to devices table")
        print("  - Initialized 'last_seen' from existing 'last_heartbeat' values")
        print("  - Created 'device_status_events' table for tracking status changes")
        print("  - Created indexes for fast querying")
        print()
        print("Note:")
        print("  - HEARTBEAT_TIMEOUT has been changed from 30s to 120s (2 minutes)")
        print("  - CHECK_INTERVAL has been changed from 10s to 30s")
        print("  - New API endpoint: GET /devices/status-events")
        
    except Exception as e:
        print(f"[Migration] Error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
