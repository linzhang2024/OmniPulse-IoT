import sqlite3
from datetime import datetime

DB_PATH = "./iot_devices.db"

def check_column_exists(cursor, table_name, column_name):
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    for col in columns:
        if col[1] == column_name:
            return True
    return False

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        print("[Migration] Checking device_data_history table...")
        
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='device_data_history'
        """)
        table_exists = cursor.fetchone()
        
        if not table_exists:
            print("[Migration] Creating device_data_history table...")
            cursor.execute("""
                CREATE TABLE device_data_history (
                    id VARCHAR(36) PRIMARY KEY,
                    device_id VARCHAR(64) NOT NULL,
                    payload JSON NOT NULL,
                    timestamp DATETIME,
                    temperature REAL,
                    humidity REAL,
                    is_alert BOOLEAN DEFAULT 0,
                    FOREIGN KEY (device_id) REFERENCES devices (device_id)
                )
            """)
            
            cursor.execute("CREATE INDEX ix_device_data_history_id ON device_data_history (id)")
            cursor.execute("CREATE INDEX ix_device_data_history_device_id ON device_data_history (device_id)")
            cursor.execute("CREATE INDEX ix_device_data_history_timestamp ON device_data_history (timestamp)")
            cursor.execute("CREATE INDEX ix_device_data_history_temperature ON device_data_history (temperature)")
            cursor.execute("CREATE INDEX ix_device_data_history_is_alert ON device_data_history (is_alert)")
            
            conn.commit()
            print("[Migration] Successfully created device_data_history table with aggregation fields")
        else:
            print("[Migration] Table exists, checking for missing columns...")
            
            if not check_column_exists(cursor, 'device_data_history', 'temperature'):
                print("[Migration] Adding 'temperature' column...")
                cursor.execute("ALTER TABLE device_data_history ADD COLUMN temperature REAL")
                print("[Migration] 'temperature' column added")
            
            if not check_column_exists(cursor, 'device_data_history', 'humidity'):
                print("[Migration] Adding 'humidity' column...")
                cursor.execute("ALTER TABLE device_data_history ADD COLUMN humidity REAL")
                print("[Migration] 'humidity' column added")
            
            if not check_column_exists(cursor, 'device_data_history', 'is_alert'):
                print("[Migration] Adding 'is_alert' column...")
                cursor.execute("ALTER TABLE device_data_history ADD COLUMN is_alert BOOLEAN DEFAULT 0")
                print("[Migration] 'is_alert' column added")
            
            if not check_column_exists(cursor, 'device_data_history', 'timestamp'):
                print("[Migration] Adding 'timestamp' column...")
                cursor.execute("ALTER TABLE device_data_history ADD COLUMN timestamp DATETIME")
                print("[Migration] 'timestamp' column added")
            
            conn.commit()
            
            print("[Migration] Checking indexes...")
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='ix_device_data_history_timestamp'")
            if not cursor.fetchone():
                print("[Migration] Creating index ix_device_data_history_timestamp...")
                cursor.execute("CREATE INDEX ix_device_data_history_timestamp ON device_data_history (timestamp)")
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='ix_device_data_history_temperature'")
            if not cursor.fetchone():
                print("[Migration] Creating index ix_device_data_history_temperature...")
                cursor.execute("CREATE INDEX ix_device_data_history_temperature ON device_data_history (temperature)")
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='ix_device_data_history_is_alert'")
            if not cursor.fetchone():
                print("[Migration] Creating index ix_device_data_history_is_alert...")
                cursor.execute("CREATE INDEX ix_device_data_history_is_alert ON device_data_history (is_alert)")
            
            conn.commit()
            print("[Migration] Indexes updated")
        
        cursor.execute("PRAGMA table_info(device_data_history)")
        columns = cursor.fetchall()
        print("\n[Migration] Final table structure:")
        for col in columns:
            print(f"  - {col[1]}: {col[2]}")
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'ix_device_data_history_%'")
        indexes = cursor.fetchall()
        print("\n[Migration] Indexes:")
        for idx in indexes:
            print(f"  - {idx[0]}")
        
        print("\n[Migration] Migration completed successfully!")
        
    except Exception as e:
        print(f"[Migration] Error: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
