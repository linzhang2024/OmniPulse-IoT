import sqlite3
from datetime import datetime

DB_PATH = "./iot_devices.db"

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
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
                    FOREIGN KEY (device_id) REFERENCES devices (device_id)
                )
            """)
            
            cursor.execute("""
                CREATE INDEX ix_device_data_history_id 
                ON device_data_history (id)
            """)
            
            cursor.execute("""
                CREATE INDEX ix_device_data_history_device_id 
                ON device_data_history (device_id)
            """)
            
            conn.commit()
            print("[Migration] Successfully created device_data_history table")
            print("[Migration] Table structure:")
            print("  - id: VARCHAR(36) PRIMARY KEY")
            print("  - device_id: VARCHAR(64) NOT NULL (Foreign Key)")
            print("  - payload: JSON NOT NULL")
            print("  - timestamp: DATETIME")
        else:
            print("[Migration] device_data_history table already exists")
        
        print("[Migration] Migration completed successfully!")
        
    except Exception as e:
        print(f"[Migration] Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
