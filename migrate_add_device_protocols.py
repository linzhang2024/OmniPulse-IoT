import sqlite3
from datetime import datetime

DB_PATH = "./iot_devices.db"

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='device_protocols'
        """)
        table_exists = cursor.fetchone()
        
        if not table_exists:
            print("[Migration] Creating device_protocols table...")
            cursor.execute("""
                CREATE TABLE device_protocols (
                    id VARCHAR(36) PRIMARY KEY,
                    device_id VARCHAR(64) NOT NULL,
                    protocol_type VARCHAR(20) DEFAULT 'json',
                    is_active BOOLEAN DEFAULT 1,
                    parse_config JSON NOT NULL,
                    field_mappings JSON NOT NULL,
                    transform_formulas JSON NOT NULL,
                    description VARCHAR(500),
                    created_at DATETIME,
                    updated_at DATETIME,
                    FOREIGN KEY (device_id) REFERENCES devices (device_id)
                )
            """)
            
            cursor.execute("""
                CREATE INDEX ix_device_protocols_id 
                ON device_protocols (id)
            """)
            
            cursor.execute("""
                CREATE INDEX ix_device_protocols_device_id 
                ON device_protocols (device_id)
            """)
            
            cursor.execute("""
                CREATE INDEX ix_device_protocols_is_active 
                ON device_protocols (is_active)
            """)
            
            conn.commit()
            print("[Migration] Successfully created device_protocols table")
            print("[Migration] Table structure:")
            print("  - id: VARCHAR(36) PRIMARY KEY")
            print("  - device_id: VARCHAR(64) NOT NULL (Foreign Key)")
            print("  - protocol_type: VARCHAR(20) DEFAULT 'json'")
            print("  - is_active: BOOLEAN DEFAULT 1")
            print("  - parse_config: JSON NOT NULL")
            print("  - field_mappings: JSON NOT NULL")
            print("  - transform_formulas: JSON NOT NULL")
            print("  - description: VARCHAR(500)")
            print("  - created_at: DATETIME")
            print("  - updated_at: DATETIME")
        else:
            print("[Migration] device_protocols table already exists")
        
        print("[Migration] Migration completed successfully!")
        
    except Exception as e:
        print(f"[Migration] Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
