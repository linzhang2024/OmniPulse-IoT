import sqlite3
import os

DB_PATH = "./iot_devices.db"

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='device_commands'")
        if cursor.fetchone():
            print("[Migration] device_commands table already exists")
        else:
            print("[Migration] Creating device_commands table...")
            cursor.execute("""
                CREATE TABLE device_commands (
                    id VARCHAR(36) PRIMARY KEY,
                    device_id VARCHAR(64) NOT NULL,
                    command_type VARCHAR(64) NOT NULL,
                    command_value VARCHAR(255),
                    status VARCHAR(32) NOT NULL DEFAULT 'pending',
                    ttl_seconds INTEGER NOT NULL DEFAULT 600,
                    expires_at DATETIME NOT NULL,
                    delivered_at DATETIME,
                    executed_at DATETIME,
                    failed_at DATETIME,
                    expired_at DATETIME,
                    error_message VARCHAR(2000),
                    result_data TEXT,
                    reason VARCHAR(500),
                    source VARCHAR(64) NOT NULL DEFAULT 'api',
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (device_id) REFERENCES devices (device_id)
                )
            """)
            conn.commit()
            print("[Migration] Successfully created device_commands table")
        
        print("[Migration] Creating indexes...")
        
        try:
            cursor.execute("CREATE INDEX ix_device_commands_id ON device_commands (id)")
            conn.commit()
            print("[Migration] Created index: ix_device_commands_id")
        except sqlite3.OperationalError as e:
            if "index already exists" in str(e):
                print("[Migration] Index ix_device_commands_id already exists")
            else:
                raise
        
        try:
            cursor.execute("CREATE INDEX ix_device_commands_device_id ON device_commands (device_id)")
            conn.commit()
            print("[Migration] Created index: ix_device_commands_device_id")
        except sqlite3.OperationalError as e:
            if "index already exists" in str(e):
                print("[Migration] Index ix_device_commands_device_id already exists")
            else:
                raise
        
        try:
            cursor.execute("CREATE INDEX ix_device_commands_status ON device_commands (status)")
            conn.commit()
            print("[Migration] Created index: ix_device_commands_status")
        except sqlite3.OperationalError as e:
            if "index already exists" in str(e):
                print("[Migration] Index ix_device_commands_status already exists")
            else:
                raise
        
        try:
            cursor.execute("CREATE INDEX ix_device_commands_expires_at ON device_commands (expires_at)")
            conn.commit()
            print("[Migration] Created index: ix_device_commands_expires_at")
        except sqlite3.OperationalError as e:
            if "index already exists" in str(e):
                print("[Migration] Index ix_device_commands_expires_at already exists")
            else:
                raise
        
        try:
            cursor.execute("CREATE INDEX ix_device_commands_created_at ON device_commands (created_at)")
            conn.commit()
            print("[Migration] Created index: ix_device_commands_created_at")
        except sqlite3.OperationalError as e:
            if "index already exists" in str(e):
                print("[Migration] Index ix_device_commands_created_at already exists")
            else:
                raise
        
        print("[Migration] Migration completed successfully!")
        
    except Exception as e:
        print(f"[Migration] Error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
