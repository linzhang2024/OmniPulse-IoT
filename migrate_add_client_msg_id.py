import sqlite3
import os

DB_PATH = "./iot_devices.db"

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("PRAGMA table_info(device_commands)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        if 'client_msg_id' not in column_names:
            print("[Migration] Adding client_msg_id column to device_commands table...")
            cursor.execute("""
                ALTER TABLE device_commands ADD COLUMN client_msg_id VARCHAR(128)
            """)
            conn.commit()
            print("[Migration] Successfully added client_msg_id column")
        else:
            print("[Migration] client_msg_id column already exists")
        
        print("[Migration] Creating unique index on client_msg_id...")
        try:
            cursor.execute("CREATE UNIQUE INDEX ix_device_commands_client_msg_id ON device_commands (client_msg_id)")
            conn.commit()
            print("[Migration] Successfully created unique index: ix_device_commands_client_msg_id")
        except sqlite3.OperationalError as e:
            if "index already exists" in str(e) or "UNIQUE constraint failed" in str(e):
                print("[Migration] Index ix_device_commands_client_msg_id already exists or data has duplicates")
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
