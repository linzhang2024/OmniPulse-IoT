import sqlite3
import json
from datetime import datetime

DB_PATH = "./iot_devices.db"

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("PRAGMA table_info(devices)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        if 'pending_commands' not in column_names:
            print("[Migration] Adding pending_commands column to devices table...")
            cursor.execute("""
                ALTER TABLE devices ADD COLUMN pending_commands TEXT DEFAULT '[]'
            """)
            conn.commit()
            print("[Migration] Successfully added pending_commands column")
        else:
            print("[Migration] pending_commands column already exists")
        
        cursor.execute("UPDATE devices SET pending_commands = '[]' WHERE pending_commands IS NULL")
        conn.commit()
        
        print("[Migration] Migration completed successfully!")
        
    except Exception as e:
        print(f"[Migration] Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
