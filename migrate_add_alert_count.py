import sqlite3

DB_PATH = "./iot_devices.db"

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("PRAGMA table_info(devices)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        if 'consecutive_alert_count' not in column_names:
            print("[Migration] Adding consecutive_alert_count column to devices table...")
            cursor.execute("""
                ALTER TABLE devices ADD COLUMN consecutive_alert_count INTEGER DEFAULT 0
            """)
            conn.commit()
            print("[Migration] Successfully added consecutive_alert_count column")
        else:
            print("[Migration] consecutive_alert_count column already exists")
        
        cursor.execute("UPDATE devices SET consecutive_alert_count = 0 WHERE consecutive_alert_count IS NULL")
        conn.commit()
        
        print("[Migration] Migration completed successfully!")
        
    except Exception as e:
        print(f"[Migration] Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
