import sqlite3
import secrets
import string

DB_PATH = "./iot_devices.db"

def generate_secret_key(length: int = 32) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("PRAGMA table_info(devices)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        if 'secret_key' not in column_names:
            print("[Migration] Adding secret_key column to devices table...")
            cursor.execute("""
                ALTER TABLE devices ADD COLUMN secret_key TEXT
            """)
            conn.commit()
            print("[Migration] Successfully added secret_key column")
        else:
            print("[Migration] secret_key column already exists")
        
        cursor.execute("SELECT device_id FROM devices WHERE secret_key IS NULL OR secret_key = ''")
        rows = cursor.fetchall()
        
        if rows:
            print(f"[Migration] Generating secret keys for {len(rows)} devices...")
            for (device_id,) in rows:
                secret_key = generate_secret_key()
                cursor.execute(
                    "UPDATE devices SET secret_key = ? WHERE device_id = ?",
                    (secret_key, device_id)
                )
                print(f"[Migration] Generated secret key for device: {device_id}")
            conn.commit()
            print(f"[Migration] Successfully generated secret keys for {len(rows)} devices")
        else:
            print("[Migration] All devices already have secret keys")
        
        print("[Migration] Migration completed successfully!")
        
    except Exception as e:
        print(f"[Migration] Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
