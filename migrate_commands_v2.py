import sqlite3
import json
import uuid
from datetime import datetime

DB_PATH = "./iot_devices.db"

def migrate_old_commands():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT device_id, pending_commands FROM devices")
        rows = cursor.fetchall()
        
        migrated_count = 0
        total_commands = 0
        
        for device_id, pending_commands_json in rows:
            if not pending_commands_json:
                continue
            
            try:
                commands = json.loads(pending_commands_json)
            except json.JSONDecodeError:
                continue
            
            if not isinstance(commands, list):
                continue
            
            has_old_format = False
            migrated_commands = []
            
            for cmd in commands:
                if not isinstance(cmd, dict):
                    continue
                
                if 'id' not in cmd or 'status' not in cmd:
                    has_old_format = True
                    total_commands += 1
                    
                    new_cmd = {
                        "id": str(uuid.uuid4()),
                        "command": cmd.get('command', 'unknown'),
                        "value": cmd.get('value', ''),
                        "status": "pending",
                        "created_at": cmd.get('timestamp') or datetime.utcnow().isoformat(),
                        "delivered_at": None,
                        "reason": cmd.get('reason')
                    }
                    migrated_commands.append(new_cmd)
                    print(f"[Migration] Converted old command for device {device_id}: {new_cmd['command']}={new_cmd['value']}")
                else:
                    migrated_commands.append(cmd)
            
            if has_old_format:
                new_json = json.dumps(migrated_commands)
                cursor.execute(
                    "UPDATE devices SET pending_commands = ? WHERE device_id = ?",
                    (new_json, device_id)
                )
                migrated_count += 1
        
        conn.commit()
        
        if migrated_count > 0:
            print(f"[Migration] Successfully migrated {total_commands} commands in {migrated_count} devices")
        else:
            print("[Migration] No old format commands found, database is already up to date")
        
    except Exception as e:
        print(f"[Migration] Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_old_commands()
