import sqlite3
from datetime import datetime

DB_PATH = "./iot_devices.db"

def migrate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='users'
        """)
        table_exists = cursor.fetchone()
        
        if not table_exists:
            print("[Migration] Creating users table...")
            cursor.execute("""
                CREATE TABLE users (
                    id VARCHAR(36) PRIMARY KEY,
                    username VARCHAR(64) NOT NULL UNIQUE,
                    password_hash VARCHAR(128) NOT NULL,
                    role VARCHAR(20) DEFAULT 'guest',
                    created_at DATETIME,
                    updated_at DATETIME
                )
            """)
            
            cursor.execute("""
                CREATE UNIQUE INDEX ix_users_username 
                ON users (username)
            """)
            
            cursor.execute("""
                CREATE INDEX ix_users_id 
                ON users (id)
            """)
            
            print("[Migration] Users table created successfully")
        
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='complaints'
        """)
        table_exists = cursor.fetchone()
        
        if not table_exists:
            print("[Migration] Creating complaints table...")
            cursor.execute("""
                CREATE TABLE complaints (
                    id VARCHAR(36) PRIMARY KEY,
                    user_id VARCHAR(36) NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    created_at DATETIME,
                    updated_at DATETIME,
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
            """)
            
            cursor.execute("""
                CREATE INDEX ix_complaints_id 
                ON complaints (id)
            """)
            
            cursor.execute("""
                CREATE INDEX ix_complaints_user_id 
                ON complaints (user_id)
            """)
            
            print("[Migration] Complaints table created successfully")
        
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='complaint_replies'
        """)
        table_exists = cursor.fetchone()
        
        if not table_exists:
            print("[Migration] Creating complaint_replies table...")
            cursor.execute("""
                CREATE TABLE complaint_replies (
                    id VARCHAR(36) PRIMARY KEY,
                    complaint_id VARCHAR(36) NOT NULL,
                    user_id VARCHAR(36) NOT NULL,
                    content TEXT NOT NULL,
                    created_at DATETIME,
                    FOREIGN KEY (complaint_id) REFERENCES complaints (id),
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
            """)
            
            cursor.execute("""
                CREATE INDEX ix_complaint_replies_id 
                ON complaint_replies (id)
            """)
            
            cursor.execute("""
                CREATE INDEX ix_complaint_replies_complaint_id 
                ON complaint_replies (complaint_id)
            """)
            
            cursor.execute("""
                CREATE INDEX ix_complaint_replies_user_id 
                ON complaint_replies (user_id)
            """)
            
            print("[Migration] Complaint_replies table created successfully")
        
        conn.commit()
        print("[Migration] All tables created successfully!")
        
    except Exception as e:
        print(f"[Migration] Error: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
