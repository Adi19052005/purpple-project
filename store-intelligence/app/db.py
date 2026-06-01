import os
import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://apex_admin:apex_secure_password@postgres-db:5432/store_intelligence")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    """Initializes schema blueprints required to catalog immutable event states."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Base Core Event Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS store_events (
            event_id UUID PRIMARY KEY,
            store_id VARCHAR(50) NOT NULL,
            camera_id VARCHAR(50) NOT NULL,
            visitor_id VARCHAR(50) NOT NULL,
            event_type VARCHAR(30) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            zone_id VARCHAR(50),
            dwell_ms INT DEFAULT 0,
            is_staff BOOLEAN DEFAULT FALSE,
            confidence FLOAT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_events_store_time ON store_events(store_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_events_visitor ON store_events(visitor_id);
    """)
    
    # 2. POS Dynamic Correlation Data Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pos_transactions (
            transaction_id VARCHAR(100) PRIMARY KEY,
            store_id VARCHAR(50) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            amount NUMERIC(10, 2) NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pos_store_time ON pos_transactions(store_id, timestamp);
    """)
    
    # 3. Active Anomalies Log Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS store_anomalies (
            id SERIAL PRIMARY KEY,
            store_id VARCHAR(50) NOT NULL,
            anomaly_type VARCHAR(50) NOT NULL,
            severity VARCHAR(20) NOT NULL,
            description TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("[+] PostgreSQL Schema tables initiated successfully.")