import os
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
from typing import Any

if os.getenv("DOCKER_ENV") == "true" or os.getenv("DATABASE_URL"):
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "postgresql://apex_admin:apex_secure_password@postgres-db:5432/store_intelligence"
    )
else:
    DATABASE_URL = "postgresql://apex_admin:apex_secure_password@localhost:5432/store_intelligence"


def get_db_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(DATABASE_URL)


def init_db() -> None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS store_events (
                    event_id UUID PRIMARY KEY,
                    store_id VARCHAR(50) NOT NULL,
                    store_code VARCHAR(50) NOT NULL,
                    camera_id VARCHAR(50) NOT NULL,
                    id_token VARCHAR(50) NOT NULL,
                    event_type VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    zone_id VARCHAR(50),
                    dwell_ms INT DEFAULT 0,
                    is_staff BOOLEAN DEFAULT FALSE,
                    confidence FLOAT NOT NULL,
                    gender_pred VARCHAR(50),
                    age_bucket VARCHAR(50),
                    wait_seconds FLOAT,
                    abandoned BOOLEAN DEFAULT FALSE
                );
                CREATE INDEX IF NOT EXISTS idx_events_id_token ON store_events(id_token);
                CREATE INDEX IF NOT EXISTS idx_events_event_type ON store_events(event_type);
            """)

            # Ensure legacy tables that may pre-exist have the expected columns
            cur.execute("""
                ALTER TABLE store_events ADD COLUMN IF NOT EXISTS store_code VARCHAR(50);
                ALTER TABLE visitor_sessions ADD COLUMN IF NOT EXISTS store_code VARCHAR(50);
                ALTER TABLE store_anomalies ADD COLUMN IF NOT EXISTS store_code VARCHAR(50);
                ALTER TABLE staff_activity ADD COLUMN IF NOT EXISTS store_code VARCHAR(50);
                ALTER TABLE heatmap_coordinates ADD COLUMN IF NOT EXISTS store_code VARCHAR(50);
                ALTER TABLE pos_transactions ADD COLUMN IF NOT EXISTS store_code VARCHAR(50);
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_store_code_time ON store_events(store_code, timestamp);
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS visitor_sessions (
                    id_token VARCHAR(50) PRIMARY KEY,
                    store_id VARCHAR(50) NOT NULL,
                    store_code VARCHAR(50) NOT NULL,
                    first_entry_time TIMESTAMPTZ NOT NULL,
                    last_activity_time TIMESTAMPTZ NOT NULL,
                    zones_visited TEXT,
                    billing_zone_entry_time TIMESTAMPTZ,
                    total_dwell_ms INT DEFAULT 0,
                    is_staff BOOLEAN DEFAULT FALSE,
                    gender_pred VARCHAR(50),
                    age_bucket VARCHAR(50),
                    wait_seconds FLOAT,
                    abandoned BOOLEAN DEFAULT FALSE
                );
                CREATE INDEX IF NOT EXISTS idx_sessions_store_code ON visitor_sessions(store_code);
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS store_anomalies (
                    id SERIAL PRIMARY KEY,
                    store_code VARCHAR(50) NOT NULL,
                    id_token VARCHAR(50),
                    anomaly_type VARCHAR(50) NOT NULL,
                    severity VARCHAR(20) NOT NULL,
                    description TEXT NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_anomalies_store_code_time ON store_anomalies(store_code, timestamp);
                CREATE INDEX IF NOT EXISTS idx_anomalies_type ON store_anomalies(anomaly_type);
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS staff_activity (
                    event_id UUID PRIMARY KEY,
                    store_code VARCHAR(50) NOT NULL,
                    camera_id VARCHAR(50) NOT NULL,
                    id_token VARCHAR(50) NOT NULL,
                    event_type VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    zone_id VARCHAR(50),
                    is_staff BOOLEAN DEFAULT TRUE,
                    confidence FLOAT NOT NULL,
                    metadata JSONB
                );
                CREATE INDEX IF NOT EXISTS idx_staff_activity_store_code_time ON staff_activity(store_code, timestamp);
                CREATE INDEX IF NOT EXISTS idx_staff_activity_id_token ON staff_activity(id_token);
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS heatmap_coordinates (
                    id SERIAL PRIMARY KEY,
                    store_code VARCHAR(50) NOT NULL,
                    camera_id VARCHAR(50) NOT NULL,
                    id_token VARCHAR(50),
                    x_normalized FLOAT NOT NULL,
                    y_normalized FLOAT NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    batch_frame_index INT
                );
                CREATE INDEX IF NOT EXISTS idx_heatmap_store_code_time ON heatmap_coordinates(store_code, timestamp);
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS pos_transactions (
                    transaction_id VARCHAR(100) PRIMARY KEY,
                    store_code VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    amount NUMERIC(10, 2) NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_pos_store_code_time ON pos_transactions(store_code, timestamp);
            """)

        conn.commit()
    finally:
        conn.close()
        print("[+] PostgreSQL Schema tables initiated successfully.")
