import redis
from datetime import datetime
from db import get_db_connection

def evaluate_realtime_anomalies(store_id: str, r_client: redis.Redis):
    """
    Scans live telemetry states against thresholds to detect operational exceptions.
    Returns generated alerts list.
    """
    alerts = []
    
    # Anomaly Rule A: Long Billing Queue Detection
    # Extract instantaneous depth metric from hot path Redis store
    live_queue_depth = r_client.hget(f"store:{store_id}:live_metrics", "queue_depth")
    
    if live_queue_depth and int(live_queue_depth) > 15:
        alerts.append({
            "anomaly_type": "QUEUE_SPIKE",
            "severity": "CRITICAL",
            "description": f"Billing line layout exceeded 15 people. Live Headcount: {live_queue_depth}."
        })
        
    # Write any caught anomalies down to PostgreSQL for audit logs
    if alerts:
        conn = get_db_connection()
        cur = conn.cursor()
        for alert in alerts:
            cur.execute("""
                INSERT INTO store_anomalies (store_id, anomaly_type, severity, description, timestamp)
                VALUES (%s, %s, %s, %s, NOW());
            """, (store_id, alert["anomaly_type"], alert["severity"], alert["description"]))
        conn.commit()
        cur.close()
        conn.close()
        
    return alerts