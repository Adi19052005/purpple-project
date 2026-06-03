import redis
from datetime import datetime, timedelta
from db import get_db_connection


def evaluate_realtime_anomalies(store_code: str, r_client: redis.Redis):
    alerts = []
    queue_depth = r_client.hget(f"store:{store_code}:live_metrics", "queue_depth")
    if queue_depth is not None and int(queue_depth) > 15:
        alerts.append({
            "anomaly_type": "QUEUE_SPIKE",
            "severity": "CRITICAL",
            "description": f"Live billing queue exceeded 15 visitors ({queue_depth}).",
            "timestamp": datetime.utcnow().isoformat(),
        })

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cutoff = datetime.utcnow() - timedelta(minutes=5)
        cur.execute(
            """
                SELECT anomaly_type, description, id_token, timestamp
                FROM store_anomalies
                WHERE store_code = %s
                  AND timestamp > %s
                ORDER BY timestamp DESC
                LIMIT 10;
            """,
            (store_code, cutoff),
        )
        rows = cur.fetchall()
        for anomaly_type, description, id_token, timestamp in rows:
            severity = "CRITICAL" if anomaly_type == "THEFT_SUSPICION_ANOMALY" else "WARNING"
            alerts.append({
                "anomaly_type": anomaly_type,
                "severity": severity,
                "description": description,
                "id_token": id_token,
                "timestamp": timestamp.isoformat() if hasattr(timestamp, "isoformat") else str(timestamp),
            })
    finally:
        conn.close()

    anomaly_key = f"store:{store_code}:anomalies_24h"
    if r_client.llen(anomaly_key) > 20:
        alerts.append({
            "anomaly_type": "ANOMALY_SURGE",
            "severity": "WARNING",
            "description": "Persistent anomaly surge in the last 24 hours.",
            "timestamp": datetime.utcnow().isoformat(),
        })

    return alerts
