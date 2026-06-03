from db import get_db_connection
from datetime import timedelta


def calculate_store_metrics(store_code: str, start_time, end_time):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
                SELECT COUNT(DISTINCT id_token)
                FROM store_events
                WHERE store_code = %s
                  AND timestamp BETWEEN %s AND %s
                  AND is_staff = FALSE
                  AND event_type = 'entry'
            """,
            (store_code, start_time, end_time),
        )
        total_customers = cur.fetchone()[0] or 0

        cur.execute(
            """
                SELECT AVG(dwell_ms)
                FROM store_events
                WHERE store_code = %s
                  AND timestamp BETWEEN %s AND %s
                  AND event_type = 'zone_dwell'
            """,
            (store_code, start_time, end_time),
        )
        avg_dwell_ms = cur.fetchone()[0] or 0
        avg_dwell_minutes = round((avg_dwell_ms / 1000.0) / 60.0, 2)

        cur.execute(
            """
                SELECT COUNT(DISTINCT id_token)
                FROM store_events
                WHERE store_code = %s
                  AND event_type = 'queue_join'
                  AND timestamp BETWEEN %s AND %s
                  AND is_staff = FALSE
            """,
            (store_code, start_time, end_time),
        )
        queue_visitors = cur.fetchone()[0] or 0

        cur.execute(
            """
                SELECT COUNT(DISTINCT id_token)
                FROM store_events
                WHERE store_code = %s
                  AND event_type = 'exit'
                  AND timestamp BETWEEN %s AND %s
                  AND is_staff = FALSE
            """,
            (store_code, start_time, end_time),
        )
        exits = cur.fetchone()[0] or 0

        cur.execute(
            """
                SELECT COUNT(DISTINCT id_token)
                FROM store_events
                WHERE store_code = %s
                  AND event_type = 'exit'
                  AND zone_id = 'ZONE_2'
                  AND timestamp BETWEEN %s AND %s
                  AND is_staff = FALSE
            """,
            (store_code, start_time, end_time),
        )
        potential_conversions = cur.fetchone()[0] or 0

        cur.execute(
            """
                SELECT AVG(EXTRACT(EPOCH FROM (timestamp - join_events.timestamp)))
                FROM store_events join_events
                INNER JOIN store_events exit_events
                ON join_events.id_token = exit_events.id_token
                WHERE join_events.store_code = %s
                  AND join_events.event_type = 'queue_join'
                  AND exit_events.store_code = %s
                  AND exit_events.event_type = 'exit'
                  AND exit_events.zone_id = 'ZONE_2'
                  AND join_events.timestamp BETWEEN %s AND %s
                  AND exit_events.timestamp BETWEEN %s AND %s
                  AND exit_events.timestamp > join_events.timestamp
                  AND EXTRACT(EPOCH FROM (exit_events.timestamp - join_events.timestamp)) < 3600
            """,
            (store_code, store_code, start_time, end_time, start_time, end_time),
        )
        avg_queue_wait_seconds = cur.fetchone()[0] or 0.0

        metrics = {
            "total_unique_customers": total_customers,
            "total_queue_visitors": queue_visitors,
            "total_exits": exits,
            "total_potential_conversions": potential_conversions,
            "conversion_rate_percentage": round((potential_conversions / total_customers * 100.0) if total_customers > 0 else 0.0, 2),
            "avg_dwell_time_minutes": avg_dwell_minutes,
            "avg_queue_wait_time_seconds": round(avg_queue_wait_seconds, 2),
        }
        return metrics
    finally:
        conn.close()
