from db import get_db_connection


def calculate_store_funnel(store_code: str, start_time, end_time):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
                SELECT COUNT(DISTINCT id_token)
                FROM store_events
                WHERE store_code = %s
                  AND event_type = 'entry'
                  AND timestamp BETWEEN %s AND %s
                  AND is_staff = FALSE
            """,
            (store_code, start_time, end_time),
        )
        stage_1 = cur.fetchone()[0] or 0

        cur.execute(
            """
                SELECT COUNT(DISTINCT id_token)
                FROM store_events
                WHERE store_code = %s
                  AND event_type = 'zone_enter'
                  AND zone_id = 'ZONE_1'
                  AND timestamp BETWEEN %s AND %s
                  AND is_staff = FALSE
            """,
            (store_code, start_time, end_time),
        )
        stage_2 = cur.fetchone()[0] or 0

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
        stage_3 = cur.fetchone()[0] or 0

        cur.execute(
            """
                SELECT COUNT(DISTINCT join_events.id_token)
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
                  AND EXTRACT(EPOCH FROM (exit_events.timestamp - join_events.timestamp)) > 30
                  AND EXTRACT(EPOCH FROM (exit_events.timestamp - join_events.timestamp)) < 3600
            """,
            (store_code, store_code, start_time, end_time, start_time, end_time),
        )
        stage_4 = cur.fetchone()[0] or 0

        def safe_ratio(numerator, denominator):
            return round((numerator / denominator * 100.0), 2) if denominator > 0 else 0.0

        return {
            "funnel_stages": [
                {"stage": "Entries", "count": stage_1, "retention_rate": 100.0, "dropoff_percentage": 0.0},
                {"stage": "Zone 1 Browsing", "count": stage_2, "retention_rate": safe_ratio(stage_2, stage_1), "dropoff_percentage": round(100.0 - safe_ratio(stage_2, stage_1), 2)},
                {"stage": "Billing Queue", "count": stage_3, "retention_rate": safe_ratio(stage_3, stage_2), "dropoff_percentage": round(100.0 - safe_ratio(stage_3, stage_2), 2)},
                {"stage": "Checkout Complete", "count": stage_4, "retention_rate": safe_ratio(stage_4, stage_3), "dropoff_percentage": round(100.0 - safe_ratio(stage_4, stage_3), 2)},
            ],
            "total_conversion_rate": safe_ratio(stage_4, stage_1),
        }
    finally:
        conn.close()
