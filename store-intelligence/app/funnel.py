from db import get_db_connection

def calculate_store_funnel(store_id: str, start_time, end_time):
    """
    Evaluates customer journey conversion steps to identify leakages.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Stage 1: Absolute Entry Traffic
    cur.execute("""
        SELECT COUNT(DISTINCT visitor_id) FROM store_events
        WHERE store_id = %s AND event_type = 'ENTRY' 
          AND timestamp BETWEEN %s AND %s AND is_staff = FALSE;
    """, (store_id, start_time, end_time))
    stage_entry = cur.fetchone()[0] or 0

    # Stage 2: Browsing Product Zones (e.g., Browsed Skincare or Fragrance areas)
    cur.execute("""
        SELECT COUNT(DISTINCT visitor_id) FROM store_events
        WHERE store_id = %s AND event_type = 'ZONE_ENTER' 
          AND zone_id IN ('SKINCARE', 'FRAGRANCE')
          AND timestamp BETWEEN %s AND %s AND is_staff = FALSE;
    """, (store_id, start_time, end_time))
    stage_browse = cur.fetchone()[0] or 0

    # Stage 3: Reached Checkout Line
    cur.execute("""
        SELECT COUNT(DISTINCT visitor_id) FROM store_events
        WHERE store_id = %s AND event_type = 'BILLING_QUEUE_JOIN'
          AND timestamp BETWEEN %s AND %s AND is_staff = FALSE;
    """, (store_id, start_time, end_time))
    stage_queue = cur.fetchone()[0] or 0

    # For Stage 4 (Purchase), we pull from the converted metric counts calculated above.
    # To keep this query fast, we approximate via total distinct transaction records.
    cur.execute("""
        SELECT COUNT(DISTINCT transaction_id) FROM pos_transactions
        WHERE store_id = %s AND timestamp BETWEEN %s AND %s;
    """, (store_id, start_time, end_time))
    stage_purchase = cur.fetchone()[0] or 0

    cur.close()
    conn.close()

    # Calculate sequential drop-off ratios safely
    def calculate_retention(current_stage, base_stage):
        if base_stage == 0: return 0.0
        return round((current_stage / base_stage) * 100, 2)

    return {
        "funnel_stages": [
            {"stage": "1_ENTRY", "count": stage_entry, "retention_rate": 100.0},
            {"stage": "2_BROWSE", "count": stage_browse, "retention_rate": calculate_retention(stage_browse, stage_entry)},
            {"stage": "3_QUEUE", "count": stage_queue, "retention_rate": calculate_retention(stage_queue, stage_browse)},
            {"stage": "4_PURCHASE", "count": stage_purchase, "retention_rate": calculate_retention(stage_purchase, stage_queue)}
        ]
    }