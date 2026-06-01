from datetime import timedelta
import psycopg2
from db import get_db_connection

def calculate_store_metrics(store_id: str, start_time, end_time):
    """
    Computes real-time traffic statistics, unique customer visits, 
    and transaction-correlated conversion rates.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Gather Total Unique Foot Traffic Count (Excluding Staff Logs)
    cur.execute("""
        SELECT COUNT(DISTINCT visitor_id) 
        FROM store_events 
        WHERE store_id = %s 
          AND timestamp BETWEEN %s AND %s
          AND is_staff = FALSE;
    """, (store_id, start_time, end_time))
    unique_visitors = cur.fetchone()[0] or 0

    if unique_visitors == 0:
        cur.close()
        conn.close()
        return {
            "total_unique_customers": 0,
            "total_transactions": 0,
            "conversion_rate_percentage": 0.0,
            "avg_dwell_time_minutes": 0.0
        }

    # 2. Extract Average Dwell Time per Unique Customer Session
    cur.execute("""
        SELECT AVG(dwell_ms) 
        FROM store_events 
        WHERE store_id = %s 
          AND timestamp BETWEEN %s AND %s
          AND event_type = 'ZONE_DWELL';
    """, (store_id, start_time, end_time))
    avg_dwell_ms = cur.fetchone()[0] or 0
    avg_dwell_mins = (avg_dwell_ms / 1000) / 60

    # 3. Time-Window Correlation (No Customer IDs) Strategy
    # Pull all unique non-staff visitors who were detected in the BILLING_ZONE
    cur.execute("""
        SELECT visitor_id, timestamp 
        FROM store_events
        WHERE store_id = %s 
          AND event_type = 'BILLING_QUEUE_JOIN'
          AND timestamp BETWEEN %s AND %s
          AND is_staff = FALSE;
    """, (store_id, start_time, end_time))
    billing_visitors = cur.fetchall()

    # Pull all actual sales transactions registered inside the store
    cur.execute("""
        SELECT transaction_id, timestamp, amount 
        FROM pos_transactions
        WHERE store_id = %s 
          AND timestamp BETWEEN %s AND %s;
    """, (store_id, start_time, end_time))
    transactions = cur.fetchall()

    # Match conversion windows: If a unique visitor was standing in the billing queue
    # in the 5-minute window immediately BEFORE a transaction hit the POS system, 
    # flag that visitor session as a converted purchase.
    converted_visitors = set()
    total_sales_count = len(transactions)

    for tx_id, tx_time, amt in transactions:
        for visitor_id, queue_join_time in billing_visitors:
            # 5-minute lookback envelope
            window_start = tx_time - timedelta(minutes=5)
            if window_start <= queue_join_time <= tx_time:
                converted_visitors.add(visitor_id)
                break # Move to next transaction once a match is found

    conversion_count = len(converted_visitors)
    conversion_rate = (conversion_count / unique_visitors) * 100

    cur.close()
    conn.close()

    return {
        "total_unique_customers": unique_visitors,
        "total_transactions": total_sales_count,
        "conversion_rate_percentage": round(conversion_rate, 2),
        "avg_dwell_time_minutes": round(avg_dwell_mins, 2)
    }