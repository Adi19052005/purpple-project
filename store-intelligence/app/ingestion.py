import os
import json
import redis
from kafka import KafkaConsumer
from datetime import datetime

from db import get_db_connection
from schemas import RetailTelemetryEvent

# --- Environment Properties ---
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:29092")
TOPIC_NAME = "retail-store-telemetry"

REDIS_HOST = os.getenv("REDIS_HOST", "redis-cache")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Initialize state cache connection
r_cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def start_ingestion_worker():
    """Persistent consumer loop extracting, validating, and cataloging Kafka event payloads."""
    print(f"[*] Ingestion Background Worker starting. Listening to Kafka Topic: '{TOPIC_NAME}'...")
    
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        group_id="analytics-backend-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    db_conn = get_db_connection()
    db_cur = db_conn.cursor()
    
    for message in consumer:
        event_data = message.value
        
        try:
            # 1. Enforce Guardrail Schema Verification via Pydantic Contract
            validated_event = RetailTelemetryEvent(**event_data)
            
            # 2. Strict Idempotency Check (Part C Gate)
            # Enforce 24-hour timeout window logic to catch duplicated transaction drops
            idempotency_key = f"evt_id:{validated_event.event_id}"
            if r_cache.set(idempotency_key, "PROCESSED", nx=True, ex=86400) is None:
                print(f"[!] Duplicate Event Dropped via Idempotency Core: {validated_event.event_id}")
                continue
                
            # 3. Hot Path State Management Strategy (Redis Updates)
            # Maintain active headcount states instantly for zero database load lag
            if validated_event.event_type == "BILLING_QUEUE_JOIN":
                r_cache.hset(f"store:{validated_event.store_id}:live_metrics", "queue_depth", validated_event.metadata.queue_depth or 0)
                
            # 4. Cold Storage Writes (PostgreSQL Ingestion)
            db_cur.execute("""
                INSERT INTO store_events (event_id, store_id, camera_id, visitor_id, event_type, timestamp, zone_id, dwell_ms, is_staff, confidence)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING;
            """, (
                validated_event.event_id,
                validated_event.store_id,
                validated_event.camera_id,
                validated_event.visitor_id,
                validated_event.event_type,
                validated_event.timestamp,
                validated_event.zone_id,
                validated_event.dwell_ms,
                validated_event.is_staff,
                validated_event.confidence
            ))
            
            db_conn.commit()
            
            # --- Dynamic Push Event Trigger (Fulfills Part E Dashboard Updates) ---
            # This is where we hook into the WebSocket server broker to push events downstream
            broadcast_live_telemetry(validated_event)
            
        except Exception as err:
            # Prevent single faulty validation frame from bricking the entire analytical server loop
            print(f"[-] Critical ingestion failure processing row record packet: {err}")
            db_conn.rollback()

def broadcast_live_telemetry(event: RetailTelemetryEvent):
    """PubSub Broadcast hook notifying dashboard instances over low latency memory channels."""
    # Use Redis PubSub channel as a light orchestration network to decouple sockets
    event_json = event.model_dump_json()
    r_cache.publish("live_stream_channel", event_json)