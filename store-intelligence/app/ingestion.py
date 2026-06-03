import os
import json
import redis
from datetime import datetime, timedelta
from typing import Optional
from kafka import KafkaConsumer

from db import get_db_connection
from schemas import RetailTelemetryEvent, EventType

if os.getenv("DOCKER_ENV") == "true" or os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
    KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:29092")
    REDIS_HOST = os.getenv("REDIS_HOST", "redis-cache")
else:
    KAFKA_BROKER = "localhost:9092"
    REDIS_HOST = "localhost"

REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
TOPIC_NAME = "retail-store-telemetry"

r_cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

LIVE_METRICS_TEMPLATE = "store:{store_code}:live_metrics"
ACTIVE_VISITORS_TEMPLATE = "store:{store_code}:active_visitors"
UNIQUE_CUSTOMER_ENTRIES_TEMPLATE = "store:{store_code}:unique_nonstaff_entries"
CONVERSION_SET_TEMPLATE = "store:{store_code}:converted_visitors"
BILLING_QUEUE_SET_TEMPLATE = "store:{store_code}:billing_queue_members"
BILLING_JOIN_TS_TEMPLATE = "visitor:{id_token}:billing_join_ts"
EXIT_HOLD_TEMPLATE = "visitor:{id_token}:exit_hold"


def ensure_live_metrics(store_code: str) -> str:
    metrics_key = LIVE_METRICS_TEMPLATE.format(store_code=store_code)
    if not r_cache.exists(metrics_key):
        r_cache.hset(metrics_key, mapping={
            "total_unique_nonstaff_entries": 0,
            "occupancy": 0,
            "queue_depth": 0,
            "total_unique_conversions": 0,
            "conversion_rate": 0.0,
        })
    return metrics_key


def get_current_occupancy(store_code: str) -> int:
    return int(r_cache.scard(ACTIVE_VISITORS_TEMPLATE.format(store_code=store_code)) or 0)


def get_current_queue_depth(store_code: str) -> int:
    return int(r_cache.scard(BILLING_QUEUE_SET_TEMPLATE.format(store_code=store_code)) or 0)


def update_conversion_rate(store_code: str) -> float:
    metrics_key = LIVE_METRICS_TEMPLATE.format(store_code=store_code)
    total_entries = int(r_cache.scard(UNIQUE_CUSTOMER_ENTRIES_TEMPLATE.format(store_code=store_code)) or 0)
    total_conversions = int(r_cache.scard(CONVERSION_SET_TEMPLATE.format(store_code=store_code)) or 0)
    conversion_value = round((total_conversions / total_entries * 100.0) if total_entries > 0 else 0.0, 2)
    r_cache.hset(metrics_key, mapping={
        "total_unique_nonstaff_entries": total_entries,
        "total_unique_conversions": total_conversions,
        "conversion_rate": conversion_value,
        "queue_depth": get_current_queue_depth(store_code),
        "occupancy": get_current_occupancy(store_code),
    })
    return conversion_value


def upsert_visitor_session(event: RetailTelemetryEvent, db_cur) -> None:
    zones_visited = event.zone_id if event.zone_id else None
    billing_entry_time = event.timestamp if event.event_type == EventType.QUEUE_JOIN else None
    db_cur.execute(
        """
            INSERT INTO visitor_sessions (
                id_token, store_id, store_code, first_entry_time, last_activity_time,
                zones_visited, billing_zone_entry_time, total_dwell_ms, is_staff,
                gender_pred, age_bucket, wait_seconds, abandoned
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_token) DO UPDATE SET
                last_activity_time = EXCLUDED.last_activity_time,
                zones_visited = CASE
                    WHEN visitor_sessions.zones_visited IS NULL THEN EXCLUDED.zones_visited
                    WHEN EXCLUDED.zones_visited IS NULL THEN visitor_sessions.zones_visited
                    ELSE visitor_sessions.zones_visited || ',' || EXCLUDED.zones_visited
                END,
                billing_zone_entry_time = COALESCE(EXCLUDED.billing_zone_entry_time, visitor_sessions.billing_zone_entry_time),
                total_dwell_ms = visitor_sessions.total_dwell_ms + EXCLUDED.total_dwell_ms,
                is_staff = EXCLUDED.is_staff,
                gender_pred = COALESCE(EXCLUDED.gender_pred, visitor_sessions.gender_pred),
                age_bucket = COALESCE(EXCLUDED.age_bucket, visitor_sessions.age_bucket),
                wait_seconds = COALESCE(EXCLUDED.wait_seconds, visitor_sessions.wait_seconds),
                abandoned = visitor_sessions.abandoned OR EXCLUDED.abandoned;
        """,
        (
            event.id_token,
            event.store_id,
            event.store_code,
            event.timestamp,
            event.timestamp,
            zones_visited,
            billing_entry_time,
            event.dwell_ms,
            event.is_staff,
            event.gender_pred,
            event.age_bucket,
            event.wait_seconds,
            event.abandoned,
        ),
    )


def log_staff_activity(event: RetailTelemetryEvent, db_cur) -> None:
    metadata_json = json.dumps(event.metadata.model_dump())
    db_cur.execute(
        """
            INSERT INTO staff_activity (
                event_id, store_code, camera_id, id_token, event_type,
                timestamp, zone_id, is_staff, confidence, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING;
        """,
        (
            event.event_id,
            event.store_code,
            event.camera_id,
            event.id_token,
            event.event_type,
            event.timestamp,
            event.zone_id,
            event.is_staff,
            event.confidence,
            metadata_json,
        ),
    )


def process_entry_event(event: RetailTelemetryEvent, db_cur) -> RetailTelemetryEvent:
    if event.is_staff:
        return event

    metrics_key = ensure_live_metrics(event.store_code)
    active_set = ACTIVE_VISITORS_TEMPLATE.format(store_code=event.store_code)
    unique_entries_set = UNIQUE_CUSTOMER_ENTRIES_TEMPLATE.format(store_code=event.store_code)

    if event.event_type == EventType.ENTRY and r_cache.exists(EXIT_HOLD_TEMPLATE.format(id_token=event.id_token)):
        event.event_type = EventType.RE_ENTRY
        event.metadata.session_seq = (event.metadata.session_seq or 1) + 1
        event.metadata.anomalies.append("RE_ENTRY")

    if event.event_type in {EventType.ENTRY, EventType.RE_ENTRY}:
        if event.event_type == EventType.ENTRY and r_cache.sadd(unique_entries_set, event.id_token):
            r_cache.hincrby(metrics_key, "total_unique_nonstaff_entries", 1)

        r_cache.sadd(active_set, event.id_token)
        r_cache.hset(metrics_key, "occupancy", get_current_occupancy(event.store_code))
        upsert_visitor_session(event, db_cur)

    return event


def process_queue_join(event: RetailTelemetryEvent) -> RetailTelemetryEvent:
    if event.event_type != EventType.QUEUE_JOIN or event.is_staff:
        return event

    billing_queue_set = BILLING_QUEUE_SET_TEMPLATE.format(store_code=event.store_code)
    metrics_key = ensure_live_metrics(event.store_code)
    r_cache.sadd(billing_queue_set, event.id_token)
    r_cache.set(BILLING_JOIN_TS_TEMPLATE.format(id_token=event.id_token), event.timestamp.isoformat(), ex=3600)
    queue_depth = get_current_queue_depth(event.store_code)
    r_cache.hset(metrics_key, "queue_depth", queue_depth)
    r_cache.hset(metrics_key, "occupancy", get_current_occupancy(event.store_code))
    return event


def process_exit_event(event: RetailTelemetryEvent, db_cur) -> RetailTelemetryEvent:
    if event.event_type != EventType.EXIT or event.is_staff:
        return event

    ticket_key = BILLING_QUEUE_SET_TEMPLATE.format(store_code=event.store_code)
    active_set = ACTIVE_VISITORS_TEMPLATE.format(store_code=event.store_code)
    metrics_key = ensure_live_metrics(event.store_code)
    r_cache.srem(active_set, event.id_token)
    r_cache.set(EXIT_HOLD_TEMPLATE.format(id_token=event.id_token), "TRUE", ex=600)
    r_cache.hset(metrics_key, "occupancy", get_current_occupancy(event.store_code))

    if r_cache.sismember(ticket_key, event.id_token):
        r_cache.srem(ticket_key, event.id_token)
        join_time_value = r_cache.get(BILLING_JOIN_TS_TEMPLATE.format(id_token=event.id_token))
        if join_time_value:
            try:
                join_time = datetime.fromisoformat(join_time_value)
                dwell_seconds = (event.timestamp - join_time).total_seconds()
                event.wait_seconds = dwell_seconds
                if dwell_seconds >= 15:
                    if r_cache.sadd(CONVERSION_SET_TEMPLATE.format(store_code=event.store_code), event.id_token):
                        r_cache.hincrby(metrics_key, "total_unique_conversions", 1)
                        update_conversion_rate(event.store_code)
            except ValueError:
                pass
        r_cache.delete(BILLING_JOIN_TS_TEMPLATE.format(id_token=event.id_token))

    r_cache.hset(metrics_key, "queue_depth", get_current_queue_depth(event.store_code))
    r_cache.hset(metrics_key, "conversion_rate", update_conversion_rate(event.store_code))
    upsert_visitor_session(event, db_cur)
    return event


def process_heatmap_batch(event: RetailTelemetryEvent, db_cur, db_conn) -> None:
    coordinates = event.metadata.spatial_coordinates or []
    if not coordinates:
        return

    redis_key = f"store:{event.store_code}:heatmap_coordinates"
    timestamp_iso = event.timestamp.isoformat()

    with db_conn.cursor() as batch_cur:
        for coord in coordinates:
            coord_payload = json.dumps({"x": coord[0], "y": coord[1], "timestamp": timestamp_iso})
            r_cache.rpush(redis_key, coord_payload)
            batch_cur.execute(
                """
                    INSERT INTO heatmap_coordinates (
                        store_code, camera_id, id_token, x_normalized, y_normalized, timestamp, batch_frame_index
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event.store_code,
                    event.camera_id,
                    event.id_token,
                    float(coord[0]),
                    float(coord[1]),
                    event.timestamp,
                    event.metadata.batch_frame_index,
                ),
            )

    r_cache.ltrim(redis_key, -10000, -1)
    db_conn.commit()
    print(f"[+] Processed heatmap batch for {event.store_code}, {len(coordinates)} coordinates.")


def process_anomalies(event: RetailTelemetryEvent, db_cur) -> None:
    for anomaly_type in event.metadata.anomalies:
        severity = "CRITICAL" if anomaly_type == "THEFT_SUSPICION_ANOMALY" else "WARNING"
        description = (
            f"Visitor {event.id_token} exited after queue wait without completing checkout."
            if anomaly_type == "LONG_DWELL_ANOMALY"
            else f"Visitor {event.id_token} exited through gate without registering a billing queue join."
        )

        db_cur.execute(
            """
                INSERT INTO store_anomalies (store_code, id_token, anomaly_type, severity, description, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                event.store_code,
                event.id_token,
                anomaly_type,
                severity,
                description,
                event.timestamp,
            ),
        )

        cache_key = f"store:{event.store_code}:anomalies_24h"
        r_cache.lpush(cache_key, json.dumps({
            "anomaly_type": anomaly_type,
            "severity": severity,
            "description": description,
            "timestamp": event.timestamp.isoformat(),
            "id_token": event.id_token,
        }))
        r_cache.expire(cache_key, 86400)
        print(f"[!] Stored anomaly {anomaly_type} for {event.id_token}")


def broadcast_live_telemetry(event: RetailTelemetryEvent) -> None:
    r_cache.publish("live_stream_channel", event.model_dump_json())


def run_ingestion_worker() -> None:
    print(f"[*] Starting ingestion worker against broker {KAFKA_BROKER}")
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            group_id="retail-analytics-ingest",
            auto_offset_reset="earliest",
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        )
    except Exception as exc:
        print(f"[-] Failed to initialize Kafka consumer: {exc}")
        return

    db_conn = get_db_connection()
    db_cur = db_conn.cursor()
    try:
        while True:
            for message in consumer:
                try:
                    payload = message.value
                    event = RetailTelemetryEvent(**payload)
                    event_key = f"evt_id:{event.event_id}"
                    if r_cache.set(event_key, "processed", nx=True, ex=86400) is None:
                        continue

                    if event.event_type == EventType.ZONE_SPATIAL_MATRIX:
                        process_heatmap_batch(event, db_cur, db_conn)
                        continue

                    if event.is_staff:
                        log_staff_activity(event, db_cur)
                        db_conn.commit()
                        broadcast_live_telemetry(event)
                        continue

                    event = process_entry_event(event, db_cur)
                    event = process_queue_join(event)
                    event = process_exit_event(event, db_cur)
                    r_cache.hset(ensure_live_metrics(event.store_code), mapping={
                        "occupancy": get_current_occupancy(event.store_code),
                        "queue_depth": get_current_queue_depth(event.store_code),
                        "conversion_rate": update_conversion_rate(event.store_code),
                    })

                    db_cur.execute(
                        """
                            INSERT INTO store_events (
                                event_id, store_id, store_code, camera_id, id_token,
                                event_type, timestamp, zone_id, dwell_ms, is_staff,
                                confidence, gender_pred, age_bucket, wait_seconds, abandoned
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (event_id) DO NOTHING;
                        """,
                        (
                            event.event_id,
                            event.store_id,
                            event.store_code,
                            event.camera_id,
                            event.id_token,
                            event.event_type,
                            event.timestamp,
                            event.zone_id,
                            event.dwell_ms,
                            event.is_staff,
                            event.confidence,
                            event.gender_pred,
                            event.age_bucket,
                            event.wait_seconds,
                            event.abandoned,
                        ),
                    )

                    if event.metadata.anomalies:
                        process_anomalies(event, db_cur)

                    db_conn.commit()
                    broadcast_live_telemetry(event)
                except Exception as exc:
                    print(f"[-] Ingestion failure: {exc}")
                    db_conn.rollback()
    finally:
        db_cur.close()
        db_conn.close()


if __name__ == "__main__":
    run_ingestion_worker()
