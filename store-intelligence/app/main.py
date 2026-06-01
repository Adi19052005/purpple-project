import os
import json
import asyncio
from datetime import datetime, timedelta
from typing import List, Optional
import redis.asyncio as aioredis # Use async variants to avoid blocking event loops
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware

from db import init_db, get_db_connection
from schemas import RetailTelemetryEvent
from metrics import calculate_store_metrics
from funnel import calculate_store_funnel
from anomalies import evaluate_realtime_anomalies

# Initialize FastAPI App instance
app = FastAPI(
    title="Apex Retail Store Intelligence Engine",
    description="Production-aware streaming and analytics API layer.",
    version="1.0.0"
)

# Enforce open CORS configuration to facilitate dashboard communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_HOST = os.getenv("REDIS_HOST", "redis-cache")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Active WebSocket connections list manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                # Handle ghost client dropouts gracefully
                pass

manager = ConnectionManager()

@app.on_event("startup")
async def startup_event():
    """Lifecycle startup hook to initiate schemas and background stream listeners."""
    # Enforce database structure checks
    init_db()
    # Trigger an asynchronous background worker loop task to handle PubSub replication
    asyncio.create_task(redis_pubsub_listener())

async def redis_pubsub_listener():
    """Listens to the Redis Hot Path broadcast channel and mirrors packets out via WebSockets."""
    redis_client = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("live_stream_channel")
    
    print("[*] WebSocket Push Pipeline listening to internal Redis PubSub bus...")
    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                # Push verified JSON payloads downstream instantly to the dashboard
                await manager.broadcast(message["data"])
    except Exception as e:
        print(f"[-] Redis PubSub Async Listener crashed: {e}")
    finally:
        await pubsub.unsubscribe("live_stream_channel")

# ---------------------------------------------------------------------------
# REST ENDPOINTS (Part C Requirements)
# ---------------------------------------------------------------------------

@app.get("/health")
def health_check():
    """Standard uptime checking probe endpoint."""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.post("/events/ingest", status_code=201)
def ingest_bulk_events(events: List[RetailTelemetryEvent]):
    """
    Accepts batches of up to 500 records manually pushed to the API layer.
    Ensures safe database transaction insertion.
    """
    if len(events) > 500:
        raise HTTPException(status_code=400, detail="Batch array length constraint exceeded. Maximum 500 records.")
        
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Instantiate atomic save transaction state block
    try:
        for event in events:
            cur.execute("""
                INSERT INTO store_events (event_id, store_id, camera_id, visitor_id, event_type, timestamp, zone_id, dwell_ms, is_staff, confidence)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING;
            """, (
                event.event_id, event.store_id, event.camera_id, event.visitor_id,
                event.event_type, event.timestamp, event.zone_id, event.dwell_ms,
                event.is_staff, event.confidence
            ))
        conn.commit()
    except Exception as err:
        conn.rollback()
        cur.close()
        conn.close()
        raise HTTPException(status_code=500, detail=f"Database batch transaction execution block failed: {err}")
        
    cur.close()
    conn.close()
    return {"status": "SUCCESS", "ingested_count": len(events)}

@app.get("/stores/{store_id}/metrics")
def get_store_metrics(
    store_id: str,
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None)
):
    """Retrieves unique headcount totals and POS time-correlated conversion profiles."""
    if not end_time: end_time = datetime.utcnow()
    if not start_time: start_time = end_time - timedelta(days=1) # Fallback window frame
    
    return calculate_store_metrics(store_id, start_time, end_time)

@app.get("/stores/{store_id}/funnel")
def get_store_funnel(
    store_id: str,
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None)
):
    """Calculates customer journey retention drop-off steps across sections."""
    if not end_time: end_time = datetime.utcnow()
    if not start_time: start_time = end_time - timedelta(days=1)
    
    return calculate_store_funnel(store_id, start_time, end_time)

@app.get("/stores/{store_id}/anomalies")
def get_store_anomalies(store_id: str):
    """Pulls current live operational variations and logs critical rule spikes."""
    import redis as sync_redis
    r_sync = sync_redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    # Trigger real-time heuristics and compile alerts
    live_alerts = evaluate_realtime_anomalies(store_id, r_sync)
    return {"store_id": store_id, "active_anomalies": live_alerts}

# ---------------------------------------------------------------------------
# WEBSOCKET STREAM ROUTER (Part E Requirements)
# ---------------------------------------------------------------------------

@app.websocket("/ws/stores/{store_id}/telemetry")
async def store_websocket_endpoint(websocket: WebSocket, store_id: str):
    """Persistent bidirectional gateway pipe serving real-time video event drops."""
    await manager.connect(websocket)
    try:
        # Loop indefinitely keeping the connection open
        while True:
            # Sockets can receive control/ping text messages from clients if necessary
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)