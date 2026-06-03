import os
import redis
from datetime import datetime, timedelta
from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from db import init_db
from metrics import calculate_store_metrics
from funnel import calculate_store_funnel
from anomalies import evaluate_realtime_anomalies

app = FastAPI(
    title="Apex Retail Store Intelligence API",
    description="Unified store telemetry API exposing live occupancy, queue, conversion, funnel, anomalies, and heatmap data.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_HOST = os.getenv("REDIS_HOST", "redis-cache")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


@app.on_event("startup")
def startup_event() -> None:
    init_db()


@app.get("/stores/{store_id}/metrics")
def get_store_metrics(
    store_id: str,
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
):
    if not end_time:
        end_time = datetime.utcnow()
    if not start_time:
        start_time = end_time - timedelta(hours=24)

    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        redis_stats = client.hgetall(f"store:{store_id}:live_metrics")
        if not redis_stats:
            redis_stats = {}
        metrics = calculate_store_metrics(store_id, start_time, end_time)
        return {
            "store_id": store_id,
            "occupancy": int(redis_stats.get("occupancy", metrics.get("total_unique_customers", 0))),
            "queue_depth": int(redis_stats.get("queue_depth", 0)),
            "conversion_rate": float(redis_stats.get("conversion_rate", metrics.get("conversion_rate_percentage", 0.0))),
            "total_unique_customers": metrics.get("total_unique_customers", 0),
            "avg_dwell_time_minutes": metrics.get("avg_dwell_time_minutes", 0.0),
            "avg_queue_wait_time_seconds": metrics.get("avg_queue_wait_time_seconds", 0.0),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Metrics retrieval failed: {exc}")


@app.get("/stores/{store_id}/funnel")
def get_store_funnel(
    store_id: str,
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
):
    if not end_time:
        end_time = datetime.utcnow()
    if not start_time:
        start_time = end_time - timedelta(hours=24)

    try:
        return calculate_store_funnel(store_id, start_time, end_time)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Funnel retrieval failed: {exc}")


@app.get("/stores/{store_id}/anomalies")
def get_store_anomalies(store_id: str):
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        return {"store_id": store_id, "active_anomalies": evaluate_realtime_anomalies(store_id, client)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Anomaly retrieval failed: {exc}")


@app.get("/stores/{store_id}/heatmaps")
def get_store_heatmaps(store_id: str, limit: int = 1000):
    limit = min(max(limit, 1), 10000)
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        entries = client.lrange(f"store:{store_id}:heatmap_coordinates", -limit, -1)
        coordinates = []
        for entry in entries:
            try:
                payload = json.loads(entry)
                coordinates.append({
                    "x": float(payload.get("x", 0.0)),
                    "y": float(payload.get("y", 0.0)),
                    "timestamp": payload.get("timestamp"),
                })
            except ValueError:
                continue
        return {"store_id": store_id, "coordinate_count": len(coordinates), "coordinates": coordinates}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Heatmap retrieval failed: {exc}")
