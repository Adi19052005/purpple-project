You can paste the following directly as your `README.md`.

# Store Intelligence Platform

A real-time retail analytics platform that processes CCTV footage, generates behavioral events, streams them through Kafka, computes store intelligence metrics, and visualizes insights through a live dashboard.

---

## Architecture

![Architecture Diagram](docs/architecture.png)

> Save the architecture image you shared as `docs/architecture.png` and keep this reference in the README.

### System Flow

```text
Raw CCTV Clips
        │
        ▼
YOLOv8 Detection + ByteTrack Tracking
        │
        ▼
Visitor Session & Event Generation
        │
        ▼
Kafka Event Stream
        │
        ▼
FastAPI Intelligence Layer
        │
 ┌──────┼─────────┐
 ▼      ▼         ▼
Postgres Redis  WebSockets
 ▼      ▼         ▼
Metrics  Cache  Live Dashboard
```

---

## Features

### Detection Pipeline

* YOLOv8 person detection
* ByteTrack multi-object tracking
* Entry / Exit detection
* Zone-based tracking
* Dwell time computation
* Queue depth estimation
* Staff filtering
* Session ID generation

### Intelligence API

* Real-time visitor metrics
* Conversion funnel analytics
* Heatmap generation
* Queue monitoring
* Operational anomaly detection
* Health monitoring

### Dashboard

* Live metrics
* Visitor counts
* Funnel visualization
* Zone heatmaps
* Queue analytics

---

## Tech Stack

| Layer            | Technology                |
| ---------------- | ------------------------- |
| CV Pipeline      | YOLOv8, OpenCV, ByteTrack |
| Streaming        | Kafka                     |
| Backend          | FastAPI                   |
| Database         | PostgreSQL                |
| Cache            | Redis                     |
| Containerization | Docker Compose            |
| Frontend         | Dashboard UI + WebSockets |

---

## Project Structure

```text
store-intelligence/
│
├── app/
│   ├── main.py
│   ├── metrics.py
│   ├── funnel.py
│   ├── anomalies.py
│   └── db.py
│
├── pipeline/
│   ├── detect.py
│   ├── tracker.py
│   └── zones.py
│
├── data/
│   └── clips/
│       ├── store1/
│       └── store2/
│
├── tests/
│
├── docs/
│   ├── DESIGN.md
│   ├── CHOICES.md
│   └── architecture.png
│
├── docker-compose.yml
└── README.md
```

---

# Setup

## Prerequisites

* Docker Desktop
* Git

Verify:

```bash
docker --version
docker compose version
```

---

## Clone Repository

```bash
git clone <repository-url>
cd store-intelligence
```

---

## Start Entire Platform

```bash
docker compose up --build
```

Expected Services:

```text
PostgreSQL
Redis
Kafka
FastAPI Backend
CV Pipeline
Dashboard
```

---

## Verify Services

```bash
docker compose ps
```

Expected:

```text
apex-postgres-db      Up
apex-redis-cache      Up
apex-kafka-broker     Up
apex-fastapi-backend  Up
apex-web-dashboard    Up
```

---

# Running Detection Pipeline

The system automatically discovers all video clips under:

```text
data/clips/
```

Example:

```text
data/clips/
├── store1/
│   ├── store1_entry.mp4
│   ├── store1_zone_1.mp4
│   ├── store1_zone_2.mp4
│   └── store1_billing.mp4
│
└── store2/
    ├── store2_entry_1.mp4
    ├── store2_entry_2.mp4
    ├── store2_zone.mp4
    └── store2_billing.mp4
```

Run:

```bash
docker compose exec cv-pipeline python detect.py
```

The pipeline:

1. Detects people
2. Tracks visitors
3. Generates visitor sessions
4. Produces events to Kafka
5. Updates metrics in real time

---

# API Endpoints

Base URL:

```text
http://localhost:8000
```

---

## Health Check

### GET /health

Returns platform health.

```http
GET /health
```

Example:

```json
{
  "status": "healthy",
  "stores_active": 2,
  "kafka": "connected",
  "database": "connected"
}
```

---

## Store Metrics

### GET /stores/{store_id}/metrics

Returns real-time store KPIs.

```http
GET /stores/STORE_BLR_002/metrics
```

Response:

```json
{
  "unique_visitors": 128,
  "conversion_rate": 0.34,
  "queue_depth": 5,
  "abandonment_rate": 0.07
}
```

---

## Conversion Funnel

### GET /stores/{store_id}/funnel

```http
GET /stores/STORE_BLR_002/funnel
```

Response:

```json
{
  "entry": 120,
  "zone_visit": 95,
  "billing": 41,
  "purchase": 37
}
```

---

## Heatmap

### GET /stores/{store_id}/heatmap

```http
GET /stores/STORE_BLR_002/heatmap
```

Response:

```json
{
  "zones": [
    {
      "zone": "ZONE_1",
      "visits": 124,
      "avg_dwell": 44
    }
  ]
}
```

---

## Anomalies

### GET /stores/{store_id}/anomalies

```http
GET /stores/STORE_BLR_002/anomalies
```

Response:

```json
{
  "severity": "WARN",
  "anomaly": "QUEUE_SPIKE",
  "suggested_action": "Open additional billing counter"
}
```

---

## Event Ingestion

### POST /events/ingest

```http
POST /events/ingest
```

Used internally by Kafka consumers.

---

# Dashboard

Dashboard URL:

```text
http://localhost:8080
```

Available Widgets:

* Visitor Metrics
* Zone Heatmap
* Funnel Analytics
* Queue Monitoring
* Real-Time Updates

---

# Event Schema

Example Event:

```json
{
  "event_id": "uuid",
  "store_id": "STORE_BLR_002",
  "camera_id": "CAM_ENTRY_01",
  "visitor_id": "VIS_001",
  "event_type": "ENTRY",
  "timestamp": "2026-03-03T14:22:10Z",
  "zone_id": "ZONE_1",
  "confidence": 0.91,
  "is_staff": false
}
```

---

# Testing

Run:

```bash
pytest
```

Coverage:

```bash
pytest --cov
```

---

# Docker Commands

Rebuild:

```bash
docker compose build
```

Restart:

```bash
docker compose restart
```

View Logs:

```bash
docker compose logs -f
```

Stop:

```bash
docker compose down
```

Remove Volumes:

```bash
docker compose down --volumes
```

---

# AI-Assisted Development

AI tools were used for:

* Event schema design
* Detection pipeline refinement
* Kafka streaming architecture
* FastAPI endpoint design
* Test generation and validation

Detailed discussion is available in:

```text
docs/DESIGN.md
docs/CHOICES.md
```

---

## North Star Metric

```text
Offline Store Conversion Rate

Visitors Who Purchased
────────────────────────
Total Unique Visitors
```

The entire platform is designed to improve the accuracy and actionability of this metric.
