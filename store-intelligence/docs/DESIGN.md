# Architectural Design Document: Spatial Store Intelligence Platform

## System Overview
This platform delivers real-time instore physical retail intelligence using edge-deployed computer vision tracking cross-referenced against transactional data layers. The system isolates employee foot traffic using customized lower-bound color filter masks to prevent distortion of customer metrics, aggregates sequential path positions into batch bursts to reduce stream payload overhead, and models live metrics including dynamic customer count, queue length variations, and real-time checkout conversion rates.

## Component Layout & Network Flow
The application architecture comprises three microservices running in an isolated subnetwork layer managed by Docker Compose:

1. **Edge CV Pipeline (`pipeline/detect.py`)**: Executes target frame decoding via YOLOv8 tracking primitives and state tracking boundaries. It standardizes dynamic object tracking data into localized data envelopes before dispatching them directly into the internal queue.
2. **Streaming Event Broker & Ingestion Engine (`app/ingestion.py`)**: Intercepts the stream telemetry. This component manages time-to-live structures inside a fast memory cache to handle customer re-entries smoothly and logs events into persistent relational tables.
3. **Rest API Service Provider (`app/main.py`)**: Provides unified query abstractions built on high-performance frameworks to feed visual data straight to web layout frontends over public loopbacks.

## AI-Assisted Decisions & Generative Development Log
During the development of this repository, automated coding assistants were utilized to expedite scaffolding, design database schemas, and optimize spatial intersection vector math. 

Key AI-assisted system choices include:
* **Mathematical Vector Isolation Crossings**: Generative prompt iterations were leveraged to engineer the Counter-Clockwise (CCW) line-intersection verification function in the tracker module to prevent dropped frame states.
* **Batch Stream Pipeline Aggregators**: Code assistants structured the buffer cache logic to compress raw x-y points into nested matrix arrays every 150 frames, reducing network requests to the message broker.
* **Cross-Origin Framework Interceptor Policies**: AI was used to ensure reliable frontend-to-backend data exchange by implementing standardized token validation and cross-origin resource sharing filters.