# Architectural Trade-Offs & Technology Choices

## 1. Detection Model Choice
The pipeline utilizes **YOLOv8n (nano)** paired with an optimized **ByteTrack** profile configuration.

* **Trade-off Analysis**: While larger tracking configurations offer higher precision metrics, they demand dedicated GPU execution loops. YOLOv8n achieves high execution speeds (>30 FPS) on standard CPU infrastructure while maintaining human detection accuracy profiles above 88%. This deployment profile ensures edge capability on lower-spec hardware without breaking retail hardware budgets.

## 2. Event Schema Design
The telemetries are serialized using JSON envelopes over standard binary formats like Avro or Protobuf.

* **Trade-off Analysis**: JSON structures prioritize system observability, debugging speed, and seamless integration with web dashboards. A universal tracking schema ensures flexibility, allowing the system to easily adapt to new features like age group segmentation, gender estimation, and dynamic queue profiling without disrupting existing database pipelines.

## 3. Storage & Cache Layer Architecture
The system uses a hybrid backend strategy combining **Redis** for ephemeral cache management and **PostgreSQL** for durable event logging.

* **Trade-off Analysis**: Handling high-frequency retail spatial data requires high throughput. Managing active metrics like current store count or live queue lengths entirely within relational databases causes structural lock contention. 
* By routing operational counters to memory string caches with 10-minute automated Time-To-Live expiration values, we successfully handle temporary customer exits without double-counting. Meanwhile, PostgreSQL safely stores structured event historical records for deep analytical reporting.