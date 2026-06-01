import os
import json
import uuid
from datetime import datetime, timedelta
import cv2
import numpy as np
from ultralytics import YOLO
from kafka import KafkaProducer

from tracker import AdvancedStoreTracker
from zones import StoreZoneManager

# --- Infrastructure Configurations ---
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:29092")
TOPIC_NAME = "retail-store-telemetry"

# Initialize Production Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all' # Enforce strict persistence validation for idempotency integrity
)

# --- Metadata Context Setup ---
STORE_ID = "STORE_BLR_002"
CAMERA_ID = "CAM_MAIN_FLOOR_01"  # Adjusted context for zone tracking
FPS = 15.0
START_TIME_UTC = datetime.utcnow()

# Uniform HSV profile boundaries for staff classification (Teal/Blue Example)
UNIFORM_HSV_LOW = [90, 50, 50]
UNIFORM_HSV_HIGH = [130, 255, 255]

# Virtual threshold line for entry/exit tracking
GATE_LINE = ((200, 800), (1720, 800))

def compute_frame_timestamp(frame_index):
    """Calculates precision ISO-8601 UTC timestamp based on structural frame offset."""
    offset_seconds = frame_index / FPS
    target_dt = START_TIME_UTC + timedelta(seconds=offset_seconds)
    return target_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def main_cv_loop(video_path):
    # Initialize YOLOv8 Weights and Module Logic Engines
    model = YOLO("yolov8n.pt")
    tracker = AdvancedStoreTracker(entry_line_coords=GATE_LINE)
    zone_manager = StoreZoneManager()
    
    cap = cv2.VideoCapture(video_path)
    frame_idx = 0
    
    # Active state caches to track frame-by-frame delta movements
    # track_id -> "ZONE_NAME" or None
    visitor_zone_states = {}
    # track_id -> timestamp_of_last_dwell_emit
    visitor_dwell_timers = {}

    print(f"[*] Ingestion Pipeline started for Video Source: {video_path}")

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
            
        timestamp_str = compute_frame_timestamp(frame_idx)
        current_time_dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
        
        # Invoke YOLOv8 + Native ByteTrack Execution
        # Class 0 enforces tracking exclusively on the 'person' bounding box array
        results = model.track(frame, persist=True, tracker="bytetrack.yaml", classes=[0], verbose=False)
        
        # Maintain active queue depth headcount matrix per frame
        active_billing_queue_count = 0
        
        if results[0].boxes and results[0].boxes.id is not None:
            boxes = results[0].boxes.xyxy.cpu().numpy()
            track_ids = results[0].boxes.id.cpu().numpy().astype(int)
            confidences = results[0].boxes.conf.cpu().numpy()
            
            # First pass: Calculate spatial presence variables for global frame states
            for bbox in boxes:
                x1, y1, x2, y2 = bbox
                b_center = (int((x1 + x2) / 2), int(y2))
                if zone_manager.check_zone_containment(b_center) == "BILLING_ZONE":
                    active_billing_queue_count += 1

            # Second pass: Process individual lifecycle tracking loops
            for bbox, track_id, conf in zip(boxes, track_ids, confidences):
                x1, y1, x2, y2 = bbox
                bottom_center = (int((x1 + x2) / 2), int(y2))
                
                # 1. Uniform matching for Staff Filter classification
                is_staff = tracker.classify_staff(frame, bbox, track_id, UNIFORM_HSV_LOW, UNIFORM_HSV_HIGH)
                visitor_id = f"VIS_{track_id:06d}"
                
                # Base Schema Generator Blueprint
                def create_base_payload(event_type, zone_id=None, dwell_ms=0):
                    return {
                        "event_id": str(uuid.uuid4()),
                        "store_id": STORE_ID,
                        "camera_id": CAMERA_ID,
                        "visitor_id": visitor_id,
                        "event_type": event_type,
                        "timestamp": timestamp_str,
                        "zone_id": zone_id,
                        "dwell_ms": dwell_ms,
                        "is_staff": is_staff,
                        "confidence": float(round(conf, 2)),
                        "metadata": {
                            "queue_depth": active_billing_queue_count if zone_id == "BILLING_ZONE" else None,
                            "sku_zone": zone_id if zone_id in ["SKINCARE", "FRAGRANCE"] else None,
                            "session_seq": 1
                        }
                    }

                # 2. Check Line Boundary Intersections (Entry / Exit Directionality)
                direction_event = tracker.is_crossing_line(track_id, bottom_center)
                if direction_event:
                    payload = create_base_payload(direction_event)
                    producer.send(TOPIC_NAME, value=payload)

                # 3. Dynamic Polygon Containment Evaluations (Zone Changes)
                assigned_zone = zone_manager.check_zone_containment(bottom_center)
                previous_zone = visitor_zone_states.get(track_id)
                
                if assigned_zone != previous_zone:
                    # Case A: Left a distinct retail zone
                    if previous_zone is not None:
                        payload = create_base_payload("ZONE_EXIT", zone_id=previous_zone)
                        producer.send(TOPIC_NAME, value=payload)
                        # Clear active tracking timers
                        visitor_dwell_timers.pop(track_id, None)
                    
                    # Case B: Entered a brand new retail zone boundary
                    if assigned_zone is not None:
                        event_type = "BILLING_QUEUE_JOIN" if assigned_zone == "BILLING_ZONE" else "ZONE_ENTER"
                        payload = create_base_payload(event_type, zone_id=assigned_zone)
                        producer.send(TOPIC_NAME, value=payload)
                        # Instantiate tracking reference clock
                        visitor_dwell_timers[track_id] = current_time_dt
                        
                    visitor_zone_states[track_id] = assigned_zone
                
                # 4. Process Interval Dwell Accumulators (Fulfills the 30s heartbeats rule)
                if assigned_zone is not None and track_id in visitor_dwell_timers:
                    start_dwell_time = visitor_dwell_timers[track_id]
                    elapsed_delta = current_time_dt - start_dwell_time
                    
                    # If tracked person remains inside polygon space for > 30 seconds interval marks
                    if elapsed_delta >= timedelta(seconds=30):
                        payload = create_base_payload(
                            "ZONE_DWELL", 
                            zone_id=assigned_zone, 
                            dwell_ms=int(elapsed_delta.total_seconds() * 1000)
                        )
                        producer.send(TOPIC_NAME, value=payload)
                        # Reset heartbeat timer mark to avoid immediate double-firing next frame
                        visitor_dwell_timers[track_id] = current_time_dt
        
        frame_idx += 1
        
    cap.release()
    producer.flush()
    print(f"[+] Processing completed successfully for frame indices up to: {frame_idx}")

if __name__ == "__main__":
    # Point directly to the standard file structure pipeline mount path
    video_target = "/app/data/clips/store_blr_002_floor.mp4"
    if os.path.exists(video_target):
        main_cv_loop(video_target)
    else:
        print(f"[-] Video file asset context not found at target: {video_target}")