import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import cv2
import numpy as np
from kafka import KafkaProducer
from ultralytics import YOLO

from tracker import AdvancedStoreTracker
from zones import StoreZoneManager

STORE_ID = "STORE_BLR_002"
STORE_CODE = "ST1076"
CAMERA_ID = "CAM_MAIN_FLOOR_01"
TOPIC_NAME = "retail-store-telemetry"
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FPS = 15.0
DARK_VALUE_THRESHOLD = 50.0
LOW_SATURATION_THRESHOLD = 40.0
HEATMAP_BATCH_SIZE = 150

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda value: __import__("json").dumps(value).encode("utf-8"),
    acks="all",
)


def format_timestamp(frame_index: int) -> str:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    return (now + timedelta(seconds=(frame_index / FPS))).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_payload(
    event_type: str,
    id_token: str,
    timestamp: str,
    zone_id: Optional[str],
    is_staff: bool,
    confidence: float,
    additional_metadata: Optional[Dict] = None,
    dwell_ms: int = 0,
    gender_pred: Optional[str] = None,
    age_bucket: Optional[str] = None,
    wait_seconds: Optional[float] = None,
    abandoned: bool = False,
) -> Dict:
    metadata = {
        "session_seq": 1,
        "anomalies": [],
    }
    if additional_metadata:
        metadata.update(additional_metadata)

    return {
        "event_id": str(uuid.uuid4()),
        "store_id": STORE_ID,
        "store_code": STORE_CODE,
        "camera_id": CAMERA_ID,
        "id_token": id_token,
        "event_type": event_type,
        "timestamp": timestamp,
        "zone_id": zone_id,
        "dwell_ms": dwell_ms,
        "is_staff": is_staff,
        "confidence": float(round(confidence, 3)),
        "gender_pred": gender_pred,
        "age_bucket": age_bucket,
        "wait_seconds": wait_seconds,
        "abandoned": abandoned,
        "metadata": metadata,
    }


def normalize_coordinate(x: float, y: float, frame_width: int, frame_height: int) -> List[float]:
    return [round(float(x) / float(frame_width), 4), round(float(y) / float(frame_height), 4)]


def main_cv_loop(video_path: str) -> None:
    model = YOLO("yolov8n.pt")
    tracker = AdvancedStoreTracker(entry_line_coords=StoreZoneManager().entry_line)
    zone_manager = StoreZoneManager()

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Unable to open video source: {video_path}")

    frame_index = 0
    zone_state: Dict[int, Optional[str]] = {}
    dwell_start: Dict[int, datetime] = {}
    heatmap_buffer: List[List[float]] = []

    print(f"[*] Starting edge pipeline for {video_path}")

    while True:
        success, frame = cap.read()
        if not success:
            break

        timestamp = format_timestamp(frame_index)
        height, width = frame.shape[:2]
        result = model.track(frame, persist=True, tracker="bytetrack.yaml", classes=[0], verbose=False)
        active_queue_count = 0

        if result and hasattr(result[0], "boxes") and result[0].boxes.id is not None:
            boxes = result[0].boxes.xyxy.cpu().numpy()
            track_ids = result[0].boxes.id.cpu().numpy().astype(int)
            confidences = result[0].boxes.conf.cpu().numpy()

            current_queue_members = []

            for bbox, track_id, confidence in zip(boxes, track_ids, confidences):
                x1, y1, x2, y2 = bbox
                bottom_center = (int((x1 + x2) / 2), int(y2))
                zone_label = zone_manager.check_zone_containment(bottom_center)
                if zone_label == "ZONE_2":
                    current_queue_members.append(track_id)

            active_queue_count = len(current_queue_members)

            for bbox, track_id, confidence in zip(boxes, track_ids, confidences):
                x1, y1, x2, y2 = bbox
                id_token = f"ID_{int(track_id)}"
                bottom_center = (int((x1 + x2) / 2), int(y2))
                is_staff = tracker.classify_staff(frame, bbox, track_id, DARK_VALUE_THRESHOLD, LOW_SATURATION_THRESHOLD)
                zone_label = zone_manager.check_zone_containment(bottom_center)
                previous_zone = zone_state.get(track_id)

                crossing = tracker.is_crossing_line(track_id, bottom_center, current_frame_idx=frame_index)
                if crossing:
                    if crossing == "ENTRY":
                        payload = build_payload(
                            event_type="entry",
                            id_token=id_token,
                            timestamp=timestamp,
                            zone_id="ENTRY_GATE",
                            is_staff=is_staff,
                            confidence=float(confidence),
                            additional_metadata={"queue_depth": active_queue_count},
                        )
                    else:
                        payload = build_payload(
                            event_type="exit",
                            id_token=id_token,
                            timestamp=timestamp,
                            zone_id="EXIT_GATE",
                            is_staff=is_staff,
                            confidence=float(confidence),
                        )
                    producer.send(TOPIC_NAME, value=payload)

                if previous_zone != zone_label:
                    if previous_zone is not None:
                        payload = build_payload(
                            event_type="zone_exit",
                            id_token=id_token,
                            timestamp=timestamp,
                            zone_id=previous_zone,
                            is_staff=is_staff,
                            confidence=float(confidence),
                        )
                        producer.send(TOPIC_NAME, value=payload)
                        dwell_start.pop(track_id, None)

                    if zone_label is not None:
                        if zone_label == "ZONE_2":
                            payload = build_payload(
                                event_type="queue_join",
                                id_token=id_token,
                                timestamp=timestamp,
                                zone_id=zone_label,
                                is_staff=is_staff,
                                confidence=float(confidence),
                                additional_metadata={
                                    "queue_depth": active_queue_count,
                                    "queue_position_at_join": active_queue_count + 1,
                                },
                            )
                        else:
                            payload = build_payload(
                                event_type="zone_enter",
                                id_token=id_token,
                                timestamp=timestamp,
                                zone_id=zone_label,
                                is_staff=is_staff,
                                confidence=float(confidence),
                            )

                        producer.send(TOPIC_NAME, value=payload)
                        dwell_start[track_id] = datetime.now(timezone.utc)

                    zone_state[track_id] = zone_label

                if zone_label == "ZONE_1" and not is_staff and track_id in dwell_start:
                    elapsed = datetime.now(timezone.utc) - dwell_start[track_id]
                    if elapsed.total_seconds() >= 30:
                        payload = build_payload(
                            event_type="zone_dwell",
                            id_token=id_token,
                            timestamp=timestamp,
                            zone_id=zone_label,
                            dwell_ms=int(elapsed.total_seconds() * 1000),
                            is_staff=is_staff,
                            confidence=float(confidence),
                        )
                        producer.send(TOPIC_NAME, value=payload)
                        dwell_start[track_id] = datetime.now(timezone.utc)

                heatmap_buffer.append(normalize_coordinate(bottom_center[0], bottom_center[1], width, height))

        if frame_index > 0 and frame_index % HEATMAP_BATCH_SIZE == 0 and heatmap_buffer:
            payload = build_payload(
                event_type="zone_spatial_matrix",
                id_token="ID_HEATMAP_BATCH",
                timestamp=timestamp,
                zone_id=None,
                is_staff=False,
                confidence=1.0,
                additional_metadata={
                    "spatial_coordinates": heatmap_buffer,
                    "coordinate_count": len(heatmap_buffer),
                    "batch_frame_index": frame_index,
                },
            )
            producer.send(TOPIC_NAME, value=payload)
            heatmap_buffer = []

        frame_index += 1

    cap.release()
    producer.flush()
    print(f"[+] Completed processing {frame_index} frames for {video_path}")


if __name__ == "__main__":
    clips_root = os.path.join(
        os.path.dirname(__file__),
        "..",
        "data",
        "clips"
    )

    video_files = []

    for root, _, files in os.walk(clips_root):
        for file in files:
            if file.lower().endswith(".mp4"):
                video_files.append(os.path.join(root, file))

    if not video_files:
        raise FileNotFoundError(
            f"No MP4 files found under: {clips_root}"
        )

    print(f"[*] Found {len(video_files)} video files")

    for video_path in sorted(video_files):
        print(f"[*] Processing: {video_path}")
        main_cv_loop(video_path)
