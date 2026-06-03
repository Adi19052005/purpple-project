from enum import Enum
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
import uuid


class EventType(str, Enum):
    ENTRY = "entry"
    EXIT = "exit"
    ZONE_ENTER = "zone_enter"
    ZONE_EXIT = "zone_exit"
    ZONE_DWELL = "zone_dwell"
    QUEUE_JOIN = "queue_join"
    RE_ENTRY = "RE_ENTRY"
    ZONE_SPATIAL_MATRIX = "zone_spatial_matrix"


class ZoneMetadata(BaseModel):
    queue_depth: Optional[int] = Field(None, description="Active queue length when a customer joins billing.")
    queue_position_at_join: Optional[int] = Field(None, description="Position in the billing queue at join time.")
    session_seq: int = Field(1, description="Incremented when a returning visitor triggers a re-entry session.")
    anomalies: List[str] = Field(default_factory=list, description="List of detected anomaly tags.")
    spatial_coordinates: Optional[List[List[float]]] = Field(None, description="Normalized (x,y) history for heatmap batches.")
    coordinate_count: Optional[int] = Field(None, description="Total number of coordinates in the heatmap payload.")
    batch_frame_index: Optional[int] = Field(None, description="Frame index associated with a heatmap batch event.")


class RetailTelemetryEvent(BaseModel):
    event_id: str = Field(..., description="Unique UUID per telemetry event.")
    store_id: str = Field(..., description="Internal store location identifier.")
    store_code: str = Field(..., description="Store branch code such as ST1076 or ST1008.")
    camera_id: str = Field(..., description="Camera source identifier.")
    id_token: str = Field(..., description="Persistent track token prefixed with ID_.")
    event_type: EventType = Field(..., description="The telemetry event classification.")
    timestamp: datetime = Field(..., description="Event timestamp in UTC.")
    zone_id: Optional[str] = Field(None, description="Spatial zone label or gate identifier.")
    dwell_ms: int = Field(0, ge=0, description="Time in zone in milliseconds.")
    is_staff: bool = Field(False, description="Staff filtration marker.")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Normalized detection confidence.")
    gender_pred: Optional[str] = Field(None, description="Optional predicted gender bucket.")
    age_bucket: Optional[str] = Field(None, description="Optional predicted age bucket.")
    wait_seconds: Optional[float] = Field(None, ge=0.0, description="Optional wait time in seconds.")
    abandoned: bool = Field(False, description="Flag for abandoned checkout or queue exit.")
    metadata: ZoneMetadata = Field(default_factory=ZoneMetadata, description="Structured payload metadata.")

    @field_validator("event_id")
    @classmethod
    def validate_uuid_string(cls, value: str) -> str:
        try:
            uuid.UUID(value)
        except ValueError as exc:
            raise ValueError("event_id must be a valid UUID string") from exc
        return value

    @field_validator("id_token")
    @classmethod
    def validate_id_token_prefix(cls, value: str) -> str:
        if not value.startswith("ID_"):
            raise ValueError("id_token must begin with 'ID_'")
        return value
