import cv2
import numpy as np
from typing import Optional, Tuple


class StoreZoneManager:
    def __init__(self, frame_width: int = 1920, frame_height: int = 1080):
        self.frame_width = frame_width
        self.frame_height = frame_height
        self.zones = {
            "ZONE_1": np.array(
                [
                    [int(frame_width * 0.05), int(frame_height * 0.10)],
                    [int(frame_width * 0.45), int(frame_height * 0.10)],
                    [int(frame_width * 0.45), int(frame_height * 0.55)],
                    [int(frame_width * 0.05), int(frame_height * 0.55)]
                ],
                dtype=np.int32,
            ),
            "ZONE_2": np.array(
                [
                    [int(frame_width * 0.60), int(frame_height * 0.65)],
                    [int(frame_width * 0.98), int(frame_height * 0.65)],
                    [int(frame_width * 0.98), int(frame_height * 0.98)],
                    [int(frame_width * 0.60), int(frame_height * 0.98)]
                ],
                dtype=np.int32,
            ),
        }
        self.entry_line = ((int(frame_width * 0.02), int(frame_height * 0.88)),
                           (int(frame_width * 0.25), int(frame_height * 0.88)))

    def check_zone_containment(self, bottom_center: Tuple[int, int]) -> Optional[str]:
        x, y = bottom_center
        for zone_name, polygon in self.zones.items():
            if cv2.pointPolygonTest(polygon, (float(x), float(y)), False) >= 0:
                return zone_name
        return None

    def is_in_entry_gate(self, bottom_center: Tuple[int, int]) -> bool:
        x, y = bottom_center
        x_min = 0
        y_min = int(self.frame_height * 0.80)
        x_max = int(self.frame_width * 0.28)
        y_max = self.frame_height
        return x_min <= x <= x_max and y_min <= y <= y_max
