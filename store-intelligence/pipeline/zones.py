import cv2
import numpy as np

class StoreZoneManager:
    def __init__(self):
        # Coordinates defining spatial regions on the 1080p camera canvas matrix
        # (These are typically parsed out from your mounted data/store_layout.json file)
        self.zones = {
            "SKINCARE": np.array([[100, 300], [500, 300], [500, 900], [100, 900]], dtype=np.int32),
            "FRAGRANCE": np.array([[600, 200], [1200, 200], [1200, 700], [600, 700]], dtype=np.int32),
            "BILLING_ZONE": np.array([[200, 850], [800, 850], [800, 1050], [200, 1050]], dtype=np.int32)
        }

    def check_zone_containment(self, bottom_center_coord):
        """
        Runs a precise ray-casting geometric calculation to check if a visitor's 
        bottom center point is located inside any defined zone polygon boundary.
        """
        x, y = bottom_center_coord
        
        for zone_name, polygon in self.zones.items():
            # cv2.pointPolygonTest returns:
            # +1 if point is inside, 0 if on the edge, -1 if outside
            is_inside = cv2.pointPolygonTest(polygon, (float(x), float(y)), False)
            if is_inside >= 0:
                return zone_name
                
        return None