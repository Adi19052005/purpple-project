import numpy as np
import cv2

class AdvancedStoreTracker:
    def __init__(self, entry_line_coords=None):
        """
        entry_line_coords: Tuple of ((x1, y1), (x2, y2)) representing the gate threshold.
        """
        self.entry_line = entry_line_coords
        self.track_history = {}  # track_id -> list of bottom-center coordinates
        self.staff_cache = set() # Track IDs recognized as staff
        self.reentry_embeddings = {} # visitor_id -> list of historical feature signatures

    def is_crossing_line(self, track_id, bottom_center):
        """
        Determines line crossing orientation using a cross-product vector approach.
        Returns: 'ENTRY', 'EXIT', or None
        """
        if not self.entry_line:
            return None
            
        if track_id not in self.track_history:
            self.track_history[track_id] = []
        
        self.track_history[track_id].append(bottom_center)
        history = self.track_history[track_id]
        
        if len(history) < 2:
            return None
            
        # Line vector (A -> B)
        p1, p2 = np.array(self.entry_line[0]), np.array(self.entry_line[1])
        line_vec = p2 - p1
        
        # Positions
        prev_pos = np.array(history[-2])
        curr_pos = np.array(history[-1])
        
        # Compute cross products to find which side of the line the points are on
        prev_cross = np.cross(line_vec, prev_pos - p1)
        curr_cross = np.cross(line_vec, curr_pos - p1)
        
        # If signs are different, a line-crossing event happened
        if np.sign(prev_cross) != np.sign(curr_cross):
            # Direction check based on sign direction (Assume positive cross product is 'Inbound')
            if curr_cross > 0:
                return "ENTRY"
            else:
                return "EXIT"
                
        return None

    def classify_staff(self, frame, bbox, track_id, hsv_lower_bound, hsv_upper_bound):
        """
        Excludes store employees by scanning uniform color distribution in HSV space.
        """
        if track_id in self.staff_cache:
            return True
            
        x1, y1, x2, y2 = map(int, bbox)
        # Prevent out-of-bounds frame cropping
        h, w, _ = frame.shape
        crop = frame[max(0, y1):min(h, y2), max(0, x1):min(w, x2)]
        
        if crop.size == 0:
            return False
            
        hsv_crop = cv2.cvtColor(crop, cv2.COLOR_BGR2HSV)
        mask = cv2.inRange(hsv_crop, np.array(hsv_lower_bound), np.array(hsv_upper_bound))
        
        # Calculate ratio of uniform color matching inside the bounding box
        matching_pixels = cv2.countNonZero(mask)
        total_pixels = crop.shape[0] * crop.shape[1]
        matching_ratio = matching_pixels / total_pixels
        
        # Threshold: If > 25% matches uniform parameters, classify as staff
        if matching_ratio > 0.25:
            self.staff_cache.add(track_id)
            return True
            
        return False

    def check_reentry(self, current_features, threshold=0.85):
        """
        Compares spatial/appearance features against recently exited visitors 
        to mitigate vendor re-entry session inflation.
        Returns matched visitor_id or None
        """
        # Feature extraction placeholders (Can be mapped to OSNet/TorchReID distances)
        for vid, hist_feats in self.reentry_embeddings.items():
            for feat in hist_feats:
                similarity = np.dot(current_features, feat) / (np.linalg.norm(current_features) * np.linalg.norm(feat))
                if similarity >= threshold:
                    return vid
        return None