import numpy as np
import cv2
from collections import deque

class AdvancedStoreTracker:
    def __init__(self, entry_line_coords=None):
        """
        entry_line_coords: Tuple of ((x1, y1), (x2, y2)) representing the gate threshold.
        """
        self.entry_line = entry_line_coords
        
        # FIX 1: Maxlen=5 bounds memory limits safely while preserving localized historical vectors
        self.track_history = {}      # track_id -> deque of bottom-center coordinates
        self.staff_cache = set()     # Track IDs recognized as staff
        self.reentry_embeddings = {} # visitor_id -> list of historical feature signatures
        
        # FIX 2: Hysteresis cooling map to prevent multiple fake crossing events from coordinate jitter
        self.last_crossing_time = {} # track_id -> index/marker of last fired event

    def is_crossing_line(self, track_id, bottom_center, current_frame_idx=0):
        """
        Determines line crossing orientation using a cross-product vector approach with hysteresis tracking.
        Returns: 'ENTRY', 'EXIT', or None
        """
        if not self.entry_line:
            return None
            
        if track_id not in self.track_history:
            self.track_history[track_id] = deque(maxlen=5)
        
        self.track_history[track_id].append(bottom_center)
        history = list(self.track_history[track_id])
        
        if len(history) < 2:
            return None
            
        # Check if this track ID has recently fired a crossing event within the last 15 frames (1 second at 15FPS)
        if track_id in self.last_crossing_time:
            if current_frame_idx - self.last_crossing_time[track_id] < 15:
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
        if np.sign(prev_cross) != np.sign(curr_cross) and prev_cross != 0:
            self.last_crossing_time[track_id] = current_frame_idx
            
            # Direction check based on sign direction (Assume positive cross product is 'Inbound')
            if curr_cross > 0:
                return "ENTRY"
            else:
                return "EXIT"
                
        return None

    def classify_staff(self, frame, bbox, track_id, dark_value_threshold=50.0, saturation_threshold=40.0):
        """
        Classifies staff by analyzing the clothing region of a tracked person.
        Crops the torso and leg area, converts to HSV, and uses low value plus low saturation
        thresholds to identify black-clad staff uniforms.
        """
        if track_id in self.staff_cache:
            return True

        x1, y1, x2, y2 = map(int, bbox)
        height = y2 - y1
        if height <= 0 or x2 <= x1:
            return False

        top_crop = y1 + int(height * 0.15)
        bottom_crop = y2 - int(height * 0.10)
        if bottom_crop <= top_crop:
            return False

        crop = frame[max(0, top_crop):min(frame.shape[0], bottom_crop), max(0, x1):min(frame.shape[1], x2)]
        if crop.size == 0:
            return False

        hsv_crop = cv2.cvtColor(crop, cv2.COLOR_BGR2HSV)
        avg_v = float(np.mean(hsv_crop[:, :, 2]))
        avg_s = float(np.mean(hsv_crop[:, :, 1]))

        if avg_v < dark_value_threshold and avg_s < saturation_threshold:
            self.staff_cache.add(track_id)
            return True

        return False

    def check_reentry(self, current_features, threshold=0.85):
        """
        Compares spatial/appearance features against recently exited visitors 
        to mitigate vendor re-entry session inflation.
        Returns matched visitor_id or None
        """
        if current_features is None or len(self.reentry_embeddings) == 0:
            return None
            
        current_features = np.array(current_features)
        curr_norm = np.linalg.norm(current_features)
        if curr_norm == 0:
            return None

        for vid, hist_feats in self.reentry_embeddings.items():
            for feat in hist_feats:
                feat = np.array(feat)
                feat_norm = np.linalg.norm(feat)
                if feat_norm == 0:
                    continue
                    
                similarity = np.dot(current_features, feat) / (curr_norm * feat_norm)
                if similarity >= threshold:
                    return vid
        return None

    def prune_track(self, track_id):
        """
        FIX 4: Cleans out dead history paths for tracks dropped out of frame state limits.
        """
        self.track_history.pop(track_id, None)
        self.last_crossing_time.pop(track_id, None)