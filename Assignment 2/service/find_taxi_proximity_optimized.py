import sys
import os
import json
import math
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from DbConnector import DbConnector

class TaxiProximityFinderOptimized:
    """
    Memory-efficient finder for taxi pairs within 5m and 5s
    Uses temporal bucketing and streaming to handle large datasets
    """
    
    def __init__(self):
        self.connection = DbConnector()
        self.cursor = self.connection.cursor
        self.DISTANCE_THRESHOLD_M = 5  # 5 meters
        self.TIME_THRESHOLD_S = 5      # 5 seconds
        self.TIME_PER_POINT = 15       # seconds between GPS points
        self.TEMPORAL_BUCKET_HOURS = 1 # Process 1 hour of trips at a time
        self.output_file = 'taxi_proximity_results.csv'  # Output file path
        self.checkpoint_file = 'taxi_proximity_checkpoint.txt'  # Checkpoint file
        self.unique_pairs = {}  # Track unique pairs across buckets
        
    def haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two lat/lon points in meters"""
        # Convert to float if needed
        lat1, lon1 = float(lat1), float(lon1)
        lat2, lon2 = float(lat2), float(lon2)
        
        R = 6371000  # Earth radius in meters
        
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        
        a = math.sin(delta_phi/2)**2 + \
            math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        
        return R * c
    
    def circles_overlap(self, lat1, lon1, r1, lat2, lon2, r2):
        """Check if two circles overlap (rough spatial filter)"""
        # Convert Decimal to float if needed
        lat1, lon1, r1 = float(lat1), float(lon1), float(r1)
        lat2, lon2, r2 = float(lat2), float(lon2), float(r2)
        
        distance = self.haversine_distance(lat1, lon1, lat2, lon2)
        return distance <= (r1 + r2 + self.DISTANCE_THRESHOLD_M)
    
    def get_temporal_buckets(self):
        """Get time range and create temporal buckets"""
        print("Analyzing temporal distribution...")
        
        self.cursor.execute("""
        SELECT 
            MIN(ts_start) as min_time,
            MAX(ts_end) as max_time,
            COUNT(*) as trip_count
        FROM Trips
        """)
        
        result = self.cursor.fetchone()
        min_time, max_time, trip_count = result
        
        print(f"Time range: {min_time} to {max_time}")
        print(f"Total trips: {trip_count:,}")
        
        # Create buckets
        buckets = []
        current = min_time
        bucket_delta = timedelta(hours=self.TEMPORAL_BUCKET_HOURS)
        
        while current < max_time:
            bucket_end = current + bucket_delta
            buckets.append((current, bucket_end))
            current = bucket_end
        
        print(f"Created {len(buckets)} temporal buckets of {self.TEMPORAL_BUCKET_HOURS} hour(s) each")
        return buckets
    
    def get_trips_in_bucket(self, start_time, end_time):
        """
        Get trips that overlap with time bucket (without polylines initially)
        Add buffer for TIME_THRESHOLD
        """
        buffer = timedelta(seconds=self.TIME_THRESHOLD_S)
        
        # Use server-side cursor for memory efficiency
        cursor = self.connection.db_connection.cursor()
        
        cursor.execute("""
        SELECT 
            trip_id, 
            taxi_id,
            domain_middle_lat, 
            domain_middle_lon, 
            domain_radius,
            ts_start,
            ts_end,
            start_epoch,
            n_points
        FROM Trips
        WHERE ts_end >= %s AND ts_start <= %s
        ORDER BY taxi_id
        """, (start_time - buffer, end_time + buffer))
        
        return cursor.fetchall()
    
    def get_polyline(self, trip_id):
        """Fetch polyline only when needed"""
        cursor = self.connection.db_connection.cursor()
        cursor.execute("SELECT polyline, start_epoch FROM Trips WHERE trip_id = %s", (trip_id,))
        result = cursor.fetchone()
        cursor.close()
        return json.loads(result[0]), result[1]
    
    def create_spatial_grid(self, trips, grid_size_m=100):
        """
        Group trips into spatial grid cells for faster comparison
        grid_size should be larger than domain radius + threshold
        """
        grid = defaultdict(list)
        
        for trip in trips:
            trip_id, taxi_id, lat, lon, radius, ts_start, ts_end, epoch, n_points = trip
            
            # Convert Decimal to float
            lat = float(lat)
            lon = float(lon)
            radius = float(radius)
            
            # Calculate grid cell (rough approximation)
            # At Porto's latitude (~41°), 1 degree ≈ 111km
            # So for 100m cells: cell_size ≈ 100/111000 ≈ 0.0009 degrees
            cell_size = grid_size_m / 111000
            
            cell_x = int(lon / cell_size)
            cell_y = int(lat / cell_size)
            
            # Add to cell and neighboring cells (to catch overlaps)
            for dx in [-1, 0, 1]:
                for dy in [-1, 0, 1]:
                    grid[(cell_x + dx, cell_y + dy)].append(trip)
        
        return grid
    
    def check_actual_proximity(self, trip1, trip2):
        """Check actual point-to-point proximity"""
        trip_id1, taxi_id1, lat1, lon1, r1, start1, end1, epoch1, n_points1 = trip1
        trip_id2, taxi_id2, lat2, lon2, r2, start2, end2, epoch2, n_points2 = trip2
        
        # Fetch polylines only when needed
        poly1, _ = self.get_polyline(trip_id1)
        poly2, _ = self.get_polyline(trip_id2)
        
        # Create timestamped points
        points1 = []
        for idx, point in enumerate(poly1):
            lon_p, lat_p = point
            timestamp = epoch1 + idx * self.TIME_PER_POINT
            points1.append((lat_p, lon_p, timestamp))
        
        points2 = []
        for idx, point in enumerate(poly2):
            lon_p, lat_p = point
            timestamp = epoch2 + idx * self.TIME_PER_POINT
            points2.append((lat_p, lon_p, timestamp))
        
        # Check all point pairs
        for lat1_p, lon1_p, t1 in points1:
            for lat2_p, lon2_p, t2 in points2:
                # Check time difference
                time_diff = abs(t1 - t2)
                if time_diff > self.TIME_THRESHOLD_S:
                    continue
                
                # Check spatial distance
                distance = self.haversine_distance(lat1_p, lon1_p, lat2_p, lon2_p)
                if distance <= self.DISTANCE_THRESHOLD_M:
                    return True, distance, time_diff
        
        return False, None, None
    
    def save_results_incremental(self, new_pairs):
        """
        Save results incrementally - append new pairs to CSV
        """
        import csv
        import os
        
        # Check if file exists to determine if we need header
        file_exists = os.path.exists(self.output_file)
        
        with open(self.output_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['taxi_id_1', 'taxi_id_2', 'trip_id_1', 'trip_id_2', 'distance_m', 'time_diff_s'])
            
            if not file_exists:
                writer.writeheader()
            
            for pair in new_pairs:
                writer.writerow({
                    'taxi_id_1': pair['taxi_id_1'],
                    'taxi_id_2': pair['taxi_id_2'],
                    'trip_id_1': pair['trip_id_1'],
                    'trip_id_2': pair['trip_id_2'],
                    'distance_m': round(pair['distance_m'], 2),
                    'time_diff_s': round(pair['time_diff_s'], 1)
                })

    def load_checkpoint(self):
        """Load the last processed bucket from checkpoint file"""
        import os
        
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    lines = f.readlines()
                    if lines:
                        last_bucket = int(lines[0].strip())
                        print(f"Found checkpoint: Last completed bucket = {last_bucket}")
                        
                        # Load unique pairs count if available
                        if len(lines) > 1:
                            unique_count = int(lines[1].strip())
                            print(f"Unique pairs so far: {unique_count}")
                        
                        return last_bucket
            except Exception as e:
                print(f"Warning: Could not load checkpoint: {e}")
        
        return 0  # Start from beginning

    def save_checkpoint(self, bucket_idx, total_buckets):
        """Save current progress to checkpoint file"""
        try:
            with open(self.checkpoint_file, 'w') as f:
                f.write(f"{bucket_idx}\n")
                f.write(f"{len(self.unique_pairs)}\n")
                f.write(f"Progress: {bucket_idx}/{total_buckets} ({100*bucket_idx/total_buckets:.1f}%)\n")
        except Exception as e:
            print(f"Warning: Could not save checkpoint: {e}")

    def load_existing_pairs(self):
        """Load existing pairs from output file to avoid duplicates"""
        import os
        import csv
        
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        taxi_pair_key = tuple(sorted([int(row['taxi_id_1']), int(row['taxi_id_2'])]))
                        self.unique_pairs[taxi_pair_key] = {
                            'taxi_pair': taxi_pair_key,
                            'taxi_id_1': int(row['taxi_id_1']),
                            'taxi_id_2': int(row['taxi_id_2']),
                            'trip_id_1': row['trip_id_1'],
                            'trip_id_2': row['trip_id_2'],
                            'distance_m': float(row['distance_m']),
                            'time_diff_s': float(row['time_diff_s'])
                        }
                print(f"Loaded {len(self.unique_pairs)} existing pairs from previous run")
            except Exception as e:
                print(f"Warning: Could not load existing pairs: {e}")

    def process_bucket(self, bucket_idx, start_time, end_time, total_buckets):
        """Process one temporal bucket"""
        print(f"\n--- Bucket {bucket_idx}/{total_buckets} ({100*bucket_idx/total_buckets:.1f}%): {start_time.strftime('%Y-%m-%d %H:%M')} ---")
        
        # Load trips for this bucket (without polylines)
        trips = self.get_trips_in_bucket(start_time, end_time)
        print(f"  Loaded {len(trips)} trips")
        
        if len(trips) < 2:
            return []
        
        # Create spatial grid
        spatial_grid = self.create_spatial_grid(trips)
        print(f"  Created spatial grid with {len(spatial_grid)} cells")
        
        # Find candidates
        candidates = []
        checked_pairs = 0
        
        for cell, cell_trips in spatial_grid.items():
            # Within each cell, compare all pairs
            for i in range(len(cell_trips)):
                trip1 = cell_trips[i]
                trip_id1, taxi_id1, lat1, lon1, r1, start1, end1, epoch1, n_points1 = trip1
                
                # Convert Decimal to float
                lat1, lon1, r1 = float(lat1), float(lon1), float(r1)
                
                for j in range(i + 1, len(cell_trips)):
                    trip2 = cell_trips[j]
                    trip_id2, taxi_id2, lat2, lon2, r2, start2, end2, epoch2, n_points2 = trip2
                    
                    # Convert Decimal to float
                    lat2, lon2, r2 = float(lat2), float(lon2), float(r2)
                    
                    # Skip same taxi
                    if taxi_id1 == taxi_id2:
                        continue
                    
                    checked_pairs += 1
                    
                    # Check if circular domains overlap
                    if not self.circles_overlap(lat1, lon1, r1, lat2, lon2, r2):
                        continue
                    
                    # Check if time ranges overlap
                    buffer = timedelta(seconds=self.TIME_THRESHOLD_S)
                    if end1 + buffer < start2 or end2 + buffer < start1:
                        continue
                    
                    candidates.append((trip1, trip2))
        
        print(f"  Checked {checked_pairs} pairs, found {len(candidates)} candidates")
        
        # Check actual proximity for candidates
        close_pairs = []
        for idx, (trip1, trip2) in enumerate(candidates):
            if idx % 100 == 0 and idx > 0:
                print(f"    Checking candidate {idx}/{len(candidates)}...")
            
            is_close, distance, time_diff = self.check_actual_proximity(trip1, trip2)
            
            if is_close:
                taxi_pair_key = tuple(sorted([trip1[1], trip2[1]]))
                
                # Only add if we haven't seen this taxi pair before
                if taxi_pair_key not in self.unique_pairs:
                    pair_data = {
                        'taxi_pair': taxi_pair_key,
                        'taxi_id_1': trip1[1],
                        'taxi_id_2': trip2[1],
                        'trip_id_1': trip1[0],
                        'trip_id_2': trip2[0],
                        'distance_m': distance,
                        'time_diff_s': time_diff
                    }
                    close_pairs.append(pair_data)
                    self.unique_pairs[taxi_pair_key] = pair_data
        
        print(f"  Found {len(close_pairs)} NEW close pairs in this bucket")
        print(f"  Total unique pairs so far: {len(self.unique_pairs)}")
        
        # Save incrementally
        if close_pairs:
            self.save_results_incremental(close_pairs)
            print(f"  ✓ Saved to {self.output_file}")
        
        # Save checkpoint
        self.save_checkpoint(bucket_idx, total_buckets)
        
        return close_pairs
    
    def find_close_taxi_pairs(self, resume=True):
        """Main function using temporal bucketing"""
        import os
        
        print("="*60)
        print("FINDING CLOSE TAXI PAIRS (OPTIMIZED)")
        print(f"Distance threshold: {self.DISTANCE_THRESHOLD_M}m")
        print(f"Time threshold: {self.TIME_THRESHOLD_S}s")
        print(f"Output file: {self.output_file}")
        print(f"Checkpoint file: {self.checkpoint_file}")
        print("="*60)
        
        # Load checkpoint and existing pairs if resuming
        start_bucket = 0
        if resume:
            start_bucket = self.load_checkpoint()
            if start_bucket > 0:
                self.load_existing_pairs()
                print(f"\n✓ Resuming from bucket {start_bucket + 1}")
        else:
            # Delete old files if starting fresh
            if os.path.exists(self.output_file):
                os.remove(self.output_file)
                print(f"Deleted old {self.output_file}")
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
                print(f"Deleted old {self.checkpoint_file}")
        
        # Get temporal buckets
        buckets = self.get_temporal_buckets()
        total_buckets = len(buckets)
        
        print(f"\nProcessing buckets {start_bucket + 1} to {total_buckets}")
        
        # Process each bucket
        for idx, (start_time, end_time) in enumerate(buckets, 1):
            # Skip already processed buckets
            if idx <= start_bucket:
                continue
            
            try:
                self.process_bucket(idx, start_time, end_time, total_buckets)
            except KeyboardInterrupt:
                print("\n\n⚠ Process interrupted by user")
                print(f"Processed {idx}/{total_buckets} buckets")
                print(f"Results saved to: {self.output_file}")
                print(f"Checkpoint saved. Run again to resume from bucket {idx + 1}")
                break
            except Exception as e:
                print(f"\n⚠ Error in bucket {idx}: {e}")
                import traceback
                traceback.print_exc()
                print("Continuing with next bucket...")
                continue
        
        print(f"\n{'='*60}")
        print(f"FINAL RESULTS")
        print(f"{'='*60}")
        print(f"Total unique taxi pairs found: {len(self.unique_pairs)}")
        print(f"Results saved to: {self.output_file}")
        
        # Clean up checkpoint if completed
        if idx >= total_buckets:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
                print(f"✓ Process completed - checkpoint file removed")
        
        return list(self.unique_pairs.values())
    
    def close_connection(self):
        self.connection.close_connection()


def main():
    finder = None
    try:
        finder = TaxiProximityFinderOptimized()
        
        # Set resume=True to continue from checkpoint, False to start fresh
        results = finder.find_close_taxi_pairs(resume=True)
        
        print(f"\n✓ Complete! Found {len(results)} unique taxi pairs")
        print(f"✓ All results in: {finder.output_file}")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if finder:
            finder.close_connection()


if __name__ == '__main__':
    main()
