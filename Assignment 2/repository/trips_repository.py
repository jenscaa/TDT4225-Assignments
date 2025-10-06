import sys
import os
from datetime import datetime, timedelta
# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from DbConnector import DbConnector

class TripsRepository:
    """
    Repository class for trips-related database operations
    """



    def __init__(self):
        self.connection = DbConnector()
        self.cursor = self.connection.cursor

    def get_total_trips(self):
        """Get total number of trips"""
        query = "SELECT COUNT(*) FROM trips"
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
    
    def get_total_gps_points(self):
        """Get total number of GPS points across all trips"""
        query = "SELECT SUM(n_points) FROM trips"
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
    
    def get_average_trips_per_taxi(self):
        """Get average number of trips per taxi"""
        query = """
        SELECT AVG(trip_count) as avg_trips_per_taxi
        FROM (
            SELECT COUNT(*) as trip_count
            FROM trips
            GROUP BY taxi_id
        ) as taxi_trip_counts
        """
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
    
    def get_call_type_statistics(self):
        """Get statistics for each call type"""
        query = """
        SELECT 
            t.call_type,
            COUNT(*) as trip_count,
            AVG(TIMESTAMPDIFF(SECOND, t.ts_start, t.ts_end)) as avg_duration_seconds,
            AVG(COALESCE(t.trip_distance_m, 0)) as avg_distance_meters,
            SUM(CASE WHEN HOUR(t.ts_start) BETWEEN 0 AND 5 THEN 1 ELSE 0 END) as trips_00_06,
            SUM(CASE WHEN HOUR(t.ts_start) BETWEEN 6 AND 11 THEN 1 ELSE 0 END) as trips_06_12,
            SUM(CASE WHEN HOUR(t.ts_start) BETWEEN 12 AND 17 THEN 1 ELSE 0 END) as trips_12_18,
            SUM(CASE WHEN HOUR(t.ts_start) BETWEEN 18 AND 23 THEN 1 ELSE 0 END) as trips_18_24
        FROM trips t
        GROUP BY t.call_type
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def get_trips_near_porto_city_hall(self, distance_meters=100):
        """Get trips that passed within specified distance of Porto City Hall"""
        porto_lat, porto_lon = 41.15794, -8.62911
        
        query = """
        SELECT DISTINCT t.trip_id, t.taxi_id, t.ts_start, t.ts_end
        FROM trips t
        INNER JOIN gps_points gp ON t.trip_id = gp.trip_id
        WHERE (
            6371000 * ACOS(
                LEAST(1.0, GREATEST(-1.0,
                    COS(RADIANS(gp.latitude)) * COS(RADIANS(%s)) * 
                    COS(RADIANS(%s) - RADIANS(gp.longitude)) + 
                    SIN(RADIANS(gp.latitude)) * SIN(RADIANS(%s))
                ))
            )
        ) <= %s
        """
        self.cursor.execute(query, (porto_lat, porto_lon, porto_lat, distance_meters))
        return self.cursor.fetchall()
    
    def get_invalid_trips(self):
        """Get trips with fewer than 3 GPS points"""
        query = """
        SELECT trip_id, taxi_id, n_points, ts_start, ts_end
        FROM trips
        WHERE n_points < 3
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def get_midnight_crossing_trips(self):
        """Get trips that started on one day and ended on the next"""
        query = """
        SELECT trip_id, taxi_id, ts_start, ts_end
        FROM trips
        WHERE DATE(ts_start) != DATE(ts_end)
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def get_circular_trips(self, distance_meters=50):
        """Get trips where start and end points are within specified distance"""
        query = """
        SELECT 
            trip_id, 
            taxi_id, 
            ts_start, 
            ts_end,
            distance_start_end_m as start_end_distance
        FROM trips
        WHERE distance_start_end_m <= %s
        """
        self.cursor.execute(query, (distance_meters,))
        return self.cursor.fetchall()
    
  

 

        
    def get_taxi_proximity_pairs_optimized(self, start_date='2014-06-01 00:00:00', 
                                       end_date='2014-06-30 23:59:59',
                                       distance_meters=5, 
                                       time_window_seconds=5,
                                       chunk_days=3):
        """
        Optimized proximity query with trip-level pre-filtering and chunking.
        Uses Trips table domains to dramatically reduce GPS point join size.
        """
        from datetime import datetime, timedelta
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        
        # Process in chunks to manage memory
        all_results = []
        current_dt = start_dt
        chunk_num = 1
        
        total_start = datetime.now()
        
        while current_dt < end_dt:
            chunk_end_dt = min(current_dt + timedelta(days=chunk_days), end_dt)
            
            print(f"\n{'='*70}")
            print(f"CHUNK {chunk_num}: {current_dt.date()} to {chunk_end_dt.date()}")
            print(f"{'='*70}")
            
            chunk_results = self._process_proximity_chunk(
                current_dt.strftime('%Y-%m-%d %H:%M:%S'),
                chunk_end_dt.strftime('%Y-%m-%d %H:%M:%S'),
                distance_meters,
                time_window_seconds
            )
            
            all_results.extend(chunk_results)
            current_dt = chunk_end_dt
            chunk_num += 1
        
        # Merge results
        print(f"\n{'='*70}")
        print("MERGING RESULTS FROM ALL CHUNKS")
        print(f"{'='*70}")
        merged_results = self._merge_chunk_results(all_results)
        
        total_time = (datetime.now() - total_start).total_seconds()
        print(f"✓ Total processing time: {total_time:.1f}s ({total_time/60:.1f} min)")
        print(f"✓ Final result: {len(merged_results)} unique taxi pairs")
        print(f"{'='*70}\n")
        
        return merged_results


    def _process_proximity_chunk(self, start_date, end_date, distance_meters, time_window_seconds):
        """
        Process single chunk with 3-phase optimization:
        1. Filter trip pairs using circular domains
        2. Load GPS points ONLY for candidate trips  
        3. Precise proximity detection with spatial grid
        """
        from datetime import datetime
        import time
        
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S').timestamp())
        end_ts = int(datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S').timestamp())
        chunk_start = time.time()
        
        # === PHASE 1: Trip-level domain filtering ===
        print("Phase 1: Filtering trips by domain overlap...")
        phase1_start = time.time()
        
        # Find trips with overlapping domains (time + space)
        self.cursor.execute("""
            WITH candidate_trips AS (
                SELECT 
                    t1.trip_id AS trip_id1,
                    t2.trip_id AS trip_id2,
                    t1.taxi_id AS taxi_id1,
                    t2.taxi_id AS taxi_id2,
                    t1.start_epoch AS start1,
                    t2.start_epoch AS start2,
                    (t1.start_epoch + t1.n_points * 15) AS end1,
                    (t2.start_epoch + t2.n_points * 15) AS end2
                FROM Trips t1
                INNER JOIN Trips t2
                    ON t2.taxi_id > t1.taxi_id
                    -- Time overlap check
                    AND t2.start_epoch <= (t1.start_epoch + t1.n_points * 15 + %s)
                    AND (t2.start_epoch + t2.n_points * 15) >= (t1.start_epoch - %s)
                    -- Spatial domain overlap (BETWEEN for better index usage)
                    AND t2.domain_middle_lat BETWEEN 
                        t1.domain_middle_lat - ((t1.domain_radius + t2.domain_radius + %s) / 111320)
                        AND t1.domain_middle_lat + ((t1.domain_radius + t2.domain_radius + %s) / 111320)
                    AND t2.domain_middle_lon BETWEEN 
                        t1.domain_middle_lon - ((t1.domain_radius + t2.domain_radius + %s) / (111320 * COS(RADIANS(t1.domain_middle_lat))))
                        AND t1.domain_middle_lon + ((t1.domain_radius + t2.domain_radius + %s) / (111320 * COS(RADIANS(t1.domain_middle_lat))))
                WHERE t1.start_epoch BETWEEN %s AND %s
                    AND t1.missing_data = 0
                    AND t2.missing_data = 0
            )
            SELECT trip_id1, trip_id2, taxi_id1, taxi_id2
            FROM candidate_trips
        """, (
            time_window_seconds,   # param 1: time overlap end
            time_window_seconds,   # param 2: time overlap start
            distance_meters,       # param 3: lat lower bound
            distance_meters,       # param 4: lat upper bound
            distance_meters,       # param 5: lon lower bound
            distance_meters,       # param 6: lon upper bound
            start_ts,              # param 7: WHERE start_epoch >=
            end_ts                 # param 8: WHERE start_epoch <=
        ))
        
        candidate_pairs = self.cursor.fetchall()
        phase1_time = time.time() - phase1_start
        
        print(f"  ✓ Found {len(candidate_pairs):,} candidate trip pairs ({phase1_time:.2f}s)")
        
        if len(candidate_pairs) == 0:
            print("  → No candidates, skipping GPS processing")
            return []
        
        # Extract unique taxi IDs from candidates
        candidate_taxis = set()
        for _, _, taxi1, taxi2 in candidate_pairs:
            candidate_taxis.add(taxi1)
            candidate_taxis.add(taxi2)
        
        print(f"  ✓ Involves {len(candidate_taxis)} unique taxis")
        
        # === PHASE 2: Optimized CTE-based proximity detection ===
        print("Phase 2: Running proximity detection with CTE...")
        phase2_start = time.time()
        
        # Build the optimized CTE query with trip filtering
        query = f"""
        WITH 
        -- Load ONLY GPS points from candidate taxis
        filtered_gps AS (
            SELECT 
                taxi_id,
                point_timestamp,
                latitude,
                longitude,
                FLOOR(latitude * 10000) AS lat_cell,
                FLOOR(longitude * 10000) AS lon_cell
            FROM gps_points
            WHERE point_timestamp BETWEEN %s AND %s
                AND taxi_id IN ({','.join(map(str, candidate_taxis))})
        ),
        -- Spatial join with grid cell optimization
        spatial_filtered AS (
            SELECT 
                t1.taxi_id AS taxi1,
                t2.taxi_id AS taxi2,
                t1.point_timestamp AS ts1,
                t2.point_timestamp AS ts2,
                t1.latitude AS lat1,
                t1.longitude AS lon1,
                t2.latitude AS lat2,
                t2.longitude AS lon2,
                (CAST(t2.point_timestamp AS SIGNED) - CAST(t1.point_timestamp AS SIGNED)) AS time_diff
            FROM filtered_gps t1
            INNER JOIN filtered_gps t2 
                ON t2.taxi_id > t1.taxi_id
                -- Time window
                AND (CAST(t2.point_timestamp AS SIGNED) - CAST(t1.point_timestamp AS SIGNED)) BETWEEN -%s AND %s
                -- Grid cell proximity (only join adjacent cells)
                AND t2.lat_cell BETWEEN t1.lat_cell - 1 AND t1.lat_cell + 1
                AND t2.lon_cell BETWEEN t1.lon_cell - 1 AND t1.lon_cell + 1
                -- Fine bounding box
                AND t2.latitude BETWEEN t1.latitude - 0.00005 AND t1.latitude + 0.00005
                AND t2.longitude BETWEEN t1.longitude - 0.00007 AND t1.longitude + 0.00007
            WHERE t1.point_timestamp BETWEEN %s AND %s
        ),
        -- Distance calculation
        distance_filtered AS (
            SELECT 
                taxi1,
                taxi2,
                ts1,
                ts2,
                time_diff,
                (6371000 * ACOS(
                    LEAST(1.0, GREATEST(-1.0,
                        COS(RADIANS(lat1)) * COS(RADIANS(lat2)) * 
                        COS(RADIANS(lon2) - RADIANS(lon1)) + 
                        SIN(RADIANS(lat1)) * SIN(RADIANS(lat2))
                    ))
                )) AS distance_m
            FROM spatial_filtered
            HAVING distance_m <= %s
        ),
        -- Deduplicate
        proximity_events AS (
            SELECT DISTINCT
                taxi1,
                taxi2,
                ts1,
                distance_m,
                time_diff
            FROM distance_filtered
        )
        -- Aggregate
        SELECT 
            taxi1,
            taxi2,
            COUNT(*) AS proximity_count,
            MIN(distance_m) AS min_distance_m,
            AVG(distance_m) AS avg_distance_m,
            MIN(ABS(time_diff)) AS min_time_diff_s,
            AVG(ABS(time_diff)) AS avg_time_diff_s
        FROM proximity_events
        GROUP BY taxi1, taxi2
        HAVING proximity_count > 0
        ORDER BY proximity_count DESC, min_distance_m ASC
        """
        
        self.cursor.execute(query, (
            start_ts - time_window_seconds,
            end_ts + time_window_seconds,
            time_window_seconds,
            start_ts,
            end_ts,
            distance_meters
        ))
        
        results = self.cursor.fetchall()
        phase2_time = time.time() - phase2_start
        
        # Stats
        chunk_time = time.time() - chunk_start
        print(f"  ✓ Phase 2 complete ({phase2_time:.2f}s)")
        print(f"  ✓ Found {len(results)} proximity pairs")
        print(f"  ✓ Chunk total: {chunk_time:.2f}s")
        
        if results:
            top = results[0]
            print(f"  → Top: Taxis {top[0]} & {top[1]}: {top[2]} events, {top[3]:.1f}m min")
        
        return results


    def _merge_chunk_results(self, chunk_results):
        """Merge proximity results from multiple chunks."""
        from collections import defaultdict
        
        merged = defaultdict(lambda: {
            'proximity_count': 0,
            'min_distance_m': float('inf'),
            'sum_distances': 0,
            'distance_count': 0,
            'min_time_diff_s': float('inf'),
            'sum_time_diffs': 0,
            'time_diff_count': 0
        })
        
        for result in chunk_results:
            taxi1, taxi2, count, min_dist, avg_dist, min_time, avg_time = result
            key = (taxi1, taxi2)
            
            merged[key]['proximity_count'] += count
            merged[key]['min_distance_m'] = min(merged[key]['min_distance_m'], min_dist)
            merged[key]['sum_distances'] += avg_dist * count
            merged[key]['distance_count'] += count
            merged[key]['min_time_diff_s'] = min(merged[key]['min_time_diff_s'], min_time)
            merged[key]['sum_time_diffs'] += avg_time * count
            merged[key]['time_diff_count'] += count
        
        final_results = []
        for (taxi1, taxi2), data in merged.items():
            final_results.append((
                taxi1,
                taxi2,
                data['proximity_count'],
                data['min_distance_m'],
                data['sum_distances'] / data['distance_count'] if data['distance_count'] > 0 else 0,
                data['min_time_diff_s'],
                data['sum_time_diffs'] / data['time_diff_count'] if data['time_diff_count'] > 0 else 0
            ))
        
        final_results.sort(key=lambda x: (-x[2], x[3]))
        return final_results


    def _ensure_proximity_indexes(self):
        """Create optimal indexes if they don't exist."""
        print("Ensuring indexes...")
        
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_trips_domain ON Trips(start_epoch, domain_middle_lat, domain_middle_lon)",
            "CREATE INDEX IF NOT EXISTS idx_gps_taxi_time ON gps_points(taxi_id, point_timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_gps_time ON gps_points(point_timestamp)",
        ]
        
        for sql in indexes:
            try:
                self.cursor.execute(sql)
            except Exception as e:
                if "Duplicate" not in str(e):
                    print(f"  Index warning: {e}")
        
        self.db_connection.commit()
        print("✓ Indexes ready\n")
    
    def close_connection(self):
        """Close database connection"""
        self.connection.close_connection()

