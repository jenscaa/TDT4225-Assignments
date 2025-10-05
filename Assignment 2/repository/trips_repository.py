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
            call_type,
            COUNT(*) as trip_count,
            AVG(TIMESTAMPDIFF(SECOND, ts_start, ts_end)) as avg_duration_seconds,
            AVG(ST_Length(ST_GeomFromGeoJSON(polyline))) as avg_distance_meters,
            SUM(CASE WHEN HOUR(ts_start) BETWEEN 0 AND 5 THEN 1 ELSE 0 END) as trips_00_06,
            SUM(CASE WHEN HOUR(ts_start) BETWEEN 6 AND 11 THEN 1 ELSE 0 END) as trips_06_12,
            SUM(CASE WHEN HOUR(ts_start) BETWEEN 12 AND 17 THEN 1 ELSE 0 END) as trips_12_18,
            SUM(CASE WHEN HOUR(ts_start) BETWEEN 18 AND 23 THEN 1 ELSE 0 END) as trips_18_24
        FROM trips
        GROUP BY call_type
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def get_trips_near_porto_city_hall(self, distance_meters=100):
        """Get trips that passed within specified distance of Porto City Hall"""
        porto_lat, porto_lon = 41.15794, -8.62911
        
        query = """
        SELECT trip_id, taxi_id, ts_start, ts_end
        FROM trips
        WHERE ST_Distance_Sphere(
            ST_GeomFromGeoJSON(polyline),
            ST_GeomFromText('POINT(%s %s)', 4326)
        ) <= %s
        """
        self.cursor.execute(query, (porto_lon, porto_lat, distance_meters))
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
            ST_Distance_Sphere(
                ST_StartPoint(ST_GeomFromGeoJSON(polyline)),
                ST_EndPoint(ST_GeomFromGeoJSON(polyline))
            ) as start_end_distance
        FROM trips
        WHERE ST_Distance_Sphere(
            ST_StartPoint(ST_GeomFromGeoJSON(polyline)),
            ST_EndPoint(ST_GeomFromGeoJSON(polyline))
        ) <= %s
        """
        self.cursor.execute(query, (distance_meters,))
        return self.cursor.fetchall()
    
    def get_taxi_proximity_candidates(self, distance_meters=5, time_window_seconds=5, 
                                  time_start=None, time_end=None):
        """
        Get CANDIDATE pairs using spatial filtering.
        Still need Python to check actual point-by-point distances.
        """
        query = """
        SELECT 
            t1.trip_id, t1.taxi_id, t1.start_epoch, t1.polyline,
            t2.trip_id, t2.taxi_id, t2.start_epoch, t2.polyline
        FROM Trips t1
        JOIN Trips t2 ON t1.taxi_id < t2.taxi_id
        WHERE 
            -- Temporal filter (narrow window)
            t1.ts_start BETWEEN %s AND %s
            AND t2.ts_start BETWEEN %s AND %s
            -- Domain overlap filter (spatial pre-filter)
            AND (t1.domain_radius + t2.domain_radius + %s) >= 
                ST_Distance_Sphere(
                    POINT(t1.domain_middle_lon, t1.domain_middle_lat),
                    POINT(t2.domain_middle_lon, t2.domain_middle_lat)
                )
            -- Temporal overlap
            AND t1.ts_end >= DATE_SUB(t2.ts_start, INTERVAL %s SECOND)
            AND t2.ts_end >= DATE_SUB(t1.ts_start, INTERVAL %s SECOND)
        """
        self.cursor.execute(query, (
            time_start, time_end, time_start, time_end,
            distance_meters, time_window_seconds, time_window_seconds
        ))
        return self.cursor.fetchall()

    def get_taxi_proximity_pairs_sliding_window(self):
        """
        Find taxi pairs within distance and time using a proper sliding window (OVER ... RANGE ...).
        Fix: cast epoch to DATETIME for RANGE INTERVAL; use numeric +/- for the join.
        """
        query = """
        WITH candidates AS (
            SELECT
                t1.taxi_id AS taxi1,
                t2.taxi_id AS taxi2,
                LEAST(t1.point_timestamp, t2.point_timestamp) AS ts_epoch,
                ST_Distance_Sphere(
                    POINT(t1.longitude, t1.latitude),
                    POINT(t2.longitude, t2.latitude)
                ) AS distance_m
            FROM gps_points t1
            JOIN gps_points t2
              ON t2.taxi_id > t1.taxi_id
             AND t2.point_timestamp BETWEEN t1.point_timestamp - %s AND t1.point_timestamp + %s
             AND t2.latitude  BETWEEN t1.latitude  - 0.000045 AND t1.latitude  + 0.000045
             AND t2.longitude BETWEEN t1.longitude - 0.000060 AND t1.longitude + 0.000060
        ),
        kept AS (
            SELECT
                taxi1,
                taxi2,
                ts_epoch,
                FROM_UNIXTIME(ts_epoch) AS ts_dt,
                distance_m,
                ROW_NUMBER() OVER (
                    PARTITION BY taxi1, taxi2, ts_epoch
                    ORDER BY distance_m
                ) AS rn
            FROM candidates
            WHERE distance_m <= %s
        ),
        windowed AS (
            SELECT
                taxi1,
                taxi2,
                ts_dt,
                distance_m,
                COUNT(*) OVER (
                    PARTITION BY taxi1, taxi2
                    ORDER BY ts_dt
                    RANGE BETWEEN INTERVAL %s SECOND PRECEDING
                              AND     INTERVAL %s SECOND FOLLOWING
                ) AS cnt_in_window
            FROM kept
            WHERE rn = 1
        )
        SELECT
            taxi1,
            taxi2,
            COUNT(*)        AS proximity_count,
            MIN(distance_m) AS min_distance,
            AVG(distance_m) AS avg_distance,
            0               AS min_time_diff,
            0               AS avg_time_diff
        FROM windowed
        GROUP BY taxi1, taxi2
        ORDER BY proximity_count DESC
        """
        # distance_meters = 5, time_window_seconds = 5
        self.cursor.execute(query, (5, 5, 5, 5, 5))
        return self.cursor.fetchall()


    def get_taxi_proximity_pairs_sliding_window_historical(self,
        overall_start_timestamp_unix=None,
        overall_end_timestamp_unix=None,
        window_size_seconds=3600,
        overlap_seconds=5,
        batch_size=10000
    ):
        """
        Batched historical run over a date range (defaults to 2014-06-01 full day), 10k rows per fetch.
        Uses sargable time-range join and streams in batches.
        """
        if overall_start_timestamp_unix is None:
            overall_start_timestamp_unix = int(datetime(2014, 6, 1, 0, 0, 0).timestamp())
        if overall_end_timestamp_unix is None:
            overall_end_timestamp_unix = int(datetime(2014, 6, 1, 23, 59, 59).timestamp())

        all_candidate_pairs = []

        current_window_start_dt = datetime.fromtimestamp(overall_start_timestamp_unix)
        overall_end_dt = datetime.fromtimestamp(overall_end_timestamp_unix)

        while current_window_start_dt <= overall_end_dt:
            window_end_dt = current_window_start_dt + timedelta(seconds=window_size_seconds)
            query_window_end_dt = window_end_dt + timedelta(seconds=overlap_seconds)
            if query_window_end_dt > overall_end_dt:
                query_window_end_dt = overall_end_dt + timedelta(seconds=5)

            start_unix = int(current_window_start_dt.timestamp())
            end_unix = int(query_window_end_dt.timestamp())

            query = """
            SELECT
                t1.taxi_id AS taxi_id_1,
                t2.taxi_id AS taxi_id_2,
                t1.point_timestamp AS timestamp_1,
                t2.point_timestamp AS timestamp_2,
                t1.longitude AS longitude_1,
                t1.latitude  AS latitude_1,
                t2.longitude AS longitude_2,
                t2.latitude  AS latitude_2,
                CAST(t2.point_timestamp AS SIGNED) - CAST(t1.point_timestamp AS SIGNED) AS time_diff_seconds
            FROM gps_points t1
            STRAIGHT_JOIN gps_points t2
              ON t2.taxi_id > t1.taxi_id
             AND t2.point_timestamp BETWEEN t1.point_timestamp - 5 AND t1.point_timestamp + 5
            WHERE t1.point_timestamp >= %s
              AND t1.point_timestamp <  %s
              AND t2.point_timestamp >= %s
              AND t2.point_timestamp <= %s
            ORDER BY t1.point_timestamp, t1.taxi_id
            """
            self.cursor.execute(query, (start_unix, end_unix, start_unix - 5, end_unix + 5))

            while True:
                rows = self.cursor.fetchmany(batch_size)
                if not rows:
                    break
                all_candidate_pairs.extend(rows)

            current_window_start_dt += timedelta(seconds=window_size_seconds)

        return all_candidate_pairs
    
    def close_connection(self):
        """Close database connection"""
        self.connection.close_connection()

