import sys
import os
from datetime import datetime

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
    
    def get_taxi_proximity_pairs(self, distance_meters=5, time_seconds=5):
        """Get pairs of taxis that were within specified distance and time"""
        # This is a complex query that would need to be implemented
        # For now, returning a placeholder
        query = """
        SELECT DISTINCT 
            t1.taxi_id as taxi1,
            t2.taxi_id as taxi2,
            COUNT(*) as proximity_count
        FROM trips t1
        JOIN trips t2 ON t1.taxi_id < t2.taxi_id
        WHERE ST_Distance_Sphere(
            ST_GeomFromGeoJSON(t1.polyline),
            ST_GeomFromGeoJSON(t2.polyline)
        ) <= %s
        AND ABS(TIMESTAMPDIFF(SECOND, t1.ts_start, t2.ts_start)) <= %s
        GROUP BY t1.taxi_id, t2.taxi_id
        HAVING COUNT(*) >= 1
        """
        self.cursor.execute(query, (distance_meters, time_seconds))
        return self.cursor.fetchall()
    
    def close_connection(self):
        """Close database connection"""
        self.connection.close_connection()

