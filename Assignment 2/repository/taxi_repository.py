import sys
import os
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from DbConnector import DbConnector

class TaxiRepository:
    """
    Repository class for taxi-related database operations
    """
    
    def __init__(self):
        self.connection = DbConnector()
        self.cursor = self.connection.cursor
    
    def get_total_taxis(self):
        """Get total number of taxis"""
        query = "SELECT COUNT(*) FROM taxis"
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
    
    def get_taxi_trip_counts(self):
        """Get trip counts for each taxi"""
        query = """
        SELECT taxi_id, COUNT(*) as trip_count
        FROM trips
        GROUP BY taxi_id
        ORDER BY trip_count DESC
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def get_top_taxis_by_trips(self, limit=20):
        """Get top taxis by number of trips"""
        query = """
        SELECT taxi_id, COUNT(*) as trip_count
        FROM trips
        GROUP BY taxi_id
        ORDER BY trip_count DESC
        LIMIT %s
        """
        self.cursor.execute(query, (limit,))
        return self.cursor.fetchall()
    
    def get_most_used_call_type_per_taxi(self):
        """Get most used call type for each taxi"""
        query = """
        SELECT taxi_id, call_type, COUNT(*) as usage_count
        FROM trips
        GROUP BY taxi_id, call_type
        HAVING COUNT(*) = (
            SELECT MAX(trip_count)
            FROM (
                SELECT COUNT(*) as trip_count
                FROM trips t2
                WHERE t2.taxi_id = Trips.taxi_id
                GROUP BY t2.call_type
            ) as max_counts
        )
        ORDER BY taxi_id
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def get_taxi_hours_and_distance(self):
        """Get total hours and distance for each taxi"""
        query = """
        SELECT 
            taxi_id,
            SUM(TIMESTAMPDIFF(SECOND, ts_start, ts_end)) / 3600.0 as total_hours,
            SUM(trip_distance_m) as total_distance_meters
        FROM Trips
        GROUP BY taxi_id
        ORDER BY total_hours DESC
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def get_taxi_idle_times(self):
        """Get idle times between consecutive trips for each taxi"""
        query = """
        WITH TripTimes AS (
            SELECT 
                taxi_id,
                ts_end,
                LEAD(ts_start) OVER (PARTITION BY taxi_id ORDER BY ts_start) as next_trip_start
            FROM trips
        )
        SELECT 
            taxi_id,
            AVG(TIMESTAMPDIFF(SECOND, ts_end, next_trip_start)) / 3600.0 as avg_idle_hours
        FROM TripTimes
        WHERE next_trip_start IS NOT NULL
        GROUP BY taxi_id
        ORDER BY avg_idle_hours DESC
        LIMIT 20
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def close_connection(self):
        """Close database connection"""
        self.connection.close_connection()
