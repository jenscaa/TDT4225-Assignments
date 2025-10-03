import sys
import os
from tabulate import tabulate
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from repository.taxi_repository import TaxiRepository
from repository.trips_repository import TripsRepository

class TaxiAnalysisService:
    """
    Service class for taxi data analysis operations
    """
    
    def __init__(self):
        self.taxi_repo = TaxiRepository()
        self.trips_repo = TripsRepository()
    
    def question_1_basic_counts(self):
        """Question 1: How many taxis, trips, and total GPS points are there?"""
        print("=" * 60)
        print("QUESTION 1: Basic Counts")
        print("=" * 60)
        
        try:
            total_taxis = self.taxi_repo.get_total_taxis()
            total_trips = self.trips_repo.get_total_trips()
            total_gps_points = self.trips_repo.get_total_gps_points()
            
            results = [
                ["Metric", "Count"],
                ["Total Taxis", f"{total_taxis:,}"],
                ["Total Trips", f"{total_trips:,}"],
                ["Total GPS Points", f"{total_gps_points:,}"]
            ]
            
            print(tabulate(results, headers="firstrow", tablefmt="grid"))
            
        except Exception as e:
            print(f"Error in question 1: {e}")
    
    def question_2_average_trips_per_taxi(self):
        """Question 2: What is the average number of trips per taxi?"""
        print("\n" + "=" * 60)
        print("QUESTION 2: Average Trips per Taxi")
        print("=" * 60)
        
        try:
            avg_trips = self.trips_repo.get_average_trips_per_taxi()
            
            print(f"Average trips per taxi: {avg_trips:.2f}")
            
        except Exception as e:
            print(f"Error in question 2: {e}")
    
    def question_3_top_20_taxis(self):
        """Question 3: List the top 20 taxis with the most trips"""
        print("\n" + "=" * 60)
        print("QUESTION 3: Top 20 Taxis by Trip Count")
        print("=" * 60)
        
        try:
            top_taxis = self.taxi_repo.get_top_taxis_by_trips(20)
            
            results = [["Rank", "Taxi ID", "Trip Count"]]
            for i, (taxi_id, trip_count) in enumerate(top_taxis, 1):
                results.append([i, taxi_id, f"{trip_count:,}"])
            
            print(tabulate(results, headers="firstrow", tablefmt="grid"))
            
        except Exception as e:
            print(f"Error in question 3: {e}")
    
    def question_4_call_type_analysis(self):
        """Question 4: Call type analysis"""
        print("\n" + "=" * 60)
        print("QUESTION 4: Call Type Analysis")
        print("=" * 60)
        
        try:
            # 4a) Most used call type per taxi
            print("\n4a) Most used call type per taxi:")
            most_used_call_types = self.taxi_repo.get_most_used_call_type_per_taxi()
            
            call_type_summary = {}
            for taxi_id, call_type, count in most_used_call_types:
                if call_type not in call_type_summary:
                    call_type_summary[call_type] = 0
                call_type_summary[call_type] += 1
            
            print("Taxi count by most used call type:")
            for call_type, taxi_count in call_type_summary.items():
                print(f"  Call type {call_type}: {taxi_count} taxis")
            
            # 4b) Call type statistics
            print("\n4b) Call type statistics:")
            call_stats = self.trips_repo.get_call_type_statistics()
            
            results = [["Call Type", "Trip Count", "Avg Duration (min)", "Avg Distance (m)", 
                       "00-06", "06-12", "12-18", "18-24"]]
            
            for row in call_stats:
                call_type, trip_count, avg_duration, avg_distance, t1, t2, t3, t4 = row
                total_trips = trip_count
                results.append([
                    call_type,
                    f"{trip_count:,}",
                    f"{avg_duration/60:.1f}",
                    f"{avg_distance:.1f}",
                    f"{t1:,} ({t1/total_trips*100:.1f}%)",
                    f"{t2:,} ({t2/total_trips*100:.1f}%)",
                    f"{t3:,} ({t3/total_trips*100:.1f}%)",
                    f"{t4:,} ({t4/total_trips*100:.1f}%)"
                ])
            
            print(tabulate(results, headers="firstrow", tablefmt="grid"))
            
        except Exception as e:
            print(f"Error in question 4: {e}")
    
    def question_5_hours_and_distance(self):
        """Question 5: Taxis with most hours and distance driven"""
        print("\n" + "=" * 60)
        print("QUESTION 5: Taxis by Hours and Distance Driven")
        print("=" * 60)
        
        try:
            taxi_stats = self.taxi_repo.get_taxi_hours_and_distance()
            
            results = [["Taxi ID", "Total Hours", "Total Distance (km)"]]
            for taxi_id, hours, distance_m in taxi_stats[:20]:  # Top 20
                results.append([
                    taxi_id,
                    f"{hours:.1f}",
                    f"{distance_m/1000:.1f}"
                ])
            
            print(tabulate(results, headers="firstrow", tablefmt="grid"))
            
        except Exception as e:
            print(f"Error in question 5: {e}")
    
    def question_6_porto_city_hall_trips(self):
        """Question 6: Trips near Porto City Hall"""
        print("\n" + "=" * 60)
        print("QUESTION 6: Trips Near Porto City Hall (within 100m)")
        print("=" * 60)
        
        try:
            near_trips = self.trips_repo.get_trips_near_porto_city_hall(100)
            
            print(f"Found {len(near_trips)} trips within 100m of Porto City Hall")
            
            if near_trips:
                results = [["Trip ID", "Taxi ID", "Start Time", "End Time"]]
                for trip_id, taxi_id, start_time, end_time in near_trips[:20]:  # Show first 20
                    results.append([trip_id, taxi_id, str(start_time), str(end_time)])
                
                print(tabulate(results, headers="firstrow", tablefmt="grid"))
                
                if len(near_trips) > 20:
                    print(f"... and {len(near_trips) - 20} more trips")
            
        except Exception as e:
            print(f"Error in question 6: {e}")
    
    def question_7_invalid_trips(self):
        """Question 7: Invalid trips (fewer than 3 GPS points)"""
        print("\n" + "=" * 60)
        print("QUESTION 7: Invalid Trips")
        print("=" * 60)
        
        try:
            invalid_trips = self.trips_repo.get_invalid_trips()
            
            print(f"Number of invalid trips: {len(invalid_trips)}")
            
            if invalid_trips:
                print("\nFirst 10 invalid trips:")
                results = [["Trip ID", "Taxi ID", "GPS Points", "Start Time", "End Time"]]
                for trip_id, taxi_id, n_points, start_time, end_time in invalid_trips[:10]:
                    results.append([trip_id, taxi_id, n_points, str(start_time), str(end_time)])
                
                print(tabulate(results, headers="firstrow", tablefmt="grid"))
            
        except Exception as e:
            print(f"Error in question 7: {e}")
    
    def question_8_taxi_proximity_pairs(self):
        """Question 8: Taxi proximity pairs"""
        print("\n" + "=" * 60)
        print("QUESTION 8: Taxi Proximity Pairs (within 5m and 5s)")
        print("=" * 60)
        
        try:
            proximity_pairs = self.trips_repo.get_taxi_proximity_pairs_sliding_window(5, 5)
            
            print(f"Found {len(proximity_pairs)} taxi pairs that were within 5m and 5s")
            
            if proximity_pairs:
                results = [["Taxi 1", "Taxi 2", "Proximity Count"]]
                for taxi1, taxi2, count in proximity_pairs[:20]:  # Show first 20
                    results.append([taxi1, taxi2, count])
                
                print(tabulate(results, headers="firstrow", tablefmt="grid"))
                
                if len(proximity_pairs) > 20:
                    print(f"... and {len(proximity_pairs) - 20} more pairs")
            
        except Exception as e:
            print(f"Error in question 8: {e}")
    
    def question_9_midnight_crossing_trips(self):
        """Question 9: Midnight crossing trips"""
        print("\n" + "=" * 60)
        print("QUESTION 9: Midnight Crossing Trips")
        print("=" * 60)
        
        try:
            midnight_trips = self.trips_repo.get_midnight_crossing_trips()
            
            print(f"Number of midnight crossing trips: {len(midnight_trips)}")
            
            if midnight_trips:
                print("\nFirst 10 midnight crossing trips:")
                results = [["Trip ID", "Taxi ID", "Start Time", "End Time"]]
                for trip_id, taxi_id, start_time, end_time in midnight_trips[:10]:
                    results.append([trip_id, taxi_id, str(start_time), str(end_time)])
                
                print(tabulate(results, headers="firstrow", tablefmt="grid"))
            
        except Exception as e:
            print(f"Error in question 9: {e}")
    
    def question_10_circular_trips(self):
        """Question 10: Circular trips (start and end within 50m)"""
        print("\n" + "=" * 60)
        print("QUESTION 10: Circular Trips (start and end within 50m)")
        print("=" * 60)
        
        try:
            circular_trips = self.trips_repo.get_circular_trips(50)
            
            print(f"Number of circular trips: {len(circular_trips)}")
            
            if circular_trips:
                print("\nFirst 10 circular trips:")
                results = [["Trip ID", "Taxi ID", "Start Time", "End Time", "Distance (m)"]]
                for trip_id, taxi_id, start_time, end_time, distance in circular_trips[:10]:
                    results.append([trip_id, taxi_id, str(start_time), str(end_time), f"{distance:.1f}"])
                
                print(tabulate(results, headers="firstrow", tablefmt="grid"))
            
        except Exception as e:
            print(f"Error in question 10: {e}")
    
    def question_11_idle_times(self):
        """Question 11: Average idle time between trips"""
        print("\n" + "=" * 60)
        print("QUESTION 11: Top 20 Taxis by Average Idle Time")
        print("=" * 60)
        
        try:
            idle_times = self.taxi_repo.get_taxi_idle_times()
            
            results = [["Rank", "Taxi ID", "Avg Idle Time (hours)"]]
            for i, (taxi_id, avg_idle_hours) in enumerate(idle_times, 1):
                results.append([i, taxi_id, f"{avg_idle_hours:.2f}"])
            
            print(tabulate(results, headers="firstrow", tablefmt="grid"))
            
        except Exception as e:
            print(f"Error in question 11: {e}")
    
    def run_all_questions(self):
        """Run all analysis questions"""
        print("PORTO TAXI DATA ANALYSIS")
        print("=" * 60)
        print(f"Analysis started at: {datetime.now()}")
        
        try:
            self.question_1_basic_counts()
            self.question_2_average_trips_per_taxi()
            self.question_3_top_20_taxis()
            self.question_4_call_type_analysis()
            self.question_5_hours_and_distance()
            self.question_6_porto_city_hall_trips()
            self.question_7_invalid_trips()
            self.question_8_taxi_proximity_pairs()
            self.question_9_midnight_crossing_trips()
            self.question_10_circular_trips()
            self.question_11_idle_times()
            
            print("\n" + "=" * 60)
            print("ANALYSIS COMPLETED SUCCESSFULLY!")
            print(f"Analysis finished at: {datetime.now()}")
            
        except Exception as e:
            print(f"Error during analysis: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.close_connections()
    
    def close_connections(self):
        """Close all database connections"""
        try:
            self.taxi_repo.close_connection()
            self.trips_repo.close_connection()
        except:
            pass

def main():
    """Main function to run the analysis"""
    service = TaxiAnalysisService()
    service.run_all_questions()

if __name__ == '__main__':
    main()


