import sys
import os
import json
import math
import csv
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from DbConnector import DbConnector

class TaxiProximityValidator:
    """
    Validates taxi proximity results by fetching actual polylines and timestamps
    to verify that pairs were indeed within 5m and 5s of each other
    """
    
    def __init__(self):
        self.connection = DbConnector()
        self.cursor = self.connection.cursor
        self.DISTANCE_THRESHOLD_M = 5  # 5 meters
        self.TIME_THRESHOLD_S = 5      # 5 seconds
        self.TIME_PER_POINT = 15       # seconds between GPS points
        
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
    
    def get_trip_data(self, trip_id):
        """Fetch polyline and timing data for a specific trip"""
        query = """
        SELECT polyline, start_epoch, ts_start, ts_end, taxi_id, n_points
        FROM trips 
        WHERE trip_id = %s
        """
        self.cursor.execute(query, (trip_id,))
        result = self.cursor.fetchone()
        
        if not result:
            return None
            
        polyline_json, start_epoch, ts_start, ts_end, taxi_id, n_points = result
        polyline = json.loads(polyline_json)
        
        return {
            'trip_id': trip_id,
            'taxi_id': taxi_id,
            'polyline': polyline,
            'start_epoch': start_epoch,
            'ts_start': ts_start,
            'ts_end': ts_end,
            'n_points': n_points
        }
    
    def create_timestamped_points(self, trip_data):
        """Create list of (lat, lon, timestamp) tuples from trip data"""
        points = []
        polyline = trip_data['polyline']
        start_epoch = trip_data['start_epoch']
        
        for idx, point in enumerate(polyline):
            lon, lat = point
            timestamp = start_epoch + idx * self.TIME_PER_POINT
            points.append((lat, lon, timestamp))
            
        return points
    
    def validate_proximity_pair(self, trip_id_1, trip_id_2, expected_distance=None, expected_time_diff=None):
        """
        Validate that two trips were actually within 5m and 5s of each other
        Returns: (is_valid, actual_min_distance, actual_min_time_diff, details)
        """
        # Fetch trip data
        trip1_data = self.get_trip_data(trip_id_1)
        trip2_data = self.get_trip_data(trip_id_2)
        
        if not trip1_data or not trip2_data:
            return False, None, None, f"Could not fetch trip data for {trip_id_1} or {trip_id_2}"
        
        # Ensure different taxis
        if trip1_data['taxi_id'] == trip2_data['taxi_id']:
            return False, None, None, f"Same taxi ID: {trip1_data['taxi_id']}"
        
        # Create timestamped points
        points1 = self.create_timestamped_points(trip1_data)
        points2 = self.create_timestamped_points(trip2_data)
        
        # Find minimum distance and time difference
        min_distance = float('inf')
        min_time_diff = float('inf')
        closest_points = None
        
        for lat1, lon1, t1 in points1:
            for lat2, lon2, t2 in points2:
                # Calculate time difference
                time_diff = abs(t1 - t2)
                
                # Only check spatial distance if within time threshold
                if time_diff <= self.TIME_THRESHOLD_S:
                    distance = self.haversine_distance(lat1, lon1, lat2, lon2)
                    
                    # Update minimums
                    if distance < min_distance:
                        min_distance = distance
                        min_time_diff = time_diff
                        closest_points = {
                            'trip1_point': (lat1, lon1, t1),
                            'trip2_point': (lat2, lon2, t2),
                            'distance': distance,
                            'time_diff': time_diff
                        }
        
        # Check if valid
        is_valid = (min_distance <= self.DISTANCE_THRESHOLD_M and 
                   min_time_diff <= self.TIME_THRESHOLD_S)
        
        details = {
            'trip1': {
                'trip_id': trip_id_1,
                'taxi_id': trip1_data['taxi_id'],
                'start_time': trip1_data['ts_start'],
                'end_time': trip1_data['ts_end'],
                'n_points': trip1_data['n_points']
            },
            'trip2': {
                'trip_id': trip_id_2,
                'taxi_id': trip2_data['taxi_id'],
                'start_time': trip2_data['ts_start'],
                'end_time': trip2_data['ts_end'],
                'n_points': trip2_data['n_points']
            },
            'closest_encounter': closest_points,
            'expected_distance': expected_distance,
            'expected_time_diff': expected_time_diff
        }
        
        return is_valid, min_distance, min_time_diff, details
    
    def validate_results_file(self, results_file='taxi_proximity_results.csv', sample_size=None, detailed_output=False):
        """
        Validate results from CSV file
        sample_size: if specified, only validate first N results
        detailed_output: if True, print detailed information for each validation
        """
        print("="*80)
        print("TAXI PROXIMITY RESULTS VALIDATION")
        print("="*80)
        print(f"Distance threshold: {self.DISTANCE_THRESHOLD_M}m")
        print(f"Time threshold: {self.TIME_THRESHOLD_S}s")
        print(f"Results file: {results_file}")
        if sample_size:
            print(f"Sample size: {sample_size} pairs")
        print("="*80)
        
        valid_count = 0
        invalid_count = 0
        error_count = 0
        total_processed = 0
        
        validation_errors = []
        distance_differences = []
        time_differences = []
        
        try:
            with open(results_file, 'r') as f:
                reader = csv.DictReader(f)
                
                for idx, row in enumerate(reader):
                    if sample_size and idx >= sample_size:
                        break
                    
                    total_processed += 1
                    trip_id_1 = row['trip_id_1']
                    trip_id_2 = row['trip_id_2']
                    expected_distance = float(row['distance_m'])
                    expected_time_diff = float(row['time_diff_s'])
                    
                    print(f"\nValidating pair {total_processed}: {trip_id_1} <-> {trip_id_2}")
                    print(f"  Expected: {expected_distance:.2f}m, {expected_time_diff:.1f}s")
                    
                    try:
                        is_valid, actual_distance, actual_time_diff, details = self.validate_proximity_pair(
                            trip_id_1, trip_id_2, expected_distance, expected_time_diff
                        )
                        
                        if is_valid:
                            valid_count += 1
                            print(f"  ✓ VALID: {actual_distance:.2f}m, {actual_time_diff:.1f}s")
                            
                            # Track differences
                            if actual_distance is not None:
                                distance_differences.append(abs(actual_distance - expected_distance))
                            if actual_time_diff is not None:
                                time_differences.append(abs(actual_time_diff - expected_time_diff))
                                
                        else:
                            invalid_count += 1
                            print(f"  ✗ INVALID: {actual_distance:.2f}m, {actual_time_diff:.1f}s" if actual_distance else "  ✗ INVALID: No close encounter found")
                            
                            validation_errors.append({
                                'pair_index': total_processed,
                                'trip_id_1': trip_id_1,
                                'trip_id_2': trip_id_2,
                                'expected_distance': expected_distance,
                                'expected_time_diff': expected_time_diff,
                                'actual_distance': actual_distance,
                                'actual_time_diff': actual_time_diff,
                                'details': details
                            })
                        
                        if detailed_output:
                            self.print_detailed_validation(details, is_valid)
                            
                    except Exception as e:
                        error_count += 1
                        print(f"  ⚠ ERROR: {e}")
                        validation_errors.append({
                            'pair_index': total_processed,
                            'trip_id_1': trip_id_1,
                            'trip_id_2': trip_id_2,
                            'error': str(e)
                        })
                    
                    # Progress update
                    if total_processed % 10 == 0:
                        print(f"\nProgress: {total_processed} pairs processed")
                        print(f"  Valid: {valid_count}, Invalid: {invalid_count}, Errors: {error_count}")
        
        except FileNotFoundError:
            print(f"ERROR: Results file '{results_file}' not found!")
            return
        
        # Print summary
        print("\n" + "="*80)
        print("VALIDATION SUMMARY")
        print("="*80)
        print(f"Total pairs processed: {total_processed}")
        print(f"Valid pairs: {valid_count} ({100*valid_count/total_processed:.1f}%)")
        print(f"Invalid pairs: {invalid_count} ({100*invalid_count/total_processed:.1f}%)")
        print(f"Errors: {error_count} ({100*error_count/total_processed:.1f}%)")
        
        if distance_differences:
            print(f"\nDistance accuracy:")
            print(f"  Average difference: {sum(distance_differences)/len(distance_differences):.3f}m")
            print(f"  Max difference: {max(distance_differences):.3f}m")
        
        if time_differences:
            print(f"\nTime accuracy:")
            print(f"  Average difference: {sum(time_differences)/len(time_differences):.3f}s")
            print(f"  Max difference: {max(time_differences):.3f}s")
        
        # Print validation errors if any
        if validation_errors and invalid_count > 0:
            print(f"\nFirst 5 validation errors:")
            for i, error in enumerate(validation_errors[:5]):
                if 'error' in error:
                    print(f"  {i+1}. Pair {error['pair_index']}: {error['error']}")
                else:
                    print(f"  {i+1}. Pair {error['pair_index']}: Expected {error['expected_distance']:.2f}m/{error['expected_time_diff']:.1f}s, "
                          f"Got {error['actual_distance']:.2f}m/{error['actual_time_diff']:.1f}s")
        
        return {
            'total_processed': total_processed,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'error_count': error_count,
            'validation_errors': validation_errors
        }
    
    def print_detailed_validation(self, details, is_valid):
        """Print detailed validation information"""
        print(f"    Trip 1: Taxi {details['trip1']['taxi_id']} ({details['trip1']['start_time']} - {details['trip1']['end_time']}, {details['trip1']['n_points']} points)")
        print(f"    Trip 2: Taxi {details['trip2']['taxi_id']} ({details['trip2']['start_time']} - {details['trip2']['end_time']}, {details['trip2']['n_points']} points)")
        
        if details['closest_encounter']:
            encounter = details['closest_encounter']
            print(f"    Closest encounter: {encounter['distance']:.2f}m apart, {encounter['time_diff']:.1f}s time diff")
            print(f"      Point 1: ({encounter['trip1_point'][0]:.6f}, {encounter['trip1_point'][1]:.6f}) at epoch {encounter['trip1_point'][2]}")
            print(f"      Point 2: ({encounter['trip2_point'][0]:.6f}, {encounter['trip2_point'][1]:.6f}) at epoch {encounter['trip2_point'][2]}")
    
    def validate_single_pair(self, trip_id_1, trip_id_2):
        """Validate a single pair of trips"""
        print(f"Validating single pair: {trip_id_1} <-> {trip_id_2}")
        
        is_valid, distance, time_diff, details = self.validate_proximity_pair(trip_id_1, trip_id_2)
        
        print(f"Result: {'VALID' if is_valid else 'INVALID'}")
        if distance is not None:
            print(f"Minimum distance: {distance:.2f}m")
            print(f"Minimum time difference: {time_diff:.1f}s")
        
        self.print_detailed_validation(details, is_valid)
        
        return is_valid, distance, time_diff, details
    
    def close_connection(self):
        """Close database connection"""
        self.connection.close_connection()


def main():
    validator = None
    try:
        validator = TaxiProximityValidator()
        
        # Check command line arguments
        import sys
        if len(sys.argv) > 1:
            if sys.argv[1] == 'single' and len(sys.argv) == 4:
                # Validate single pair
                trip_id_1, trip_id_2 = sys.argv[2], sys.argv[3]
                validator.validate_single_pair(trip_id_1, trip_id_2)
            else:
                print("Usage for single pair: python validate_taxi_proximity_results.py single <trip_id_1> <trip_id_2>")
        else:
            # Validate results file
            results_file = 'taxi_proximity_results.csv'
            
            # Ask user for validation options
            print("Taxi Proximity Results Validator")
            print("1. Validate all results")
            print("2. Validate sample (first N results)")
            print("3. Validate with detailed output")
            
            choice = input("Choose option (1-3): ").strip()
            
            sample_size = None
            detailed_output = False
            
            if choice == '2':
                sample_size = int(input("Enter sample size: "))
            elif choice == '3':
                detailed_output = True
                sample_response = input("Also limit to sample size? (y/N): ").strip().lower()
                if sample_response == 'y':
                    sample_size = int(input("Enter sample size: "))
            
            validator.validate_results_file(
                results_file=results_file,
                sample_size=sample_size,
                detailed_output=detailed_output
            )
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if validator:
            validator.close_connection()


if __name__ == '__main__':
    main()
