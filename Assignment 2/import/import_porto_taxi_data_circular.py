import csv
import json
import sys
import os
from datetime import datetime
import time
import mysql.connector  # <-- ADD THIS IMPORT

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from DbConnector import DbConnector

class CircularDomainPortoTaxiDataImporter:
    """
    Porto taxi data importer using circular domain approach for spatial indexing.
    Stores a middle point and radius for each trip instead of H3 cells.
    """

    def __init__(self, batch_size=5000, resume_from_row=0):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.batch_size = batch_size
        self.resume_from_row = resume_from_row
        
        # Domain calculation constants
        self.TIME_PER_POINT = 15  # seconds per GPS point
        self.MAX_SPEED_KMH = 120  # maximum assumed speed in km/h
        self.MAX_SPEED_MS = self.MAX_SPEED_KMH * 1000 / 3600  # convert to m/s (33.33 m/s)
        
        # Batch collection
        self.trips_batch = []
        
        # Statistics
        self.stats = {
            'rows_processed': 0,
            'trips_imported': 0,
            'trips_skipped': 0,
            'total_points': 0,
            'total_radius': 0,
            'start_time': time.time()
        }

    def import_data(self, csv_file_path):
        """
        Main import function with circular domain generation
        """
        print("Starting Circular Domain Porto taxi data import...")
        print(f"Resuming from row {self.resume_from_row}")
        print(f"Batch size: {self.batch_size}")
        print(f"Domain calculation: {self.TIME_PER_POINT}s per point, max speed {self.MAX_SPEED_KMH} km/h")
        print("="*60)
        
        start_time = time.time()
        
        # Step 1: Import taxis first (required for foreign key)
        self.import_taxis(csv_file_path)
        
        # Step 2: Import trips with circular domains
        self.import_trips_with_circular_domains(csv_file_path)
        
        # Step 3: Process any remaining batches
        self._flush_remaining_batches()
        
        end_time = time.time()
        self._print_final_statistics(start_time, end_time)

    def import_taxis(self, csv_file_path):
        """
        Import unique taxi IDs from the CSV
        """
        print("Importing taxis...")
        
        # Get unique taxi IDs from CSV
        taxi_ids = set()
        processed_count = 0
        
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                taxi_ids.add(int(row['TAXI_ID']))
                processed_count += 1
                if processed_count % 10000 == 0:
                    print(f"Processed {processed_count} rows for taxi extraction...")

        # Insert unique taxis in batches
        print("Inserting taxis into database...")
        taxi_list = sorted(list(taxi_ids))
        
        for i in range(0, len(taxi_list), self.batch_size):
            batch = taxi_list[i:i + self.batch_size]
            
            values = [(taxi_id,) for taxi_id in batch]
            self.cursor.executemany(
                "INSERT INTO taxis (taxi_id) VALUES (%s) ON DUPLICATE KEY UPDATE taxi_id = taxi_id",
                values
            )
            self.db_connection.commit()
            print(f"Inserted batch {i//self.batch_size + 1} of taxis ({len(batch)} items)")

        print(f"Imported {len(taxi_ids)} unique taxis")

    def import_trips_with_circular_domains(self, csv_file_path):
        """
        Import trips with JSON polyline and circular domain generation
        """
        print("Importing trips with circular domains...")
        
        batch_count = 0
        last_progress_time = time.time()
        
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                self.stats['rows_processed'] += 1
                
                # Skip rows until we reach the resume point
                if self.stats['rows_processed'] <= self.resume_from_row:
                    continue
                
                # Skip trips with missing data
                if row['MISSING_DATA'].lower() == 'true':
                    self.stats['trips_skipped'] += 1
                    continue
                
                # Process trip
                if self._process_trip_row(row):
                    self.stats['trips_imported'] += 1
                    
                    # Process batches when they reach batch_size
                    if len(self.trips_batch) >= self.batch_size:
                        self._insert_trips_batch()
                        batch_count += 1
                        
                        # Log progress every 5 batches
                        if batch_count % 5 == 0:
                            self._log_progress(batch_count, last_progress_time)
                            last_progress_time = time.time()
                
                # Log progress every 1000 rows
                if self.stats['rows_processed'] % 1000 == 0:
                    self._log_progress(batch_count, last_progress_time)

    def _process_trip_row(self, row):
        """
        Process a single trip row and calculate circular domain
        """
        try:
            # Parse basic trip data
            trip_id = row['TRIP_ID']
            taxi_id = int(row['TAXI_ID'])
            call_type = row['CALL_TYPE']
            daytype = row['DAY_TYPE']
            missing_data = row['MISSING_DATA'].lower() == 'true'
            
            # Parse timestamp
            timestamp_str = row['TIMESTAMP']
            start_epoch = int(timestamp_str)
            ts_start = datetime.fromtimestamp(start_epoch)
            
            # Parse origin fields
            origin_call = None
            origin_stand = None
            
            if call_type == 'A' and row['ORIGIN_CALL']:
                try:
                    origin_call = int(float(row['ORIGIN_CALL']))
                except ValueError:
                    pass
                    
            if call_type == 'B' and row['ORIGIN_STAND']:
                try:
                    origin_stand = int(float(row['ORIGIN_STAND']))
                except ValueError:
                    pass
            
            # Parse polyline JSON
            try:
                polyline = json.loads(row['POLYLINE'])
            except:
                return False
            
            if not polyline or len(polyline) == 0:
                return False
            
            n_points = len(polyline)
            self.stats['total_points'] += n_points
            
            # Calculate time duration and end time
            time_seconds = n_points * self.TIME_PER_POINT
            ts_end = datetime.fromtimestamp(start_epoch + time_seconds)
            
            # Calculate circular domain using the already calculated time
            middle_lat, middle_lon, radius = self._calculate_circular_domain(polyline, time_seconds)
            self.stats['total_radius'] += radius
            
            # Add to trips batch
            self.trips_batch.append((
                trip_id,
                taxi_id,
                call_type,
                origin_call,
                origin_stand,
                ts_start,
                ts_end,
                daytype,
                missing_data,
                n_points,
                json.dumps(polyline),
                middle_lat,
                middle_lon,
                radius,
                start_epoch
            ))
            
            return True
            
        except Exception as e:
            print(f"Error processing trip {row.get('TRIP_ID', 'unknown')}: {e}")
            return False

    def _calculate_circular_domain(self, polyline, time_seconds):
        """
        Calculate circular domain for a polyline.
        Returns: (middle_latitude, middle_longitude, radius_in_meters)
        
        Algorithm:
        1. Find start and end points of polyline
        2. Calculate middle point (midpoint between start and end)
        3. Calculate radius: time_seconds * max_speed (120 km/h = 33.33 m/s)
        
        Args:
            polyline: List of [lon, lat] coordinates
            time_seconds: Pre-calculated trip duration in seconds
        """
        # Get start and end points
        start_point = polyline[0]  # [lon, lat]
        end_point = polyline[-1]    # [lon, lat]
        
        start_lon, start_lat = start_point[0], start_point[1]
        end_lon, end_lat = end_point[0], end_point[1]
        
        # Calculate middle point (simple average for midpoint)
        middle_lat = (start_lat + end_lat) / 2.0
        middle_lon = (start_lon + end_lon) / 2.0
        
        # Calculate radius based on maximum possible distance at max speed
        # This ensures the circle covers all possible paths
        radius_meters = time_seconds * self.MAX_SPEED_MS
        
        return middle_lat, middle_lon, radius_meters

    def _insert_trips_batch(self):
        """
        Insert a batch of trips with circular domains (with auto-reconnect)
        """
        if not self.trips_batch:
            return
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Insert trips
                self.cursor.executemany("""
                INSERT INTO Trips (trip_id, taxi_id, call_type, origin_call, origin_stand, 
                                 ts_start, ts_end, daytype, missing_data, n_points, polyline, 
                                 domain_middle_lat, domain_middle_lon, domain_radius, start_epoch)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE trip_id = trip_id
                """, self.trips_batch)
                
                self.db_connection.commit()
                
                # Clear batch
                self.trips_batch = []
                return  # Success
                
            except mysql.connector.errors.DatabaseError as e:
                # Handle constraint violations and database errors
                if "chk_end_after_start" in str(e):
                    print(f"\n⚠ Constraint violation detected: {e}")
                    print("Finding problematic trip(s) in batch...")
                    
                    # Try to identify and skip the problematic trip
                    for trip in self.trips_batch:
                        trip_id, taxi_id, call_type, origin_call, origin_stand, ts_start, ts_end, *rest = trip
                        if ts_end < ts_start:
                            print(f"  Problem trip: {trip_id} - ts_start={ts_start}, ts_end={ts_end}")
                    
                    # Skip this batch and continue
                    self.stats['trips_skipped'] += len(self.trips_batch)
                    self.trips_batch = []
                    print("Skipped problematic batch. Continuing...")
                    return
                else:
                    print(f"Database error: {e}")
                    try:
                        self.db_connection.rollback()
                    except:
                        print("Could not rollback")
                    raise
                    
            except mysql.connector.errors.OperationalError as e:
                print(f"\n⚠ Connection error on attempt {attempt + 1}/{max_retries}: {e}")
                
                if attempt < max_retries - 1:
                    # Try to reconnect
                    if self.connection.reconnect():
                        print("Retrying batch insert...")
                        continue
                    else:
                        print("Failed to reconnect. Aborting.")
                        raise
                else:
                    print("Max retries reached. Aborting.")
                    raise
                    
            except Exception as e:
                print(f"Error inserting batch: {e}")
                try:
                    self.db_connection.rollback()
                except:
                    print("Could not rollback - connection may be lost")
                raise

    def _flush_remaining_batches(self):
        """
        Process any remaining data in batches
        """
        if self.trips_batch:
            print("Flushing remaining trips...")
            self._insert_trips_batch()

    def _log_progress(self, batch_count, last_progress_time):
        """
        Log progress with comprehensive statistics
        """
        current_time = time.time()
        elapsed = current_time - self.stats['start_time']
        rows_per_sec = self.stats['rows_processed'] / elapsed if elapsed > 0 else 0
        trips_per_sec = self.stats['trips_imported'] / elapsed if elapsed > 0 else 0
        avg_radius = self.stats['total_radius'] / max(self.stats['trips_imported'], 1)
        
        print(f"Row {self.stats['rows_processed']:,} | "
              f"Trips: {self.stats['trips_imported']:,} | "
              f"Skipped: {self.stats['trips_skipped']:,} | "
              f"Points: {self.stats['total_points']:,} | "
              f"Avg Radius: {avg_radius:.0f}m | "
              f"Rate: {rows_per_sec:.1f} rows/s, {trips_per_sec:.1f} trips/s | "
              f"Batches: {batch_count}")

    def _print_final_statistics(self, start_time, end_time):
        """
        Print comprehensive final statistics
        """
        elapsed = end_time - start_time
        avg_radius = self.stats['total_radius'] / max(self.stats['trips_imported'], 1)
        avg_points = self.stats['total_points'] / max(self.stats['trips_imported'], 1)
        avg_duration = avg_points * self.TIME_PER_POINT / 60  # in minutes
        
        print("\n" + "="*80)
        print("CIRCULAR DOMAIN IMPORT COMPLETED")
        print("="*80)
        print(f"Total time: {elapsed:.2f} seconds")
        print(f"Rows processed: {self.stats['rows_processed']:,}")
        print(f"Trips imported: {self.stats['trips_imported']:,}")
        print(f"Trips skipped: {self.stats['trips_skipped']:,}")
        print(f"Total GPS points: {self.stats['total_points']:,}")
        print(f"Average points per trip: {avg_points:.1f}")
        print(f"Average trip duration: {avg_duration:.1f} minutes")
        print(f"Average domain radius: {avg_radius:.0f} meters ({avg_radius/1000:.2f} km)")
        print(f"Processing rate: {self.stats['rows_processed'] / elapsed:.1f} rows/second")
        print(f"Import rate: {self.stats['trips_imported'] / elapsed:.1f} trips/second")
        print("="*80)

    def close_connection(self):
        """
        Close the database connection
        """
        self.connection.close_connection()


def main():
    importer = None
    
    try:
        print("="*60)
        print("CIRCULAR DOMAIN PORTO TAXI DATA IMPORTER")
        print("="*60)
        print("Features:")
        print("- JSON polyline storage")
        print("- Circular domain (middle point + radius)")
        print("- Domain based on: time * max_speed (120 km/h)")
        print("- Calculated end time (ts_start + n_points * 15s)")
        print("- Minimal storage footprint")
        print("- Auto-reconnect on connection loss")
        print("="*60)
        
        # Configuration
        batch_size = 5000
        resume_from_row = 551000  # <-- UPDATE THIS to resume from where it crashed
        
        print(f"\n⚠ RESUMING FROM ROW {resume_from_row}")
        print("Change resume_from_row in main() to adjust starting point\n")
        
        importer = CircularDomainPortoTaxiDataImporter(
            batch_size=batch_size,
            resume_from_row=resume_from_row
        )
        
        csv_path = 'C:\\Users\\Valde\\School2025Autumn\\TDT4225\\TDT4225-Assignments\\Assignment 2\\porto.csv'
        importer.import_data(csv_path)
        
    except Exception as e:
        print(f"ERROR: Failed to import data: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if importer:
            importer.close_connection()


if __name__ == '__main__':
    main()
