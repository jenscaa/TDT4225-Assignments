import csv
import json
import sys
import os
from collections import defaultdict, Counter
import pandas as pd
from datetime import datetime

class DuplicateTripAnalyzer:
    """
    EDA script to analyze duplicate trip IDs in Porto taxi data
    """
    
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.duplicates = defaultdict(list)
        self.trip_stats = {}
        self.same_call_type_duplicates = defaultdict(list)  # NEW: track same call_type duplicates
        
    def analyze_duplicates(self):
        """
        Analyze duplicate trip IDs and their characteristics
        """
        print("Starting duplicate trip ID analysis...")
        print("="*60)
        
        row_count = 0
        duplicate_count = 0
        trip_id_counts = Counter()
        trip_call_type_counts = Counter()  # NEW: track (trip_id, call_type) combinations
        
        # First pass: count occurrences of each trip_id
        print("Pass 1: Counting trip ID occurrences...")
        with open(self.csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                row_count += 1
                trip_id = row['TRIP_ID']
                call_type = row['CALL_TYPE']
                
                trip_id_counts[trip_id] += 1
                trip_call_type_counts[(trip_id, call_type)] += 1  # NEW: track combinations
                
                if row_count % 50000 == 0:
                    print(f"Processed {row_count:,} rows...")
        
        # Find duplicates
        duplicate_trip_ids = {tid: count for tid, count in trip_id_counts.items() if count > 1}
        duplicate_trip_call_types = {(tid, ct): count for (tid, ct), count in trip_call_type_counts.items() if count > 1}
        duplicate_count = sum(count for count in duplicate_trip_ids.values())
        
        print(f"\nTotal rows processed: {row_count:,}")
        print(f"Unique trip IDs: {len(trip_id_counts):,}")
        print(f"Duplicate trip IDs: {len(duplicate_trip_ids):,}")
        print(f"Total duplicate entries: {duplicate_count:,}")
        print(f"NEW: Duplicate (trip_id, call_type) combinations: {len(duplicate_trip_call_types)}")
        
        # Second pass: collect detailed information about duplicates
        print("\nPass 2: Analyzing duplicate trip details...")
        self._analyze_duplicate_details(duplicate_trip_ids, duplicate_trip_call_types)
        
        return duplicate_trip_ids, duplicate_trip_call_types
        
    def _analyze_duplicate_details(self, duplicate_trip_ids, duplicate_trip_call_types):
        """
        Analyze the details of duplicate trip IDs
        """
        row_count = 0
        
        with open(self.csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                row_count += 1
                trip_id = row['TRIP_ID']
                call_type = row['CALL_TYPE']
                
                # Process regular duplicates
                if trip_id in duplicate_trip_ids:
                    self.duplicates[trip_id].append(row)
                
                # Process same call_type duplicates
                if (trip_id, call_type) in duplicate_trip_call_types:
                    self.same_call_type_duplicates[(trip_id, call_type)].append(row)
                
                if row_count % 50000 == 0:
                    print(f"Processed {row_count:,} rows...")
        
        # Analyze each duplicate group
        print(f"\nAnalyzing {len(self.duplicates)} duplicate trip ID groups...")
        print(f"Analyzing {len(self.same_call_type_duplicates)} same call_type duplicate groups...")
        self._print_duplicate_analysis()
        
    def _print_duplicate_analysis(self):
        """
        Print detailed analysis of duplicates
        """
        print("\n" + "="*80)
        print("DUPLICATE TRIP ID ANALYSIS")
        print("="*80)
        
        # Statistics
        total_duplicates = sum(len(entries) for entries in self.duplicates.values())
        total_same_call_type = sum(len(entries) for entries in self.same_call_type_duplicates.values())
        
        print(f"Total duplicate entries: {total_duplicates}")
        print(f"Number of duplicate trip ID groups: {len(self.duplicates)}")
        print(f"NEW: Total same call_type duplicate entries: {total_same_call_type}")
        print(f"NEW: Number of same call_type duplicate groups: {len(self.same_call_type_duplicates)}")
        
        # Show examples of different types of duplicates (limited to 2)
        print("\nExamples of duplicate trip IDs (showing first 2):")
        print("-" * 50)
        
        example_count = 0
        for trip_id, entries in self.duplicates.items():
            if example_count >= 2:  # Show only first 2 examples
                break
                
            print(f"\nTrip ID: {trip_id}")
            print(f"Number of entries: {len(entries)}")
            
            # Compare entries
            for i, entry in enumerate(entries):
                print(f"  Entry {i+1}:")
                print(f"    TAXI_ID: {entry['TAXI_ID']}")
                print(f"    CALL_TYPE: {entry['CALL_TYPE']}")
                print(f"    ORIGIN_CALL: {entry['ORIGIN_CALL']}")
                print(f"    ORIGIN_STAND: {entry['ORIGIN_STAND']}")
                print(f"    TIMESTAMP: {entry['TIMESTAMP']}")
                print(f"    DAY_TYPE: {entry['DAY_TYPE']}")
                print(f"    MISSING_DATA: {entry['MISSING_DATA']}")
                
                # Show polyline info - only point count
                try:
                    polyline = json.loads(entry['POLYLINE'])
                    print(f"    POLYLINE points: {len(polyline)}")
                    if len(polyline) > 0:
                        print(f"    Start point: {polyline[0]}")
                        print(f"    End point: {polyline[-1]}")
                except:
                    print(f"    POLYLINE: Invalid JSON")
                
            example_count += 1
        
        # NEW: Show examples of same call_type duplicates
        print("\n" + "="*50)
        print("SAME CALL_TYPE DUPLICATES (PROBLEMATIC FOR COMPOSITE KEY)")
        print("="*50)
        
        example_count = 0
        for (trip_id, call_type), entries in self.same_call_type_duplicates.items():
            if example_count >= 3:  # Show first 3 examples
                break
                
            print(f"\nTrip ID: {trip_id}, Call Type: {call_type}")
            print(f"Number of entries: {len(entries)}")
            
            # Compare entries
            for i, entry in enumerate(entries):
                print(f"  Entry {i+1}:")
                print(f"    TAXI_ID: {entry['TAXI_ID']}")
                print(f"    ORIGIN_CALL: {entry['ORIGIN_CALL']}")
                print(f"    ORIGIN_STAND: {entry['ORIGIN_STAND']}")
                print(f"    TIMESTAMP: {entry['TIMESTAMP']}")
                print(f"    DAY_TYPE: {entry['DAY_TYPE']}")
                print(f"    MISSING_DATA: {entry['MISSING_DATA']}")
                
                # Show polyline info
                try:
                    polyline = json.loads(entry['POLYLINE'])
                    print(f"    POLYLINE points: {len(polyline)}")
                    if len(polyline) > 0:
                        print(f"    Start point: {polyline[0]}")
                        print(f"    End point: {polyline[-1]}")
                except:
                    print(f"    POLYLINE: Invalid JSON")
                
            example_count += 1
        
        # Analyze patterns
        self._analyze_duplicate_patterns()
        
    def _analyze_duplicate_patterns(self):
        """
        Analyze patterns in duplicates
        """
        print("\n" + "="*50)
        print("DUPLICATE PATTERN ANALYSIS")
        print("="*50)
        
        # Group by number of duplicates
        duplicate_counts = Counter()
        for entries in self.duplicates.values():
            duplicate_counts[len(entries)] += 1
        
        print("Distribution of duplicate counts:")
        for count, frequency in sorted(duplicate_counts.items()):
            print(f"  {count} duplicates: {frequency} trip IDs")
        
        # Analyze call type patterns
        call_type_patterns = defaultdict(int)
        taxi_id_patterns = defaultdict(int)
        
        for trip_id, entries in self.duplicates.items():
            # Check if all entries have same taxi_id
            taxi_ids = set(entry['TAXI_ID'] for entry in entries)
            if len(taxi_ids) == 1:
                taxi_id_patterns['same_taxi'] += 1
            else:
                taxi_id_patterns['different_taxis'] += 1
            
            # Check call type patterns
            call_types = set(entry['CALL_TYPE'] for entry in entries)
            call_types_str = ','.join(sorted(call_types))
            call_type_patterns[call_types_str] += 1
        
        print(f"\nTaxi ID patterns:")
        for pattern, count in taxi_id_patterns.items():
            print(f"  {pattern}: {count}")
        
        print(f"\nCall type patterns:")
        for pattern, count in call_type_patterns.items():
            print(f"  {pattern}: {count}")
        
        # NEW: Analyze same call_type duplicates
        self._analyze_same_call_type_patterns()
        
        # Analyze polyline points patterns
        self._analyze_polyline_patterns()
        
        # Analyze specific problematic trip ID from error
        self._analyze_problematic_trip()
        
    def _analyze_same_call_type_patterns(self):
        """
        NEW: Analyze patterns in same call_type duplicates
        """
        print("\n" + "="*50)
        print("SAME CALL_TYPE DUPLICATE PATTERNS")
        print("="*50)
        
        if not self.same_call_type_duplicates:
            print("No same call_type duplicates found!")
            return
        
        # Group by number of same call_type duplicates
        same_call_type_counts = Counter()
        for entries in self.same_call_type_duplicates.values():
            same_call_type_counts[len(entries)] += 1
        
        print("Distribution of same call_type duplicate counts:")
        for count, frequency in sorted(same_call_type_counts.items()):
            print(f"  {count} duplicates: {frequency} (trip_id, call_type) combinations")
        
        # Analyze taxi_id patterns in same call_type duplicates
        same_taxi_same_call_type = 0
        different_taxi_same_call_type = 0
        
        for (trip_id, call_type), entries in self.same_call_type_duplicates.items():
            taxi_ids = set(entry['TAXI_ID'] for entry in entries)
            if len(taxi_ids) == 1:
                same_taxi_same_call_type += 1
            else:
                different_taxi_same_call_type += 1
        
        print(f"\nTaxi ID patterns in same call_type duplicates:")
        print(f"  Same taxi: {same_taxi_same_call_type}")
        print(f"  Different taxis: {different_taxi_same_call_type}")
        
        # Show examples of problematic cases
        print(f"\nExamples of problematic same call_type duplicates:")
        example_count = 0
        for (trip_id, call_type), entries in self.same_call_type_duplicates.items():
            if example_count >= 3:
                break
            
            taxi_ids = set(entry['TAXI_ID'] for entry in entries)
            print(f"\n  Trip ID: {trip_id}, Call Type: {call_type}")
            print(f"  Number of entries: {len(entries)}")
            print(f"  Taxi IDs: {sorted(taxi_ids)}")
            
            for i, entry in enumerate(entries):
                try:
                    polyline = json.loads(entry['POLYLINE'])
                    points = len(polyline)
                except:
                    points = 0
                print(f"    Entry {i+1}: TAXI_ID={entry['TAXI_ID']}, Points={points}, TIMESTAMP={entry['TIMESTAMP']}")
            
            example_count += 1
        
    def _analyze_polyline_patterns(self):
        """
        Analyze polyline point patterns in duplicate pairs
        """
        print("\n" + "="*50)
        print("POLYLINE POINTS PATTERN ANALYSIS")
        print("="*50)
        
        # Count patterns for different duplicate group sizes
        polyline_patterns = defaultdict(int)
        has_short_trip = 0  # Count of groups with at least one trip ≤3 points
        total_groups = len(self.duplicates)
        
        for trip_id, entries in self.duplicates.items():
            if len(entries) == 2:  # Focus on pairs first
                try:
                    # Get polyline point counts
                    points1 = len(json.loads(entries[0]['POLYLINE']))
                    points2 = len(json.loads(entries[1]['POLYLINE']))
                    
                    # Check if either trip has ≤3 points
                    if points1 <= 3 or points2 <= 3:
                        has_short_trip += 1
                    
                    # Create pattern description
                    min_points = min(points1, points2)
                    max_points = max(points1, points2)
                    ratio = max_points / min_points if min_points > 0 else float('inf')
                    
                    if min_points <= 3:
                        pattern = f"One trip ≤3 points ({min_points}), other has {max_points} (ratio: {ratio:.1f}x)"
                    else:
                        pattern = f"Both trips >3 points ({points1}, {points2}, ratio: {ratio:.1f}x)"
                    
                    polyline_patterns[pattern] += 1
                    
                except Exception as e:
                    print(f"  Error analyzing polyline for trip {trip_id}: {e}")
        
        print(f"Analysis of {len([g for g in self.duplicates.values() if len(g) == 2])} duplicate pairs:")
        print(f"\nGroups with at least one trip ≤3 points: {has_short_trip}")
        print(f"Groups with both trips >3 points: {total_groups - has_short_trip}")
        print(f"Percentage with short trip (≤3 points): {has_short_trip/total_groups*100:.1f}%")
        
        print(f"\nDetailed patterns:")
        for pattern, count in sorted(polyline_patterns.items(), key=lambda x: x[1], reverse=True):
            print(f"  {pattern}: {count}")
        
        # Show examples of different patterns
        print(f"\nExamples by pattern:")
        self._show_polyline_examples()
        
    def _show_polyline_examples(self):
        """
        Show examples of different polyline patterns
        """
        examples_shown = defaultdict(int)
        max_examples_per_pattern = 2
        
        for trip_id, entries in self.duplicates.items():
            if len(entries) == 2:  # Focus on pairs
                try:
                    points1 = len(json.loads(entries[0]['POLYLINE']))
                    points2 = len(json.loads(entries[1]['POLYLINE']))
                    
                    # Determine pattern
                    if points1 <= 3 or points2 <= 3:
                        pattern_key = "short_trip"
                    else:
                        pattern_key = "both_long"
                    
                    if examples_shown[pattern_key] < max_examples_per_pattern:
                        print(f"\n  {pattern_key.upper()} pattern example:")
                        print(f"    Trip ID: {trip_id}")
                        print(f"    Entry 1: TAXI_ID={entries[0]['TAXI_ID']}, CALL_TYPE={entries[0]['CALL_TYPE']}, Points={points1}")
                        print(f"    Entry 2: TAXI_ID={entries[1]['TAXI_ID']}, CALL_TYPE={entries[1]['CALL_TYPE']}, Points={points2}")
                        
                        # Show start/end points for comparison
                        poly1 = json.loads(entries[0]['POLYLINE'])
                        poly2 = json.loads(entries[1]['POLYLINE'])
                        print(f"    Entry 1 start/end: {poly1[0] if poly1 else 'None'} / {poly1[-1] if poly1 else 'None'}")
                        print(f"    Entry 2 start/end: {poly2[0] if poly2 else 'None'} / {poly2[-1] if poly2 else 'None'}")
                        
                        examples_shown[pattern_key] += 1
                        
                except Exception as e:
                    continue
        
        # Analyze the relationship between call types and polyline lengths
        self._analyze_call_type_vs_polyline_length()
        
    def _analyze_call_type_vs_polyline_length(self):
        """
        Analyze relationship between call types and polyline lengths
        """
        print(f"\n" + "="*40)
        print("CALL TYPE vs POLYLINE LENGTH ANALYSIS")
        print("="*40)
        
        call_type_lengths = defaultdict(list)
        
        for trip_id, entries in self.duplicates.items():
            if len(entries) == 2:  # Focus on pairs
                for entry in entries:
                    try:
                        call_type = entry['CALL_TYPE']
                        polyline = json.loads(entry['POLYLINE'])
                        points = len(polyline)
                        call_type_lengths[call_type].append(points)
                    except:
                        continue
        
        print("Average polyline length by call type in duplicate pairs:")
        for call_type, lengths in call_type_lengths.items():
            if lengths:
                avg_length = sum(lengths) / len(lengths)
                min_length = min(lengths)
                max_length = max(lengths)
                print(f"  CALL_TYPE {call_type}: avg={avg_length:.1f}, min={min_length}, max={max_length}, count={len(lengths)}")
        
        # Check if certain call types tend to have shorter polylines
        print(f"\nCall type with shortest polylines:")
        shortest_avg = float('inf')
        shortest_call_type = None
        for call_type, lengths in call_type_lengths.items():
            if lengths:
                avg_length = sum(lengths) / len(lengths)
                if avg_length < shortest_avg:
                    shortest_avg = avg_length
                    shortest_call_type = call_type
        
        if shortest_call_type:
            print(f"  {shortest_call_type} has shortest average length: {shortest_avg:.1f} points")
        
    def _analyze_problematic_trip(self):
        """
        Analyze the specific trip ID that caused the error
        """
        print("\n" + "="*50)
        print("ANALYSIS OF PROBLEMATIC TRIP ID: 1374014097620000337")
        print("="*50)
        
        problematic_id = "1374014097620000337"
        problematic_call_type = "C"
        
        # Check in regular duplicates
        if problematic_id in self.duplicates:
            entries = self.duplicates[problematic_id]
            print(f"Found {len(entries)} entries for trip ID {problematic_id}")
            
            for i, entry in enumerate(entries):
                print(f"\nEntry {i+1}:")
                print(f"  TAXI_ID: {entry['TAXI_ID']}")
                print(f"  CALL_TYPE: {entry['CALL_TYPE']}")
                print(f"  ORIGIN_CALL: {entry['ORIGIN_CALL']}")
                print(f"  ORIGIN_STAND: {entry['ORIGIN_STAND']}")
                print(f"  TIMESTAMP: {entry['TIMESTAMP']}")
                print(f"  DAY_TYPE: {entry['DAY_TYPE']}")
                print(f"  MISSING_DATA: {entry['MISSING_DATA']}")
                
                try:
                    polyline = json.loads(entry['POLYLINE'])
                    print(f"  POLYLINE points: {len(polyline)}")
                    if len(polyline) > 0:
                        print(f"  Start: {polyline[0]}")
                        print(f"  End: {polyline[-1]}")
                except Exception as e:
                    print(f"  POLYLINE error: {e}")
        else:
            print(f"Trip ID {problematic_id} not found in regular duplicates")
        
        # Check in same call_type duplicates
        if (problematic_id, problematic_call_type) in self.same_call_type_duplicates:
            entries = self.same_call_type_duplicates[(problematic_id, problematic_call_type)]
            print(f"\nNEW: Found {len(entries)} entries for (trip_id={problematic_id}, call_type={problematic_call_type})")
            
            for i, entry in enumerate(entries):
                print(f"\nSame Call Type Entry {i+1}:")
                print(f"  TAXI_ID: {entry['TAXI_ID']}")
                print(f"  ORIGIN_CALL: {entry['ORIGIN_CALL']}")
                print(f"  ORIGIN_STAND: {entry['ORIGIN_STAND']}")
                print(f"  TIMESTAMP: {entry['TIMESTAMP']}")
                print(f"  DAY_TYPE: {entry['DAY_TYPE']}")
                print(f"  MISSING_DATA: {entry['MISSING_DATA']}")
                
                try:
                    polyline = json.loads(entry['POLYLINE'])
                    print(f"  POLYLINE points: {len(polyline)}")
                    if len(polyline) > 0:
                        print(f"  Start: {polyline[0]}")
                        print(f"  End: {polyline[-1]}")
                except Exception as e:
                    print(f"  POLYLINE error: {e}")
        else:
            print(f"Combination ({problematic_id}, {problematic_call_type}) not found in same call_type duplicates")
    
    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two points in meters"""
        import math
        R = 6371000  # Earth radius in meters
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        a = math.sin(delta_phi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R * c
    
    def generate_recommendations(self):
        """
        Generate recommendations for handling duplicates
        """
        print("\n" + "="*60)
        print("RECOMMENDATIONS FOR HANDLING DUPLICATES")
        print("="*60)
        
        print("Based on the analysis, here are the recommended approaches:")
        print("\n1. MODIFY PRIMARY KEY:")
        print("   - Current: (trip_id, call_type) - FAILS due to same call_type duplicates")
        print("   - NEW OPTION A: (trip_id, call_type, taxi_id)")
        print("   - NEW OPTION B: (trip_id, call_type, timestamp)")
        print("   - NEW OPTION C: Add auto-increment ID as primary key")
        
        print("\n2. DEDUPLICATION STRATEGY:")
        print("   - Keep the entry with the most GPS points")
        print("   - Or keep the entry with call_type 'A' (if available)")
        print("   - Or keep the entry with the longest polyline")
        print("   - NEW: Handle same call_type duplicates by keeping best entry")
        
        print("\n3. DATA VALIDATION:")
        print("   - Add validation to check for meaningful differences")
        print("   - Log duplicates for manual review")
        print("   - Consider if duplicates represent different phases of the same trip")
        
        print("\n4. DATABASE SCHEMA CHANGES:")
        print("   - Option A: Composite primary key (trip_id, call_type, taxi_id)")
        print("   - Option B: Add a unique constraint on (trip_id, call_type, timestamp)")
        print("   - Option C: Create a new unique trip_id by appending call_type")
        print("   - Option D: Use auto-increment ID + unique constraint")
        
        print("\n5. IMPORT MODIFICATION:")
        print("   - Modify the importer to handle duplicates gracefully")
        print("   - Add duplicate detection before insertion")
        print("   - Implement conflict resolution logic")
        print("   - NEW: Handle same call_type duplicates with deduplication")
        
        print("\n6. POLYLINE ANALYSIS INSIGHTS:")
        print("   - Check if one entry is always a simplified version (≤3 points)")
        print("   - Consider keeping the more detailed version for analysis")
        print("   - Use the short version for quick lookups if needed")

def main():
    """
    Main function to run the duplicate analysis
    """
    print("="*60)
    print("PORTO TAXI DATA - DUPLICATE TRIP ID ANALYSIS")
    print("="*60)
    
    csv_path = 'porto.csv'
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file '{csv_path}' not found!")
        print("Please make sure the file exists in the current directory.")
        return
    
    analyzer = DuplicateTripAnalyzer(csv_path)
    
    try:
        # Run the analysis
        duplicate_trip_ids, duplicate_trip_call_types = analyzer.analyze_duplicates()
        
        # Generate recommendations
        analyzer.generate_recommendations()
        
        print("\n" + "="*60)
        print("ANALYSIS COMPLETED")
        print("="*60)
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
