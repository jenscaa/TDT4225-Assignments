#!/usr/bin/env python3
"""
Standalone script to run Task 8 using the sliding window algorithm
"""

import sys
import os
import time

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from service.taxi_proximity_sliding_window import TaxiProximitySlidingWindowService


def main():
    print("="*70)
    print("TASK 8: TAXI PROXIMITY DETECTION")
    print("Using: Sliding Window + Spatial Grid Algorithm")
    print("="*70)
    
    service = TaxiProximitySlidingWindowService()
    
    # Full year: July 2013 - June 2014
    start_date = '2013-07-01 00:00:00'
    end_date = '2014-07-01 00:00:00'
    
    # For testing on a smaller dataset, uncomment this:
    # start_date = '2013-07-01 00:00:00'
    # end_date = '2013-07-08 00:00:00'  # Just 1 week
    
    overall_start = time.time()
    
    try:
        results = service.find_proximity_pairs(
            start_date=start_date,
            end_date=end_date,
            time_window=5,         # 5 seconds
            distance=5.0,          # 5 meters
            grid_cell=12.0,        # 12 meter grid cells
            chunk_hours=24,        # Process 1 day at a time
            limit_rows=0,          # No limit (0 = process all)
            output_csv='task8_proximity_results.csv'
        )
        
        overall_time = time.time() - overall_start
        
        print("\n" + "="*70)
        print("TASK 8 COMPLETE")
        print("="*70)
        print(f"Total execution time: {overall_time/60:.1f} minutes ({overall_time/3600:.2f} hours)")
        print(f"Total unique taxi pairs found: {len(results)}")
        print(f"Results saved to: task8_proximity_results.csv")
        print("="*70)
        
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user. Partial results may be available.")
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
