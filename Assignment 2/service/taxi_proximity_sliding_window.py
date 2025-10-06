#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
taxi_proximity_sliding_window.py

Fast taxi proximity detection using sliding window + spatial grid bucketing.
DB-light, Python-heavy approach for finding taxi pairs within 5m and 5s.

Approach:
1) Stream gps_points ordered by timestamp
2) Maintain sliding window of last 5 seconds of points
3) Use spatial grid bucketing (~10-12m cells) to prune 99%+ of comparisons
4) Only compute exact Haversine distance for nearby candidates
5) Aggregate metrics per taxi pair
"""

import sys
import os
import math
import csv
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from DbConnector import DbConnector


# ----------------------- Utilities -----------------------

def parse_dt(s: str) -> int:
    """Parse 'YYYY-mm-dd HH:MM:SS' to epoch seconds (UTC assumed)."""
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    return int(dt.timestamp())


def haversine_meters(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance between two WGS84 points in meters."""
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def lat_cell_size_deg(meters: float = 10.0) -> float:
    """Approximate degrees latitude per 'meters'."""
    return meters / 111320.0  # ~111.32 km per degree lat


def lon_cell_size_deg(lat_deg: float, meters: float = 10.0) -> float:
    """Approximate degrees longitude per 'meters' at latitude 'lat_deg'."""
    return meters / (111320.0 * max(0.1, math.cos(math.radians(lat_deg))))


def grid_key(lat: float, lon: float, lat_deg_per_cell: float, lon_deg_per_cell: float):
    """Map (lat, lon) to integer grid cell key."""
    return (int(math.floor(lat / lat_deg_per_cell)), int(math.floor(lon / lon_deg_per_cell)))


# ----------------------- Core Algorithm -----------------------

class ProximityDetector:
    def __init__(
        self,
        time_window_s: int = 5,
        distance_m: float = 5.0,
        grid_cell_m: float = 12.0,
        chunk_hours: int = 24,
        limit_rows: int = 0,
    ):
        self.connection = DbConnector()
        self.cursor = self.connection.cursor
        self.db_connection = self.connection.db_connection
        
        self.time_window_s = time_window_s
        self.distance_m = distance_m
        self.grid_cell_m = grid_cell_m
        self.chunk_hours = chunk_hours
        self.limit_rows = limit_rows  # 0 means no limit

        self.pair_stats = defaultdict(lambda: {
            "count": 0,
            "min_dist": float("inf"),
            "sum_dist": 0.0,
            "min_dt": float("inf"),
            "sum_dt": 0.0
        })

    def _stream_points(self, start_epoch: int, end_epoch: int):
        """
        Stream gps points in [start_epoch, end_epoch) ordered by timestamp, taxi_id.
        Yields tuples: (taxi_id:int, ts:int, lat:float, lon:float)
        
        Uses buffered fetching for mysql.connector (which doesn't have true server-side cursors)
        """
        sql = """
            SELECT taxi_id, point_timestamp, latitude, longitude
            FROM gps_points
            WHERE point_timestamp >= %s AND point_timestamp < %s
            ORDER BY point_timestamp ASC, taxi_id ASC
        """
        
        # Create a new unbuffered cursor for streaming
        cursor = self.db_connection.cursor(buffered=False)
        
        try:
            cursor.execute(sql, (start_epoch, end_epoch))
            row_count = 0
            
            for row in cursor:
                taxi_id, ts, lat, lon = row
                # Convert Decimal to float
                yield int(taxi_id), int(ts), float(lat), float(lon)
                row_count += 1
                if self.limit_rows and row_count >= self.limit_rows:
                    break
        finally:
            cursor.close()

    def _update_pair_stats(self, taxi1: int, taxi2: int, distance_m: float, dt_s: int):
        """Update statistics for a taxi pair"""
        key = (taxi1, taxi2) if taxi1 < taxi2 else (taxi2, taxi1)
        st = self.pair_stats[key]
        st["count"] += 1
        if distance_m < st["min_dist"]:
            st["min_dist"] = distance_m
        st["sum_dist"] += distance_m

        if dt_s < st["min_dt"]:
            st["min_dt"] = dt_s
        st["sum_dt"] += dt_s

    def _process_chunk(self, chunk_start: int, chunk_end: int):
        """
        Process one time chunk using sliding window + spatial grid.
        """
        # sliding window of points: (taxi_id, ts, lat, lon, ci, cj, lon_deg_per_cell)
        window = deque()

        # grid mapping: (ci, cj) -> list of point tuples
        grid = defaultdict(list)

        lat_deg_per_cell = lat_cell_size_deg(self.grid_cell_m)
        
        points_processed = 0
        comparisons_made = 0
        proximity_events = 0

        # Stream rows
        for taxi_id, ts, lat, lon in self._stream_points(chunk_start, chunk_end):
            points_processed += 1
            
            # Drop old points (older than time_window_s)
            min_ts = ts - self.time_window_s
            while window and window[0][1] < min_ts:
                old_point = window.popleft()
                # Don't bother removing from grid; we'll filter by time on read

            # Compute grid cell for current point
            lon_deg_per_cell = lon_cell_size_deg(lat, self.grid_cell_m)
            ci, cj = grid_key(lat, lon, lat_deg_per_cell, lon_deg_per_cell)
            point_tuple = (taxi_id, ts, lat, lon, ci, cj, lon_deg_per_cell)

            # Candidate search: 3x3 neighborhood
            for di in (-1, 0, 1):
                for dj in (-1, 0, 1):
                    cell_points = grid.get((ci + di, cj + dj), [])
                    if not cell_points:
                        continue
                    
                    for (ctaxi, cts, clat, clon, cci, ccj, cloncell) in cell_points:
                        comparisons_made += 1
                        
                        # Filter by time window
                        if cts < ts - self.time_window_s:
                            continue  # stale (outside time window)
                        if ctaxi == taxi_id:
                            continue  # same taxi
                        
                        dt = abs(ts - cts)
                        if dt > self.time_window_s:
                            continue
                        
                        # Exact distance
                        dist = haversine_meters(lat, lon, clat, clon)
                        if dist <= self.distance_m:
                            self._update_pair_stats(taxi_id, ctaxi, dist, dt)
                            proximity_events += 1

            # Insert current point into structures
            window.append(point_tuple)
            grid[(ci, cj)].append(point_tuple)

            # Periodic compaction of grid lists to remove stale refs
            # Keeps lists short and lookup fast
            if ts % 60 == 0:  # roughly every minute boundary
                min_ts_for_keep = ts - self.time_window_s
                for key in list(grid.keys()):
                    lst = grid[key]
                    if not lst:
                        continue
                    # keep only points within window
                    nlst = [p for p in lst if p[1] >= min_ts_for_keep]
                    if nlst:
                        grid[key] = nlst
                    else:
                        del grid[key]
        
        return points_processed, comparisons_made, proximity_events

    def run(self, start_epoch: int, end_epoch: int):
        """
        Run detection over [start_epoch, end_epoch) in hourly/day chunks.
        """
        t0 = time.time()
        chunk = timedelta(hours=self.chunk_hours)
        cur = datetime.fromtimestamp(start_epoch)
        end_dt = datetime.fromtimestamp(end_epoch)

        total_points = 0
        total_comparisons = 0
        total_events = 0
        chunk_idx = 1

        while cur < end_dt:
            next_dt = min(cur + chunk, end_dt)
            cs, ce = int(cur.timestamp()), int(next_dt.timestamp())

            print(f"\n== Chunk {chunk_idx}: {cur}  ->  {next_dt} ==")
            c0 = time.time()
            points, comparisons, events = self._process_chunk(cs, ce)
            c1 = time.time()
            
            total_points += points
            total_comparisons += comparisons
            total_events += events
            
            print(f"   Processed: {points:,} points")
            print(f"   Comparisons: {comparisons:,}")
            print(f"   Proximity events: {events:,}")
            print(f"   ✓ Chunk time: {(c1 - c0):.1f}s")
            print(f"   ✓ Rate: {points/(c1-c0):,.0f} points/sec")

            cur = next_dt
            chunk_idx += 1

        t1 = time.time()
        print(f"\n{'='*70}")
        print(f"All chunks finished in {(t1 - t0)/60:.1f} min.")
        print(f"Total points processed: {total_points:,}")
        print(f"Total comparisons: {total_comparisons:,}")
        print(f"Total proximity events: {total_events:,}")
        print(f"Unique taxi pairs: {len(self.pair_stats):,}")
        print(f"{'='*70}")

    def results(self):
        """
        Return sorted list of (taxi1, taxi2, count, min_dist, avg_dist, min_dt, avg_dt).
        """
        out = []
        for (a, b), st in self.pair_stats.items():
            if st["count"] <= 0:
                continue
            out.append((
                a, b,
                st["count"],
                st["min_dist"],
                st["sum_dist"] / st["count"],
                st["min_dt"],
                st["sum_dt"] / st["count"],
            ))
        out.sort(key=lambda x: (-x[2], x[3], x[5]))  # by count desc, min_dist asc, min_dt asc
        return out

    def write_csv(self, path: str, rows=None):
        """Write results to CSV file"""
        rows = rows or self.results()
        header = ["taxi1", "taxi2", "proximity_count", "min_distance_m",
                  "avg_distance_m", "min_time_diff_s", "avg_time_diff_s"]
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            for r in rows:
                w.writerow(r)
    
    def close_connection(self):
        """Close database connection"""
        self.connection.close_connection()


# ----------------------- Service Integration -----------------------

class TaxiProximitySlidingWindowService:
    """
    Service wrapper for taxi proximity detection using sliding window algorithm
    """
    
    def __init__(self):
        self.detector = None
    
    def find_proximity_pairs(
        self,
        start_date: str = '2013-07-01 00:00:00',
        end_date: str = '2014-07-01 00:00:00',
        time_window: int = 5,
        distance: float = 5.0,
        grid_cell: float = 12.0,
        chunk_hours: int = 24,
        limit_rows: int = 0,
        output_csv: str = 'taxi_proximity_results.csv'
    ):
        """
        Find taxi proximity pairs using sliding window algorithm
        
        Args:
            start_date: Start date 'YYYY-mm-dd HH:MM:SS'
            end_date: End date 'YYYY-mm-dd HH:MM:SS'
            time_window: Time window in seconds (default 5)
            distance: Distance threshold in meters (default 5.0)
            grid_cell: Grid cell size in meters (default 12.0)
            chunk_hours: Process in chunks of this many hours (default 24)
            limit_rows: Limit rows for testing (0 = no limit)
            output_csv: Output CSV file path
        
        Returns:
            List of proximity pair results
        """
        
        start_epoch = parse_dt(start_date)
        end_epoch = parse_dt(end_date)

        print("=" * 70)
        print("TAXI PROXIMITY DETECTION - SLIDING WINDOW ALGORITHM")
        print("=" * 70)
        print(f"Time range: [{start_date} .. {end_date})")
        print(f"Time window: {time_window}s")
        print(f"Distance threshold: {distance}m")
        print(f"Grid cell size: {grid_cell}m")
        print(f"Chunk size: {chunk_hours} hours")
        if limit_rows > 0:
            print(f"Row limit (testing): {limit_rows:,}")
        print("=" * 70)

        self.detector = ProximityDetector(
            time_window_s=time_window,
            distance_m=distance,
            grid_cell_m=grid_cell,
            chunk_hours=chunk_hours,
            limit_rows=limit_rows,
        )
        
        try:
            self.detector.run(start_epoch, end_epoch)
            rows = self.detector.results()

            print("\nTop 20 pairs:")
            print("-" * 90)
            print(f"{'Taxi 1':<10} {'Taxi 2':<10} {'Count':<8} {'MinDist(m)':<12} "
                  f"{'AvgDist(m)':<12} {'MinDT(s)':<10} {'AvgDT(s)':<10}")
            print("-" * 90)
            for r in rows[:20]:
                t1, t2, cnt, mind, avgd, mindt, avgdt = r
                print(f"{t1:<10} {t2:<10} {cnt:<8} {mind:<12.2f} {avgd:<12.2f} "
                      f"{mindt:<10.0f} {avgdt:<10.2f}")

            if output_csv:
                self.detector.write_csv(output_csv, rows=rows)
                print(f"\nSaved results to: {os.path.abspath(output_csv)}")

            print(f"\nTotal unique pairs: {len(rows)}")
            
            return rows
            
        finally:
            if self.detector:
                self.detector.close_connection()


# ----------------------- CLI -----------------------

def main():
    """Command-line interface"""
    import argparse
    
    ap = argparse.ArgumentParser(description="Detect taxi proximity pairs (<=5m & <=5s) fast.")
    ap.add_argument("--start", default='2013-07-01 00:00:00', help="YYYY-mm-dd HH:MM:SS")
    ap.add_argument("--end", default='2014-07-01 00:00:00', help="YYYY-mm-dd HH:MM:SS")
    ap.add_argument("--time-window", type=int, default=5, help="seconds")
    ap.add_argument("--distance", type=float, default=5.0, help="meters")
    ap.add_argument("--grid-cell", type=float, default=12.0, help="grid cell size in meters")
    ap.add_argument("--chunk-hours", type=int, default=24, help="process in time chunks")
    ap.add_argument("--limit-rows", type=int, default=0, help="for testing; 0 = no limit")
    ap.add_argument("--output", default="taxi_proximity_results.csv", help="CSV output path")

    args = ap.parse_args()

    service = TaxiProximitySlidingWindowService()
    service.find_proximity_pairs(
        start_date=args.start,
        end_date=args.end,
        time_window=args.time_window,
        distance=args.distance,
        grid_cell=args.grid_cell,
        chunk_hours=args.chunk_hours,
        limit_rows=args.limit_rows,
        output_csv=args.output
    )
    
    print("\nDone.")


if __name__ == "__main__":
    main()
