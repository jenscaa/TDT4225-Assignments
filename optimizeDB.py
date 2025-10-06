def create_optimized_indexes_for_proximity(self):
    """
    Add optimized indexes for the proximity query.
    These are critical for performance!
    """
    print("Creating optimized indexes for proximity queries...")
    
    # 1. Composite index for the join condition (MOST IMPORTANT)
    self.cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_gps_taxi_time_spatial 
        ON gps_points(taxi_id, point_timestamp, latitude, longitude)
    """)
    print("✓ Created composite index: taxi_id, point_timestamp, latitude, longitude")
    
    # 2. Additional index for timestamp range queries
    self.cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_gps_timestamp_taxi 
        ON gps_points(point_timestamp, taxi_id)
    """)
    print("✓ Created index: point_timestamp, taxi_id")
    
    # 3. Spatial index if you want to use MySQL spatial types (optional but helpful)
    # First, check if we can add a computed spatial column
    try:
        self.cursor.execute("""
            ALTER TABLE gps_points 
            ADD COLUMN IF NOT EXISTS point_geom POINT AS 
                (POINT(longitude, latitude)) STORED
        """)
        
        self.cursor.execute("""
            CREATE SPATIAL INDEX IF NOT EXISTS idx_gps_spatial 
            ON gps_points(point_geom)
        """)
        print("✓ Created spatial index on point_geom")
    except Exception as e:
        print(f"  Note: Spatial index not created: {e}")
    
    self.db_connection.commit()
    print("\nAll indexes created successfully!")


def optimize_mysql_config(self):
    """
    Optimize MySQL configuration for this query.
    Add these to your MySQL config file (my.cnf or my.ini):
    """
    config_recommendations = """
    # Add to your MySQL configuration file for better performance:
    
    [mysqld]
    # Increase join buffer for large joins
    join_buffer_size = 256M
    
    # Increase sort buffer
    sort_buffer_size = 64M
    
    # Increase read buffer
    read_rnd_buffer_size = 64M
    
    # Increase tmp table size for in-memory operations
    tmp_table_size = 512M
    max_heap_table_size = 512M
    
    # InnoDB buffer pool (set to 70-80% of RAM if dedicated server)
    innodb_buffer_pool_size = 4G
    
    # Enable parallel query execution (MySQL 8.0+)
    innodb_parallel_read_threads = 4
    """
    
    print(config_recommendations)
    
    # You can also set some session variables programmatically:
    try:
        self.cursor.execute("SET SESSION join_buffer_size = 256 * 1024 * 1024")
        self.cursor.execute("SET SESSION sort_buffer_size = 64 * 1024 * 1024")
        self.cursor.execute("SET SESSION tmp_table_size = 512 * 1024 * 1024")
        self.cursor.execute("SET SESSION max_heap_table_size = 512 * 1024 * 1024")
        print("✓ Session variables optimized")
    except Exception as e:
        print(f"Could not set session variables: {e}")


def analyze_query_performance(self):
    """
    Run EXPLAIN to see query execution plan
    """
    explain_query = """
    EXPLAIN 
    WITH spatial_filtered AS (
        SELECT 
            t1.taxi_id AS taxi1,
            t2.taxi_id AS taxi2,
            t1.point_timestamp AS ts1,
            t2.point_timestamp AS ts2,
            t1.latitude AS lat1,
            t1.longitude AS lon1,
            t2.latitude AS lat2,
            t2.longitude AS lon2
        FROM gps_points t1
        INNER JOIN gps_points t2 
            ON t2.taxi_id > t1.taxi_id
            AND t2.point_timestamp BETWEEN t1.point_timestamp - 5 AND t1.point_timestamp + 5
            AND t2.latitude BETWEEN t1.latitude - 0.00005 AND t1.latitude + 0.00005
            AND t2.longitude BETWEEN t1.longitude - 0.00007 AND t1.longitude + 0.00007
        WHERE t1.point_timestamp BETWEEN 1401580800 AND 1401667199
    )
    SELECT taxi1, taxi2, COUNT(*) FROM spatial_filtered GROUP BY taxi1, taxi2
    """
    
    self.cursor.execute(explain_query)
    results = self.cursor.fetchall()
    
    print("\nQuery Execution Plan:")
    print("-" * 80)
    for row in results:
        print(row)
    print("-" * 80)


# PERFORMANCE ANALYSIS HELPER
def estimate_row_processing(self):
    """
    Estimate how many rows the query will process
    """
    # Count total GPS points in date range
    self.cursor.execute("""
        SELECT COUNT(*), COUNT(DISTINCT taxi_id)
        FROM gps_points
        WHERE point_timestamp BETWEEN 1401580800 AND 1401667199
    """)
    total_points, unique_taxis = self.cursor.fetchone()
    
    print(f"\nData Statistics:")
    print(f"  Total GPS points in range: {total_points:,}")
    print(f"  Unique taxis: {unique_taxis}")
    print(f"  Avg points per taxi: {total_points/unique_taxis if unique_taxis > 0 else 0:,.0f}")
    
    # Estimate join size (rough upper bound)
    # Each point can potentially join with points from other taxis within ±5 seconds
    avg_concurrent_taxis = 20  # Estimate
    estimated_join_rows = total_points * avg_concurrent_taxis
    
    print(f"\nEstimated query complexity:")
    print(f"  Potential join combinations: ~{estimated_join_rows:,} rows")
    print(f"  After spatial filter: ~{estimated_join_rows * 0.01:,.0f} rows (1% pass rate)")
    print(f"  After distance filter: ~{estimated_join_rows * 0.001:,.0f} rows (0.1% pass rate)")


if __name__ == "__main__":
    optimize_mysql_config()
    create_optimized_indexes_for_proximity()
    analyze_query_performance()
    estimate_row_processing()
    

