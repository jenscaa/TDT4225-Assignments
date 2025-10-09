import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from DbConnector import DbConnector

class CircularDomainDatabaseSetup:
    """
    Sets up the circular domain-based database schema with JSON polyline storage
    and efficient spatial indexing using middle point + radius
    SIMPLIFIED: Uses trip_id as primary key with latest-wins strategy
    """

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def setup_database(self):
        """
        Create new circular domain-based schema with JSON polyline storage
        """
        print("Setting up Circular Domain Porto taxi database schema...")
        print("This will DROP existing tables and create new structure!")

        # Step 1: Create database
        self.cursor.execute("CREATE DATABASE IF NOT EXISTS porto_taxi_test CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
        self.cursor.execute("USE porto_taxi_test")

        # Step 2: Drop old tables
        self.drop_old_tables()

        # Step 3: Create new tables
        self.create_new_tables()

        # Step 4: Create indexes
        self.create_indexes()

        # Step 5: Create constraints
        self.create_constraints()

        print("Circular domain database schema setup completed!")

    def drop_old_tables(self):
        """
        Drop old tables to make way for new circular domain-based structure
        """
        print("Dropping old tables...")
        
        # Tables to drop (in dependency order)
        tables_to_drop = [
            'TripDomainH3',     # Drop first (has foreign keys)
            'Trips',            # Drop second
            'trip_points',      # Drop third
            'gps_points',       # Drop fourth
            'trips',            # Drop fifth (old trips table)
            'known_locations',  # Drop sixth
            'taxis'             # Drop seventh (will recreate)
        ]
        
        for table in tables_to_drop:
            try:
                self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"Dropped table: {table}")
            except Exception as e:
                print(f"Warning: Could not drop table {table}: {e}")
        
        print("Old tables dropped successfully")

    def create_new_tables(self):
        """
        Create new circular domain-based tables
        SIMPLIFIED: Single trip_id primary key with latest-wins strategy
        """
        print("Creating new circular domain-based tables...")

        # Create taxis table (reused from original schema)
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS taxis (
            taxi_id INT PRIMARY KEY
        )
        """)

        # Create new Trips table with JSON polyline and circular domain
        # SIMPLIFIED: Single primary key on trip_id, add import_timestamp for latest-wins
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS Trips (
            trip_id VARCHAR(32) PRIMARY KEY,
            call_type ENUM('A','B','C') NOT NULL,
            taxi_id INT NOT NULL,
            origin_call INT NULL,
            origin_stand INT NULL,
            ts_start DATETIME NOT NULL,
            ts_end DATETIME NOT NULL,
            daytype ENUM('A','B','C') NOT NULL,
            missing_data BOOLEAN NOT NULL,
            n_points INT NOT NULL,
            polyline JSON NOT NULL,
            domain_middle_lat DECIMAL(10, 8) NOT NULL,
            domain_middle_lon DECIMAL(11, 8) NOT NULL,
            domain_radius FLOAT NOT NULL,
            start_epoch INT UNSIGNED NOT NULL,
            trip_distance_m FLOAT NOT NULL DEFAULT 0,
            distance_start_end_m FLOAT NOT NULL DEFAULT 0,
            import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (taxi_id) REFERENCES taxis(taxi_id),
            CHECK (JSON_VALID(polyline))
        )
        """)

        # Create known_locations table (reuse from old schema)
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS known_locations (
            id INT AUTO_INCREMENT PRIMARY KEY,
            location_type ENUM('stand', 'call') NOT NULL,
            location_id INT NOT NULL,
            longitude DECIMAL(10,7) NOT NULL,
            latitude DECIMAL(10,7) NOT NULL,
            UNIQUE KEY unique_location (location_type, location_id)
        )
        """)

        print("New circular domain-based tables created successfully")

        self.create_gps_points_table()

    def create_indexes(self):
        """
        Create indexes for optimal performance
        """
        print("Creating indexes...")

        # Helper function to safely create index
        def create_index_if_not_exists(table, index_name, columns):
            try:
                # Check if index already exists
                self.cursor.execute(f"""
                SELECT COUNT(*) FROM information_schema.statistics 
                WHERE table_schema = 'porto_taxi_test' 
                AND table_name = '{table}' 
                AND index_name = '{index_name}'
                """)
                
                if self.cursor.fetchone()[0] == 0:
                    self.cursor.execute(f"CREATE INDEX {index_name} ON {table}({columns})")
                    print(f"Created index {index_name} on {table}")
                else:
                    print(f"Index {index_name} already exists on {table}")
                    
            except Exception as e:
                print(f"Warning: Could not create index {index_name} on {table}: {e}")

        # Indexes for Trips table
        create_index_if_not_exists("Trips", "idx_trips_taxi", "taxi_id")
        create_index_if_not_exists("Trips", "idx_trips_start_ts", "ts_start")
        create_index_if_not_exists("Trips", "idx_trips_call_type", "call_type")
        create_index_if_not_exists("Trips", "idx_trips_daytype", "daytype")
        create_index_if_not_exists("Trips", "idx_trips_missing_data", "missing_data")
        create_index_if_not_exists("Trips", "idx_trips_import_timestamp", "import_timestamp")  # NEW
        
        # Spatial indexes for circular domain
        create_index_if_not_exists("Trips", "idx_domain_middle_lat", "domain_middle_lat")
        create_index_if_not_exists("Trips", "idx_domain_middle_lon", "domain_middle_lon")
        create_index_if_not_exists("Trips", "idx_domain_radius", "domain_radius")
        
        # Composite index for spatial queries
        create_index_if_not_exists("Trips", "idx_domain_spatial", "domain_middle_lat, domain_middle_lon, domain_radius")

        # Indexes for known_locations
        create_index_if_not_exists("known_locations", "idx_known_type", "location_type")
        create_index_if_not_exists("known_locations", "idx_known_id", "location_id")

        print("Indexes created successfully")

    def create_constraints(self):
        """
        Create additional constraints and checks
        """
        print("Creating constraints...")

        # Helper function to safely add constraint
        def add_constraint_if_not_exists(table, constraint_name, constraint_def):
            try:
                # Check if constraint already exists
                self.cursor.execute(f"""
                SELECT COUNT(*) FROM information_schema.table_constraints 
                WHERE table_schema = 'porto_taxi_test' 
                AND table_name = '{table}' 
                AND constraint_name = '{constraint_name}'
                """)
                
                if self.cursor.fetchone()[0] == 0:
                    self.cursor.execute(f"ALTER TABLE {table} ADD CONSTRAINT {constraint_name} {constraint_def}")
                    print(f"Added constraint {constraint_name} to {table}")
                else:
                    print(f"Constraint {constraint_name} already exists on {table}")
                    
            except Exception as e:
                print(f"Warning: Could not add constraint {constraint_name} to {table}: {e}")

        # Add check constraints for data validation
        add_constraint_if_not_exists("Trips", "chk_n_points_positive", "CHECK (n_points > 0)")
        add_constraint_if_not_exists("Trips", "chk_start_epoch_positive", "CHECK (start_epoch > 0)")
        add_constraint_if_not_exists("Trips", "chk_radius_positive", "CHECK (domain_radius > 0)")
        add_constraint_if_not_exists("Trips", "chk_latitude_valid", "CHECK (domain_middle_lat BETWEEN -90 AND 90)")
        add_constraint_if_not_exists("Trips", "chk_longitude_valid", "CHECK (domain_middle_lon BETWEEN -180 AND 180)")

        print("Constraints created successfully")

    def create_gps_points_table(self):
        """Create table with individual GPS points and timestamps - SIMPLIFIED"""
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS gps_points (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            trip_id VARCHAR(32) NOT NULL,
            taxi_id INT NOT NULL,
            point_index INT NOT NULL,
            latitude DECIMAL(10, 8) NOT NULL,
            longitude DECIMAL(11, 8) NOT NULL,
            point_timestamp INT UNSIGNED NOT NULL,  -- Unix epoch
            FOREIGN KEY (trip_id) REFERENCES Trips(trip_id),
            FOREIGN KEY (taxi_id) REFERENCES taxis(taxi_id),
            INDEX idx_timestamp (point_timestamp),
            INDEX idx_taxi_time (taxi_id, point_timestamp),
            INDEX idx_spatial (latitude, longitude),
            INDEX idx_trip (trip_id)
        )
        """)

    def verify_schema(self):
        """
        Verify the new schema is correctly set up
        """
        print("\nVerifying schema...")
        
        # Check tables exist
        self.cursor.execute("""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'porto_taxi_test' 
        ORDER BY table_name
        """)
        
        tables = [row[0] for row in self.cursor.fetchall()]
        expected_tables = ['Trips', 'known_locations', 'taxis', 'gps_points']
        
        print(f"Found tables: {tables}")
        
        for table in expected_tables:
            if table in tables:
                print(f"✓ {table} table exists")
            else:
                print(f"✗ {table} table missing")
        
        # Check primary key structure
        self.cursor.execute("""
        SELECT column_name, ordinal_position FROM information_schema.key_column_usage 
        WHERE table_schema = 'porto_taxi_test' 
        AND table_name = 'Trips' 
        AND constraint_name = 'PRIMARY'
        ORDER BY ordinal_position
        """)
        
        primary_key_cols = self.cursor.fetchall()
        print(f"\nPrimary key columns in Trips:")
        for col in primary_key_cols:
            print(f"  - {col[0]} (position {col[1]})")
        
        # Check import_timestamp column
        self.cursor.execute("""
        SELECT column_name, data_type, column_type FROM information_schema.columns 
        WHERE table_schema = 'porto_taxi_test' 
        AND table_name = 'Trips' 
        AND column_name = 'import_timestamp'
        """)
        
        timestamp_column = self.cursor.fetchone()
        if timestamp_column:
            print(f"\nImport timestamp column: {timestamp_column[0]} ({timestamp_column[1]})")
        else:
            print(f"\n✗ Import timestamp column missing!")
        
        # Check domain columns
        self.cursor.execute("""
        SELECT column_name, data_type, column_type FROM information_schema.columns 
        WHERE table_schema = 'porto_taxi_test' 
        AND table_name = 'Trips' 
        AND column_name LIKE 'domain%'
        """)
        
        domain_columns = self.cursor.fetchall()
        print(f"\nDomain columns in Trips:")
        for col in domain_columns:
            print(f"  - {col[0]}: {col[1]} ({col[2]})")
        
        # Check JSON column
        self.cursor.execute("""
        SELECT column_name, data_type FROM information_schema.columns 
        WHERE table_schema = 'porto_taxi_test' 
        AND table_name = 'Trips' 
        AND data_type = 'json'
        """)
        
        json_columns = self.cursor.fetchall()
        print(f"\nJSON columns in Trips: {[col[0] for col in json_columns]}")
        
        # Check indexes
        self.cursor.execute("""
        SELECT table_name, index_name FROM information_schema.statistics 
        WHERE table_schema = 'porto_taxi_test' 
        AND table_name = 'Trips'
        AND index_name LIKE 'idx_domain%'
        ORDER BY index_name
        """)
        
        indexes = self.cursor.fetchall()
        print(f"\nDomain indexes created:")
        for idx in indexes:
            print(f"  - {idx[1]}")
        
        print("\nSchema verification completed!")

    def close_connection(self):
        """
        Close the database connection
        """
        self.connection.close_connection()

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
            CREATE INDEX idx_gps_timestamp_taxi 
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


def main():
    setup = None
    try:
        print("="*60)
        print("CIRCULAR DOMAIN PORTO TAXI DATABASE SETUP")
        print("="*60)
        print("This will:")
        print("1. Drop ALL existing tables")
        print("2. Create new circular domain-based schema")
        print("3. Set up JSON polyline storage")
        print("4. Store domain as: middle_lat, middle_lon, radius")
        print("5. Create spatial indexing for fast proximity queries")
        print("6. Minimal storage footprint (no H3 cells)")
        print("7. SIMPLIFIED: Single primary key on trip_id")
        print("8. NEW: Latest-wins strategy with import_timestamp")
        print("="*60)
        
        response = input("Continue? (y/N): ").strip().lower()
        if response != 'y':
            print("Setup cancelled.")
            return
        
        setup = CircularDomainDatabaseSetup()
        # setup.create_optimized_indexes_for_proximity()
        setup.analyze_query_performance()
        setup.estimate_row_processing()
        # setup.setup_database()
        # setup.verify_schema()
        
        print("\n" + "="*60)
        print("SETUP COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("Next steps:")
        print("1. Run the circular domain importer: import_porto_taxi_data_circular.py")
        print("2. Database size will be significantly reduced (from 34GB to ~3-5GB)")
        print("3. Use spatial queries with distance calculations from middle point")
        print("4. Latest version of each trip_id will be kept automatically")
        
    except Exception as e:
        print(f"ERROR: Failed to setup database: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if setup:
            setup.close_connection()


if __name__ == '__main__':
    main()
