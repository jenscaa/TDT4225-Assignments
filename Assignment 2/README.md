# Porto Taxi Database Project

## Overview

This project implements a MySQL database for the Porto taxi dataset according to the specifications in `changelog-master.xml`. The dataset contains GPS trajectory data from taxi trips in Porto, Portugal.

## Data Structure

### CSV Format
The source data is in CSV format with the following columns:
- `TRIP_ID`: Unique trip identifier
- `CALL_TYPE`: Type of call (A, B, or C)
- `ORIGIN_CALL`: Origin call ID (only for CALL_TYPE A)
- `ORIGIN_STAND`: Origin stand ID (only for CALL_TYPE B)
- `TAXI_ID`: Taxi identifier
- `TIMESTAMP`: Unix timestamp of trip start
- `DAY_TYPE`: Day classification (A, B, or C)
- `MISSING_DATA`: Boolean indicating if trip has missing GPS data
- `POLYLINE`: JSON array of GPS coordinates [[lon, lat], [lon, lat], ...]

### Database Schema

The database consists of four main tables:

#### 1. `taxis`
- `taxi_id` (INT, PRIMARY KEY): Unique taxi identifier
- Contains 448 unique taxis

#### 2. `trips`
- `id` (BIGINT AUTO_INCREMENT, PRIMARY KEY): Internal trip ID
- `trip_uid` (VARCHAR(64), UNIQUE): Original trip ID from CSV
- `taxi_id` (INT, FOREIGN KEY): Reference to taxi
- `call_type` (ENUM 'A','B','C'): Type of call
- `origin_call` (INT, NULLABLE): Origin call ID
- `origin_stand` (INT, NULLABLE): Origin stand ID
- `day_type` (ENUM 'A','B','C'): Day classification
- `start_epoch` (INT UNSIGNED): Unix timestamp
- `start_ts` (DATETIME): Parsed timestamp
- `missing_data` (BOOLEAN): Whether trip has missing data
- Contains ~1.7M valid trips (excludes trips with missing GPS data)

#### 3. `gps_points`
- `id` (BIGINT AUTO_INCREMENT, PRIMARY KEY): Internal GPS point ID
- `longitude` (DECIMAL(10,7)): Longitude coordinate
- `latitude` (DECIMAL(10,7)): Latitude coordinate
- UNIQUE constraint on (longitude, latitude)
- Contains ~100K-200K unique GPS coordinates

#### 4. `trip_points`
- `id` (BIGINT AUTO_INCREMENT, PRIMARY KEY): Internal mapping ID
- `trip_id` (BIGINT, FOREIGN KEY): Reference to trip
- `point_id` (BIGINT, FOREIGN KEY): Reference to GPS point
- `seq` (INT UNSIGNED): Sequence number in trajectory
- `seconds_offset` (INT GENERATED): Calculated offset (seq * 15 seconds)
- UNIQUE constraint on (trip_id, seq)
- Maps trips to their GPS trajectories

## Data Analysis Summary

From the analysis in `part1.ipynb`:

### Dataset Statistics
- **Total rows**: 1,710,670
- **Valid trips** (MISSING_DATA=False): 1,710,660
- **Trips with missing data**: 10 (excluded from import)

### Key Findings
- **448 unique taxis** with varying activity levels
- **Top 5 most active taxis**: 20000080 (10,746 trips), 20000403 (9,238 trips), etc.
- **CALL_TYPE distribution**: A, B, C with different origin requirements
- **Significant missing values**:
  - ORIGIN_CALL: 1,345,900 missing (only present for CALL_TYPE A)
  - ORIGIN_STAND: 904,091 missing (only present for CALL_TYPE B)
- **81 duplicate TRIP_ID rows** (handled by unique constraint)
- **GPS trajectories vary greatly**: 0 to 3,881 points per trip
- **15-second intervals** between GPS points (based on generated column)

### Data Quality Notes
- Only 10 trips have missing GPS data (excluded from import)
- ORIGIN_CALL and ORIGIN_STAND have significant missing values
- POLYLINE contains temporal GPS trajectory with 15-second intervals
- 448 unique taxis with varying activity levels

## Setup Instructions

### 1. Database Setup
Run the database setup script to create all tables, indexes, and constraints:


```bash
python setup_database.py
```
Note this will clear the databse, if allready ran and populated.

This will:
- Create the `porto_taxi` database
- Set up all tables according to `changelog-master.xml`
- Create indexes for optimal query performance
- Add check constraints to enforce data integrity
- Create unique constraints to prevent duplicates

### 2. Data Import
Import the CSV data into the database:

```bash
python import/import_porto_taxi_data.py
```

This will:
- Import all unique taxi IDs
- Import trip data (excluding trips with missing GPS data)
- Parse POLYLINE JSON data into GPS coordinates
- Create mappings between trips and GPS points
- Handle duplicate GPS coordinates efficiently

### 3. Data Analysis
View the data analysis summary:

```bash
python data_analysis_summary.py
```

## File Descriptions

- `DbConnector.py`: Database connection class
- `setup_database.py`: Database schema setup script
- `import/import_porto_taxi_data.py`: Data import script
- `data_analysis_summary.py`: Data analysis summary
- `example.py`: Example usage of the database
- `part1.ipynb`: Original data analysis notebook
- `changelog-master.xml`: Database schema definition
- `porto.csv`: Source dataset (large file ~200MB)

## Usage Examples

### Basic Connection
```python
from DbConnector import DbConnector

connection = DbConnector()
# Use connection.db_connection and connection.cursor
connection.close_connection()
```

### Example Queries
```python
# Get all trips for a specific taxi
cursor.execute("SELECT * FROM trips WHERE taxi_id = %s", (20000080,))

# Get GPS trajectory for a trip
cursor.execute("""
    SELECT tp.seq, gp.longitude, gp.latitude, tp.seconds_offset
    FROM trip_points tp
    JOIN gps_points gp ON tp.point_id = gp.id
    WHERE tp.trip_id = %s
    ORDER BY tp.seq
""", (trip_id,))

# Find trips within a geographic area
cursor.execute("""
    SELECT t.*, COUNT(tp.id) as point_count
    FROM trips t
    JOIN trip_points tp ON t.id = tp.trip_id
    JOIN gps_points gp ON tp.point_id = gp.id
    WHERE gp.longitude BETWEEN %s AND %s
    AND gp.latitude BETWEEN %s AND %s
    GROUP BY t.id
""", (lon_min, lon_max, lat_min, lat_max))
```

## Notes

- The database is designed for efficient spatial and temporal queries
- GPS coordinates are stored with high precision (DECIMAL(10,7))
- The schema enforces data integrity through constraints
- Only valid trips (without missing GPS data) are imported
- The import process handles large datasets efficiently using batching
