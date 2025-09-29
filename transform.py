import duckdb
import logging

# Configure logging to write to transform.log file with timestamp format
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)

def transform_yellow_trips(con):
    """
    Add calculated columns to yellow trips table using batched UPDATE operations.
    
    This function:
    - Adds 6 new columns: trip_co2_kgs, avg_mph, hour_of_day, day_of_week, week_of_year, month_of_year
    - Uses batched processing (1M rows per batch) to handle large dataset efficiently
    - Calculates CO2 emissions by joining with vehicle_emissions table
    - Extracts time-based features from pickup datetime
    - Processes 731M+ rows in manageable chunks to avoid memory issues
    
    Args:
        con: Active DuckDB database connection
    """
    logger.info("Starting yellow trips transformations")
    print("Transforming yellow trips data...")
    
    # Add calculated columns individually (DuckDB doesn't support multiple ADD COLUMN in one statement)
    con.execute("ALTER TABLE yellow_trips ADD COLUMN IF NOT EXISTS trip_co2_kgs DOUBLE")    # CO2 emissions in kilograms
    con.execute("ALTER TABLE yellow_trips ADD COLUMN IF NOT EXISTS avg_mph DOUBLE")         # Average speed in miles per hour
    con.execute("ALTER TABLE yellow_trips ADD COLUMN IF NOT EXISTS hour_of_day INTEGER")   # Hour of pickup (0-23)
    con.execute("ALTER TABLE yellow_trips ADD COLUMN IF NOT EXISTS day_of_week INTEGER")   # Day of week (0=Sunday)
    con.execute("ALTER TABLE yellow_trips ADD COLUMN IF NOT EXISTS week_of_year INTEGER")  # Week number (1-52)
    con.execute("ALTER TABLE yellow_trips ADD COLUMN IF NOT EXISTS month_of_year INTEGER") # Month number (1-12)
    
    # Process updates in batches to avoid memory exhaustion with large datasets
    print("Updating yellow trips in batches...")
    logger.info("Starting batched update of yellow trips")
    
    # Get total row count to calculate number of batches needed
    total_rows = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
    batch_size = 1000000  # Process 1M rows per batch for memory efficiency
    batches = (total_rows + batch_size - 1) // batch_size  # Calculate total batches (round up)
    
    print(f"Processing {total_rows:,} rows in {batches} batches of {batch_size:,} rows each...")
    
    # Process each batch individually to maintain memory constraints
    for i in range(batches):
        offset = i * batch_size  # Calculate starting row for this batch
        print(f"Processing batch {i+1}/{batches} (rows {offset:,} to {offset + batch_size - 1:,})")
        logger.info(f"Processing batch {i+1}/{batches}")
        
        # Update batch using rowid-based filtering for precise row targeting
        con.execute(f"""
            UPDATE yellow_trips 
            SET 
                trip_co2_kgs = (trip_distance * ve.co2_grams_per_mile) / 1000.0,      -- Convert grams to kg
                avg_mph = trip_distance / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0),  -- Miles per hour
                hour_of_day = EXTRACT(HOUR FROM tpep_pickup_datetime),               -- Hour (0-23)
                day_of_week = EXTRACT(DOW FROM tpep_pickup_datetime),                -- Day of week (0=Sunday)
                week_of_year = EXTRACT(WEEK FROM tpep_pickup_datetime),              -- Week number (1-52)
                month_of_year = EXTRACT(MONTH FROM tpep_pickup_datetime)             -- Month (1-12)
            FROM vehicle_emissions ve
            WHERE ve.vehicle_type = 'yellow_taxi'
            AND yellow_trips.rowid IN (
                SELECT rowid FROM yellow_trips 
                ORDER BY rowid 
                LIMIT {batch_size} OFFSET {offset}
            )
        """)
        
    print("Yellow trips batch processing completed!")
    
    # Get count of transformed rows
    count = con.execute("SELECT COUNT(*) FROM yellow_trips WHERE trip_co2_kgs IS NOT NULL").fetchone()[0]
    print(f"Yellow trips transformed: {count:,} rows")
    logger.info(f"Yellow trips transformed: {count:,} rows")

def transform_green_trips(con):
    """
    Add calculated columns to green trips table using batched UPDATE operations.
    
    This function mirrors transform_yellow_trips but processes green taxi data:
    - Adds the same 6 calculated columns as yellow trips
    - Uses lpep_* datetime columns instead of tpep_* columns
    - Applies the same batched processing approach for memory efficiency
    - Joins with vehicle_emissions using 'green_taxi' vehicle type
    
    Args:
        con: Active DuckDB database connection
    """
    logger.info("Starting green trips transformations")
    print("Transforming green trips data...")
    
    # Add calculated columns (one at a time due to DuckDB limitation)
    con.execute("ALTER TABLE green_trips ADD COLUMN IF NOT EXISTS trip_co2_kgs DOUBLE")
    con.execute("ALTER TABLE green_trips ADD COLUMN IF NOT EXISTS avg_mph DOUBLE")
    con.execute("ALTER TABLE green_trips ADD COLUMN IF NOT EXISTS hour_of_day INTEGER")
    con.execute("ALTER TABLE green_trips ADD COLUMN IF NOT EXISTS day_of_week INTEGER")
    con.execute("ALTER TABLE green_trips ADD COLUMN IF NOT EXISTS week_of_year INTEGER")
    con.execute("ALTER TABLE green_trips ADD COLUMN IF NOT EXISTS month_of_year INTEGER")
    
    # Update in batches to avoid memory issues
    print("Updating green trips in batches...")
    logger.info("Starting batched update of green trips")
    
    # Get total count for progress tracking
    total_rows = con.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]
    batch_size = 1000000  # 1M rows per batch
    batches = (total_rows + batch_size - 1) // batch_size
    
    print(f"Processing {total_rows:,} rows in {batches} batches of {batch_size:,} rows each...")
    
    for i in range(batches):
        offset = i * batch_size
        print(f"Processing batch {i+1}/{batches} (rows {offset:,} to {offset + batch_size - 1:,})")
        logger.info(f"Processing batch {i+1}/{batches}")
        
        con.execute(f"""
            UPDATE green_trips 
            SET 
                trip_co2_kgs = (trip_distance * ve.co2_grams_per_mile) / 1000.0,
                avg_mph = trip_distance / (EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600.0),
                hour_of_day = EXTRACT(HOUR FROM lpep_pickup_datetime),
                day_of_week = EXTRACT(DOW FROM lpep_pickup_datetime),
                week_of_year = EXTRACT(WEEK FROM lpep_pickup_datetime),
                month_of_year = EXTRACT(MONTH FROM lpep_pickup_datetime)
            FROM vehicle_emissions ve
            WHERE ve.vehicle_type = 'green_taxi'
            AND green_trips.rowid IN (
                SELECT rowid FROM green_trips 
                ORDER BY rowid 
                LIMIT {batch_size} OFFSET {offset}
            )
        """)
        
    print("Green trips batch processing completed!")
    
    # Get count of transformed rows
    count = con.execute("SELECT COUNT(*) FROM green_trips WHERE trip_co2_kgs IS NOT NULL").fetchone()[0]
    print(f"Green trips transformed: {count:,} rows")
    logger.info(f"Green trips transformed: {count:,} rows")

def verify_transformations(con):
    """Verify that all transformations have been applied correctly"""
    logger.info("Starting transformation verification")
    print("\n=== TRANSFORMATION VERIFICATION ===")
    
    # Check yellow trips
    print("\nYellow Trips Verification:")
    
    # Check for NULL values in new columns
    null_co2 = con.execute("SELECT COUNT(*) FROM yellow_trips WHERE trip_co2_kgs IS NULL").fetchone()[0]
    null_mph = con.execute("SELECT COUNT(*) FROM yellow_trips WHERE avg_mph IS NULL").fetchone()[0]
    null_hour = con.execute("SELECT COUNT(*) FROM yellow_trips WHERE hour_of_day IS NULL").fetchone()[0]
    null_dow = con.execute("SELECT COUNT(*) FROM yellow_trips WHERE day_of_week IS NULL").fetchone()[0]
    null_week = con.execute("SELECT COUNT(*) FROM yellow_trips WHERE week_of_year IS NULL").fetchone()[0]
    null_month = con.execute("SELECT COUNT(*) FROM yellow_trips WHERE month_of_year IS NULL").fetchone()[0]
    
    print(f"Trips with NULL trip_co2_kgs: {null_co2}")
    print(f"Trips with NULL avg_mph: {null_mph}")
    print(f"Trips with NULL hour_of_day: {null_hour}")
    print(f"Trips with NULL day_of_week: {null_dow}")
    print(f"Trips with NULL week_of_year: {null_week}")
    print(f"Trips with NULL month_of_year: {null_month}")
    
    # Sample statistics
    stats = con.execute("""
        SELECT 
            AVG(trip_co2_kgs) as avg_co2,
            AVG(avg_mph) as avg_speed,
            MIN(hour_of_day) as min_hour,
            MAX(hour_of_day) as max_hour,
            MIN(day_of_week) as min_dow,
            MAX(day_of_week) as max_dow,
            MIN(week_of_year) as min_week,
            MAX(week_of_year) as max_week,
            MIN(month_of_year) as min_month,
            MAX(month_of_year) as max_month
        FROM yellow_trips
    """).fetchone()
    
    print(f"Average CO2 per trip: {stats[0]:.3f} kg")
    print(f"Average speed: {stats[1]:.1f} mph")
    print(f"Hour range: {stats[2]} to {stats[3]}")
    print(f"Day of week range: {stats[4]} to {stats[5]}")
    print(f"Week range: {stats[6]} to {stats[7]}")
    print(f"Month range: {stats[8]} to {stats[9]}")
    
    logger.info(f"Yellow trips - NULL counts: CO2={null_co2}, MPH={null_mph}, Hour={null_hour}, DOW={null_dow}, Week={null_week}, Month={null_month}")
    logger.info(f"Yellow trips - Avg CO2: {stats[0]:.3f}, Avg Speed: {stats[1]:.1f}")
    
    # Check green trips
    print("\nGreen Trips Verification:")
    
    # Check for NULL values in new columns
    null_co2_g = con.execute("SELECT COUNT(*) FROM green_trips WHERE trip_co2_kgs IS NULL").fetchone()[0]
    null_mph_g = con.execute("SELECT COUNT(*) FROM green_trips WHERE avg_mph IS NULL").fetchone()[0]
    null_hour_g = con.execute("SELECT COUNT(*) FROM green_trips WHERE hour_of_day IS NULL").fetchone()[0]
    null_dow_g = con.execute("SELECT COUNT(*) FROM green_trips WHERE day_of_week IS NULL").fetchone()[0]
    null_week_g = con.execute("SELECT COUNT(*) FROM green_trips WHERE week_of_year IS NULL").fetchone()[0]
    null_month_g = con.execute("SELECT COUNT(*) FROM green_trips WHERE month_of_year IS NULL").fetchone()[0]
    
    print(f"Trips with NULL trip_co2_kgs: {null_co2_g}")
    print(f"Trips with NULL avg_mph: {null_mph_g}")
    print(f"Trips with NULL hour_of_day: {null_hour_g}")
    print(f"Trips with NULL day_of_week: {null_dow_g}")
    print(f"Trips with NULL week_of_year: {null_week_g}")
    print(f"Trips with NULL month_of_year: {null_month_g}")
    
    # Sample statistics
    stats_g = con.execute("""
        SELECT 
            AVG(trip_co2_kgs) as avg_co2,
            AVG(avg_mph) as avg_speed,
            MIN(hour_of_day) as min_hour,
            MAX(hour_of_day) as max_hour,
            MIN(day_of_week) as min_dow,
            MAX(day_of_week) as max_dow,
            MIN(week_of_year) as min_week,
            MAX(week_of_year) as max_week,
            MIN(month_of_year) as min_month,
            MAX(month_of_year) as max_month
        FROM green_trips
    """).fetchone()
    
    print(f"Average CO2 per trip: {stats_g[0]:.3f} kg")
    print(f"Average speed: {stats_g[1]:.1f} mph")
    print(f"Hour range: {stats_g[2]} to {stats_g[3]}")
    print(f"Day of week range: {stats_g[4]} to {stats_g[5]}")
    print(f"Week range: {stats_g[6]} to {stats_g[7]}")
    print(f"Month range: {stats_g[8]} to {stats_g[9]}")
    
    logger.info(f"Green trips - NULL counts: CO2={null_co2_g}, MPH={null_mph_g}, Hour={null_hour_g}, DOW={null_dow_g}, Week={null_week_g}, Month={null_month_g}")
    logger.info(f"Green trips - Avg CO2: {stats_g[0]:.3f}, Avg Speed: {stats_g[1]:.1f}")
    
    # Check if transformations were successful
    total_nulls = null_co2 + null_mph + null_hour + null_dow + null_week + null_month + null_co2_g + null_mph_g + null_hour_g + null_dow_g + null_week_g + null_month_g
    
    if total_nulls == 0 and stats[2] >= 0 and stats[3] <= 23 and stats_g[2] >= 0 and stats_g[3] <= 23:
        print("\nALL TRANSFORMATIONS VERIFIED - DATA IS TRANSFORMED")
        logger.info("All transformations verified - data is transformed")
        return True
    else:
        print("\nTRANSFORMATION VERIFICATION FAILED - ISSUES FOUND")
        logger.warning("Transformation verification failed - issues found")
        return False

def transform_data():
    """Main transformation function"""
    con = None
    
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("STARTING DATA TRANSFORMATION PROCESS")
        print("Starting data transformation process...")
        
        # Configure DuckDB settings for large dataset processing
        print("Configuring DuckDB settings for large dataset...")
        con.execute("SET memory_limit='12GB'")  # Increase memory limit significantly
        con.execute("SET threads=1")  # Use single thread to minimize memory usage
        con.execute("SET preserve_insertion_order=false")  # Disable order preservation for efficiency
        con.execute("PRAGMA max_temp_directory_size='100GB'")  # Allow much more temp space
        con.execute("SET temp_directory='/tmp'")  # Use system temp directory
        logger.info("DuckDB settings configured for large dataset processing")
        
        # Transform both taxi types
        transform_yellow_trips(con)
        transform_green_trips(con)
        
        # Verify transformations were successful
        verify_transformations(con)
        
        logger.info("DATA TRANSFORMATION PROCESS COMPLETED")
        print("\nData transformation process completed")
        
    except Exception as e:
        print(f"Error during transformation: {e}")
        logger.error(f"Error during transformation: {e}")
        raise
    finally:
        if con:
            con.close()

if __name__ == "__main__":
    # I am doing the transformation in DBT
    transform_data()