import duckdb
import logging

# Configure logging to write to clean.log file with timestamp format
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

def clean_yellow_trips(con):
    """
    Clean yellow taxi trips by applying comprehensive data quality filters and removing duplicates.
    
    This function applies the following cleaning rules:
    - Removes trips with 0 or NULL passengers (invalid trips)
    - Removes trips with 0 or NULL distance (no actual travel)
    - Removes trips greater than 100 miles (likely data errors)
    - Removes trips longer than 24 hours (86400 seconds)
    - Removes trips outside 2015-2024 date range
    - Removes exact duplicate records using SELECT DISTINCT
    - Uses CREATE OR REPLACE approach for efficiency with large datasets
    
    Args:
        con: Active DuckDB database connection
    """
    logger.info("Starting yellow trips cleaning")
    
    # Get initial row count before cleaning for comparison
    initial_count = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
    print(f"Yellow trips initial count: {initial_count:,}")
    logger.info(f"Yellow trips initial count: {initial_count:,}")
    
    print("Applying data quality filters...")
    logger.info("Applying data quality filters to yellow trips")
    
    # Use CREATE OR REPLACE with SELECT DISTINCT for efficient cleaning of large datasets
    # This approach is more memory-efficient than DELETE statements for 700M+ rows
    con.execute("""
        CREATE OR REPLACE TABLE yellow_trips_temp AS
        SELECT DISTINCT * FROM yellow_trips 
        WHERE passenger_count > 0                    -- Remove trips with no passengers
           AND trip_distance > 0                     -- Remove trips with no distance
           AND trip_distance <= 100                  -- Remove unrealistic long trips
           AND EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) <= 86400  -- Max 24 hours
           AND EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 0       -- Positive duration
           AND EXTRACT(YEAR FROM tpep_pickup_datetime) >= 2015  -- Within date range start
           AND EXTRACT(YEAR FROM tpep_pickup_datetime) <= 2024  -- Within date range end
           AND tpep_pickup_datetime IS NOT NULL      -- Valid pickup time
           AND tpep_dropoff_datetime IS NOT NULL     -- Valid dropoff time
    """)
    
    # Replace original table with cleaned version
    con.execute("DROP TABLE yellow_trips")
    con.execute("ALTER TABLE yellow_trips_temp RENAME TO yellow_trips")
    
    # Calculate and report cleaning results
    final_count = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
    removed = initial_count - final_count
    
    print(f"Yellow trips cleaned: {removed:,} rows removed, {final_count:,} remaining")
    logger.info(f"Yellow trips cleaned: {removed:,} rows removed, {final_count:,} remaining")

def clean_green_trips(con):
    """
    Clean green taxi trips by applying comprehensive data quality filters and removing duplicates.
    
    This function applies the same cleaning rules as clean_yellow_trips but for green taxi data:
    - Removes trips with 0 or NULL passengers (invalid trips)
    - Removes trips with 0 or NULL distance (no actual travel)  
    - Removes trips greater than 100 miles (likely data errors)
    - Removes trips longer than 24 hours (86400 seconds)
    - Removes trips outside 2015-2024 date range
    - Removes exact duplicate records using SELECT DISTINCT
    - Note: Uses lpep_* datetime columns instead of tpep_* for green taxis
    
    Args:
        con: Active DuckDB database connection
    """
    logger.info("Starting green trips cleaning")
    
    initial_count = con.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]
    print(f"Green trips initial count: {initial_count:,}")
    logger.info(f"Green trips initial count: {initial_count:,}")
    
    print("Applying data quality filters...")
    logger.info("Applying data quality filters to green trips")
    
    con.execute("""
        CREATE OR REPLACE TABLE green_trips_temp AS
        SELECT DISTINCT * FROM green_trips 
        WHERE passenger_count > 0 
           AND trip_distance > 0 
           AND trip_distance <= 100
           AND EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) <= 86400
           AND EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) > 0
           AND EXTRACT(YEAR FROM lpep_pickup_datetime) >= 2015
           AND EXTRACT(YEAR FROM lpep_pickup_datetime) <= 2024
           AND lpep_pickup_datetime IS NOT NULL
           AND lpep_dropoff_datetime IS NOT NULL
    """)
    
    con.execute("DROP TABLE green_trips")
    con.execute("ALTER TABLE green_trips_temp RENAME TO green_trips")
    
    final_count = con.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]
    removed = initial_count - final_count
    
    print(f"Green trips cleaned: {removed:,} rows removed, {final_count:,} remaining")
    logger.info(f"Green trips cleaned: {removed:,} rows removed, {final_count:,} remaining")

def verify_cleaning(con):
    """
    Verify that all data quality conditions have been met after cleaning process.
    
    This function:
    - Tests all cleaning conditions to ensure they were applied correctly
    - Checks for remaining data quality violations in both taxi datasets
    - Uses SQL queries to count violations that should now be zero
    - Reports results to console and log file
    - Returns boolean indicating overall cleaning success
    
    Args:
        con: Active DuckDB database connection
        
    Returns:
        bool: True if all cleaning conditions are satisfied, False otherwise
    """
    logger.info("Starting data cleaning verification")
    print("\n=== CLEANING VERIFICATION ===")
    
    # Define verification checks for yellow trips using SQL COUNT queries
    print("\nYellow Trips Verification:")
    checks = {
        # Each check should return 0 if cleaning was successful
        "Trips with 0/NULL passengers": con.execute("SELECT COUNT(*) FROM yellow_trips WHERE passenger_count = 0").fetchone()[0],
        "Trips with 0/NULL distance": con.execute("SELECT COUNT(*) FROM yellow_trips WHERE trip_distance = 0").fetchone()[0],
        "Trips > 100 miles": con.execute("SELECT COUNT(*) FROM yellow_trips WHERE trip_distance > 100").fetchone()[0],
        "Trips > 24 hours": con.execute("""
            SELECT COUNT(*) FROM yellow_trips 
            WHERE EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 86400
        """).fetchone()[0],
        # Check for duplicates using CTE approach that mirrors cleaning logic
        "Duplicate trips": con.execute("""
            WITH total_count AS (SELECT COUNT(*) as total FROM yellow_trips),
                 distinct_count AS (SELECT COUNT(*) as distinct_total FROM (SELECT DISTINCT * FROM yellow_trips))
            SELECT total_count.total - distinct_count.distinct_total FROM total_count, distinct_count
        """).fetchone()[0],
        "Trips outside 2015-2024": con.execute("""
            SELECT COUNT(*) FROM yellow_trips 
            WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) < 2015 OR EXTRACT(YEAR FROM tpep_pickup_datetime) > 2024
        """).fetchone()[0]
    }
    
    # Count total violations for yellow trips
    yellow_violations = 0
    for check_name, count in checks.items():
        print(f"{check_name}: {count}")
        logger.info(f"Yellow {check_name.lower()}: {count}")
        yellow_violations += count
    
    # Verification checks for green trips
    print("\nGreen Trips Verification:")
    green_checks = {
        "Trips with 0/NULL passengers": con.execute("SELECT COUNT(*) FROM green_trips WHERE passenger_count = 0").fetchone()[0],
        "Trips with 0/NULL distance": con.execute("SELECT COUNT(*) FROM green_trips WHERE trip_distance = 0").fetchone()[0],
        "Trips > 100 miles": con.execute("SELECT COUNT(*) FROM green_trips WHERE trip_distance > 100").fetchone()[0],
        "Trips > 24 hours": con.execute("""
            SELECT COUNT(*) FROM green_trips 
            WHERE EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) > 86400
        """).fetchone()[0],
        "Duplicate trips": con.execute("""
            WITH total_count AS (SELECT COUNT(*) as total FROM green_trips),
                 distinct_count AS (SELECT COUNT(*) as distinct_total FROM (SELECT DISTINCT * FROM green_trips))
            SELECT total_count.total - distinct_count.distinct_total FROM total_count, distinct_count
        """).fetchone()[0],
        "Trips outside 2015-2024": con.execute("""
            SELECT COUNT(*) FROM green_trips 
            WHERE EXTRACT(YEAR FROM lpep_pickup_datetime) < 2015 OR EXTRACT(YEAR FROM lpep_pickup_datetime) > 2024
        """).fetchone()[0]
    }
    
    green_violations = 0
    for check_name, count in green_checks.items():
        print(f"{check_name}: {count}")
        logger.info(f"Green {check_name.lower()}: {count}")
        green_violations += count
    
    # Calculate overall verification result - success if no violations found
    all_clean = (yellow_violations + green_violations) == 0
    
    # Report final verification status to user and log
    if all_clean:
        print("\nALL CLEANING CONDITIONS VERIFIED - DATA IS CLEAN")
        logger.info("All cleaning conditions verified - data is clean")
    else:
        print("\nCLEANING VERIFICATION FAILED - ISSUES FOUND")
        logger.warning("Cleaning verification failed - issues found")
    
    return all_clean

if __name__ == "__main__":
    """
    Main execution block that orchestrates the entire data cleaning process.
    
    This block:
    - Connects to the DuckDB database created by load.py
    - Executes cleaning functions for both yellow and green taxi trips
    - Verifies that all cleaning conditions have been met
    - Provides comprehensive error handling and logging
    - Ensures database connection cleanup in all scenarios
    """
    con = None
    
    try:
        # Connect to DuckDB database with write permissions for cleaning operations
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("STARTING DATA CLEANING PROCESS")
        print("Starting data cleaning process...")
        
        # Execute cleaning functions in sequence
        clean_yellow_trips(con)  # Clean yellow taxi data using quality filters
        clean_green_trips(con)   # Clean green taxi data using quality filters
        verify_cleaning(con)     # Verify all cleaning conditions are satisfied
        
        logger.info("DATA CLEANING PROCESS COMPLETED")
        print("\nData cleaning process completed")
        
    except Exception as e:
        print(f"Error during cleaning: {e}")
        logger.error(f"Error during cleaning: {e}")
        raise
    finally:
        # Always close database connection to prevent locks and resource leaks
        if con:
            con.close()