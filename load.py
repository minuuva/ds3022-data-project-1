import duckdb
import time
import logging
import pandas as pd

# Configure logging to write to file with timestamp format
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():
    """
    Load yellow and green taxi parquet files from 2015-2024 into DuckDB tables.
    
    This function:
    - Creates empty yellow_trips and green_trips tables with proper schema
    - Loops through all combinations of taxi colors (yellow/green), years (2015-2024), and months (1-12)
    - Downloads and inserts data from NYC taxi parquet files using DuckDB's read_parquet function
    - Implements rate limiting (20 second delay) to avoid overloading the data source
    - Handles missing files gracefully with try/catch blocks
    - Logs all operations for debugging and monitoring
    """
    con = None
    
    try:
        # Establish connection to DuckDB database file
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        
        # Create empty tables with proper schema by reading one sample file
        # Using WHERE 1=0 creates table structure without data
        con.execute("""
            CREATE OR REPLACE TABLE yellow_trips AS 
            SELECT * FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet')
            WHERE 1=0  -- Create empty table with schema
        """)
        
        con.execute("""
            CREATE OR REPLACE TABLE green_trips AS 
            SELECT * FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet')
            WHERE 1=0  -- Create empty table with schema
        """)
        
        logger.info("Created empty tables with schema")
        
        # Define parameters for data loading loop
        colors = ['yellow', 'green']    # Two taxi types
        years = range(2015, 2025)       # 2015-2024 inclusive (10 years)
        months = range(1, 13)           # 1-12 (12 months)
        
        # Calculate total files for progress tracking (2 colors × 10 years × 12 months = 240 files)
        total_files = len(colors) * len(years) * len(months)
        current_file = 0
        
        # Triple nested loop to load all taxi data files
        for color in colors:
            for year in years:
                for month in months:
                    current_file += 1
                    # Construct URL using f-string formatting for dynamic file paths
                    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02d}.parquet"
                    
                    try:
                        # Display progress to user
                        print(f"Loading {current_file}/{total_files}: {color} {year}-{month:02d}")
                        logger.info(f"Loading {url}")
                        
                        # Insert data directly from URL using DuckDB's read_parquet function
                        con.execute(f"""
                            INSERT INTO {color}_trips 
                            SELECT * FROM read_parquet('{url}')
                        """)
                        
                        print(f"✓ Successfully loaded {color} {year}-{month:02d}")
                        logger.info(f"Successfully loaded {color} {year}-{month:02d}")
                        
                    except Exception as e:
                        # Handle missing files or network errors gracefully
                        print(f"✗ Failed to load {color} {year}-{month:02d}: {e}")
                        logger.warning(f"Failed to load {color} {year}-{month:02d}: {e}")
                    
                    # Rate limiting: Sleep 20 seconds between requests to avoid overwhelming server
                    # Skip sleep after the last file to finish faster
                    if current_file < total_files:
                        time.sleep(20)
                        
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        # Always close database connection to prevent locks
        if con:
            con.close()

def load_emissions_data():
    """
    Load vehicle emissions data from local CSV file into DuckDB table.
    
    This function:
    - Reads the vehicle_emissions.csv file using pandas
    - Registers the dataframe as a temporary table in DuckDB
    - Creates a permanent vehicle_emissions table from the temporary data
    - Provides CO2 emissions factors (grams per mile) for yellow and green taxis
    """
    con = None
    
    try:
        # Connect to the same DuckDB database
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Loading emissions data")
        
        # Read CSV file into pandas DataFrame for easy data handling
        emissions_df = pd.read_csv('data/vehicle_emissions.csv')
        
        # Register DataFrame as temporary table in DuckDB for SQL operations
        con.register('emissions_temp', emissions_df)
        
        # Create permanent table from temporary registered DataFrame
        con.execute("""
            CREATE OR REPLACE TABLE vehicle_emissions AS 
            SELECT * FROM emissions_temp
        """)
        
        # Verify data loaded correctly and report count to user
        count = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
        print(f"Loaded {count} vehicle emission records")
        logger.info(f"Loaded {count} vehicle emission records")
        
    except Exception as e:
        print(f"Error loading emissions data: {e}")
        logger.error(f"Error loading emissions data: {e}")
        raise
    finally:
        # Clean up database connection
        if con:
            con.close()

def basic_data_summarization():
    """
    Generate and display basic summary statistics for all loaded data.
    
    This function:
    - Connects to DuckDB in read-only mode for data analysis
    - Calculates key metrics (count, date ranges, distances) for both taxi types
    - Formats and displays comprehensive summary statistics to console and log
    - Provides verification that data loading completed successfully
    """
    con = None
    
    try:
        # Connect in read-only mode since we're only querying data
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Generating data summary")
        
        # Query yellow taxi summary statistics using aggregate functions
        yellow_stats = con.execute("""
            SELECT 
                COUNT(*) as total_trips,
                MIN(tpep_pickup_datetime) as earliest_trip,
                MAX(tpep_pickup_datetime) as latest_trip,
                AVG(trip_distance) as avg_distance,
                SUM(trip_distance) as total_distance
            FROM yellow_trips
        """).fetchone()
        
        # Query green taxi summary statistics (note different datetime column names)
        green_stats = con.execute("""
            SELECT 
                COUNT(*) as total_trips,
                MIN(lpep_pickup_datetime) as earliest_trip,
                MAX(lpep_pickup_datetime) as latest_trip,
                AVG(trip_distance) as avg_distance,
                SUM(trip_distance) as total_distance
            FROM green_trips
        """).fetchone()
        
        # Get count of emissions lookup records
        emissions_count = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
        
        # Format comprehensive summary report with proper number formatting
        summary = f"""
DATA LOADING SUMMARY
Yellow Taxi Trips: {yellow_stats[0]:,}
  Date Range: {yellow_stats[1]} to {yellow_stats[2]}
  Average Distance: {yellow_stats[3]:.2f} miles
  Total Distance: {yellow_stats[4]:,.0f} miles

Green Taxi Trips: {green_stats[0]:,}
  Date Range: {green_stats[1]} to {green_stats[2]}  
  Average Distance: {green_stats[3]:.2f} miles
  Total Distance: {green_stats[4]:,.0f} miles

Vehicle Emissions Records: {emissions_count}
Total Combined Trips: {yellow_stats[0] + green_stats[0]:,}
        """
        
        # Output summary to both console and log file
        print(summary)
        logger.info(summary)
        
    except Exception as e:
        print(f"Error generating summary: {e}")
        logger.error(f"Error generating summary: {e}")
        raise
    finally:
        # Ensure database connection is properly closed
        if con:
            con.close()

if __name__ == "__main__":
    """
    Main execution block that orchestrates the entire data loading process.
    
    Executes three key functions in sequence:
    1. load_parquet_files() - Downloads and loads 240 taxi data files
    2. load_emissions_data() - Loads vehicle emissions lookup table  
    3. basic_data_summarization() - Generates summary statistics
    
    Includes comprehensive error handling and logging for the entire process.
    """
    try:
        logger.info("STARTING DATA LOADING PROCESS")
        print("Starting NYC Taxi Data Loading Process (2015-2024)")
        
        # Execute the three main loading functions in sequence
        load_parquet_files()      # Load taxi trip data (240 files)
        load_emissions_data()     # Load emissions lookup table
        basic_data_summarization() # Generate summary statistics
        
        logger.info("DATA LOADING PROCESS COMPLETED")
        print("\nLOADING PROCESS COMPLETED")
        
    except Exception as e:
        # Log fatal errors and exit with error code
        logger.error(f"Fatal error in main execution: {e}")
        print(f"Fatal error: {e}")
        exit(1)