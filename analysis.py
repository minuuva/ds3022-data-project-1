import duckdb
import logging
import matplotlib.pyplot as plt
import pandas as pd

# Configure logging to write to analysis.log file with timestamp format
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)

def largest_co2_trip_analysis(con):
    """
    Find the single largest carbon producing trip for each cab type (YELLOW and GREEN).
    
    This function:
    - Queries both transformed tables to find maximum CO2 emissions per trip
    - Uses ORDER BY trip_co2_kgs DESC LIMIT 1 to get the top trip
    - Displays trip details including CO2 amount, distance, and datetime
    - Satisfies rubric requirement for "single largest carbon producing trip"
    
    Args:
        con: Active DuckDB database connection (read-only)
    """
    print("\n=== LARGEST CO2 PRODUCING TRIPS ===")
    logger.info("Starting largest CO2 trip analysis")
    
    # Yellow taxi largest CO2 trip
    yellow_max = con.execute("""
        SELECT trip_co2_kgs, trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime
        FROM yellow_trips_transformed 
        WHERE trip_co2_kgs IS NOT NULL
        ORDER BY trip_co2_kgs DESC 
        LIMIT 1
    """).fetchone()
    
    # Green taxi largest CO2 trip
    green_max = con.execute("""
        SELECT trip_co2_kgs, trip_distance, lpep_pickup_datetime, lpep_dropoff_datetime
        FROM green_trips_transformed 
        WHERE trip_co2_kgs IS NOT NULL
        ORDER BY trip_co2_kgs DESC 
        LIMIT 1
    """).fetchone()
    
    print(f"YELLOW TAXI - Largest CO2 producing trip: {yellow_max[0]:.3f} kg CO2")
    print(f"  Trip distance: {yellow_max[1]:.2f} miles")
    print(f"  Pickup time: {yellow_max[2]}")
    print(f"  Dropoff time: {yellow_max[3]}")
    
    print(f"\nGREEN TAXI - Largest CO2 producing trip: {green_max[0]:.3f} kg CO2")
    print(f"  Trip distance: {green_max[1]:.2f} miles")
    print(f"  Pickup time: {green_max[2]}")
    print(f"  Dropoff time: {green_max[3]}")
    
    logger.info(f"Yellow max CO2: {yellow_max[0]:.3f} kg, Green max CO2: {green_max[0]:.3f} kg")

def co2_by_hour_analysis(con):
    """
    Calculate most/least carbon heavy hours of day for each cab type (1-24).
    
    This function satisfies the rubric requirement to find the most and least 
    carbon heavy hours across the entire year for both YELLOW and GREEN trips.
    Uses AVG(trip_co2_kgs) grouped by hour_of_day to find patterns.
    
    Args:
        con: Active DuckDB database connection (read-only)
    """
    print("\n=== CO2 ANALYSIS BY HOUR OF DAY ===")
    logger.info("Starting CO2 by hour analysis")
    
    # Yellow taxi by hour
    yellow_hours = con.execute("""
        SELECT hour_of_day, AVG(trip_co2_kgs) as avg_co2
        FROM yellow_trips_transformed 
        WHERE trip_co2_kgs IS NOT NULL AND hour_of_day IS NOT NULL
        GROUP BY hour_of_day
        ORDER BY hour_of_day
    """).fetchall()
    
    # Green taxi by hour  
    green_hours = con.execute("""
        SELECT hour_of_day, AVG(trip_co2_kgs) as avg_co2
        FROM green_trips_transformed 
        WHERE trip_co2_kgs IS NOT NULL AND hour_of_day IS NOT NULL
        GROUP BY hour_of_day
        ORDER BY hour_of_day
    """).fetchall()
    
    # Find max/min for yellow
    yellow_max_hour = max(yellow_hours, key=lambda x: x[1])
    yellow_min_hour = min(yellow_hours, key=lambda x: x[1])
    
    # Find max/min for green
    green_max_hour = max(green_hours, key=lambda x: x[1])
    green_min_hour = min(green_hours, key=lambda x: x[1])
    
    print(f"YELLOW TAXI - Most carbon heavy hour: {yellow_max_hour[0]:02d}:00 ({yellow_max_hour[1]:.4f} kg CO2 avg)")
    print(f"YELLOW TAXI - Least carbon heavy hour: {yellow_min_hour[0]:02d}:00 ({yellow_min_hour[1]:.4f} kg CO2 avg)")
    print(f"GREEN TAXI - Most carbon heavy hour: {green_max_hour[0]:02d}:00 ({green_max_hour[1]:.4f} kg CO2 avg)")
    print(f"GREEN TAXI - Least carbon heavy hour: {green_min_hour[0]:02d}:00 ({green_min_hour[1]:.4f} kg CO2 avg)")
    
    logger.info(f"Yellow peak hour: {yellow_max_hour[0]}, Green peak hour: {green_max_hour[0]}")

def co2_by_day_analysis(con):
    """Calculate most/least carbon heavy days of week for each cab type"""
    print("\n=== CO2 ANALYSIS BY DAY OF WEEK ===")
    logger.info("Starting CO2 by day of week analysis")
    
    day_names = {0: 'Sunday', 1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 
                 4: 'Thursday', 5: 'Friday', 6: 'Saturday'}
    
    # Yellow taxi by day
    yellow_days = con.execute("""
        SELECT day_of_week, AVG(trip_co2_kgs) as avg_co2
        FROM yellow_trips_transformed 
        WHERE trip_co2_kgs IS NOT NULL AND day_of_week IS NOT NULL
        GROUP BY day_of_week
        ORDER BY day_of_week
    """).fetchall()
    
    # Green taxi by day
    green_days = con.execute("""
        SELECT day_of_week, AVG(trip_co2_kgs) as avg_co2
        FROM green_trips_transformed 
        WHERE trip_co2_kgs IS NOT NULL AND day_of_week IS NOT NULL
        GROUP BY day_of_week
        ORDER BY day_of_week
    """).fetchall()
    
    # Find max/min for yellow
    yellow_max_day = max(yellow_days, key=lambda x: x[1])
    yellow_min_day = min(yellow_days, key=lambda x: x[1])
    
    # Find max/min for green
    green_max_day = max(green_days, key=lambda x: x[1])
    green_min_day = min(green_days, key=lambda x: x[1])
    
    print(f"YELLOW TAXI - Most carbon heavy day: {day_names[yellow_max_day[0]]} ({yellow_max_day[1]:.4f} kg CO2 avg)")
    print(f"YELLOW TAXI - Least carbon heavy day: {day_names[yellow_min_day[0]]} ({yellow_min_day[1]:.4f} kg CO2 avg)")
    print(f"GREEN TAXI - Most carbon heavy day: {day_names[green_max_day[0]]} ({green_max_day[1]:.4f} kg CO2 avg)")
    print(f"GREEN TAXI - Least carbon heavy day: {day_names[green_min_day[0]]} ({green_min_day[1]:.4f} kg CO2 avg)")
    
    logger.info(f"Yellow peak day: {day_names[yellow_max_day[0]]}, Green peak day: {day_names[green_max_day[0]]}")

def co2_by_week_analysis(con):
    """Calculate most/least carbon heavy weeks of year for each cab type"""
    print("\n=== CO2 ANALYSIS BY WEEK OF YEAR ===")
    logger.info("Starting CO2 by week of year analysis")
    
    # Yellow taxi by week
    yellow_weeks = con.execute("""
        SELECT week_of_year, AVG(trip_co2_kgs) as avg_co2
        FROM yellow_trips_transformed 
        WHERE trip_co2_kgs IS NOT NULL AND week_of_year IS NOT NULL
        GROUP BY week_of_year
        ORDER BY week_of_year
    """).fetchall()
    
    # Green taxi by week
    green_weeks = con.execute("""
        SELECT week_of_year, AVG(trip_co2_kgs) as avg_co2
        FROM green_trips_transformed 
        WHERE trip_co2_kgs IS NOT NULL AND week_of_year IS NOT NULL
        GROUP BY week_of_year
        ORDER BY week_of_year
    """).fetchall()
    
    # Find max/min for yellow
    yellow_max_week = max(yellow_weeks, key=lambda x: x[1])
    yellow_min_week = min(yellow_weeks, key=lambda x: x[1])
    
    # Find max/min for green
    green_max_week = max(green_weeks, key=lambda x: x[1])
    green_min_week = min(green_weeks, key=lambda x: x[1])
    
    print(f"YELLOW TAXI - Most carbon heavy week: Week {yellow_max_week[0]} ({yellow_max_week[1]:.4f} kg CO2 avg)")
    print(f"YELLOW TAXI - Least carbon heavy week: Week {yellow_min_week[0]} ({yellow_min_week[1]:.4f} kg CO2 avg)")
    print(f"GREEN TAXI - Most carbon heavy week: Week {green_max_week[0]} ({green_max_week[1]:.4f} kg CO2 avg)")
    print(f"GREEN TAXI - Least carbon heavy week: Week {green_min_week[0]} ({green_min_week[1]:.4f} kg CO2 avg)")
    
    logger.info(f"Yellow peak week: {yellow_max_week[0]}, Green peak week: {green_max_week[0]}")

def co2_by_month_analysis(con):
    """Calculate most/least carbon heavy months of year for each cab type"""
    print("\n=== CO2 ANALYSIS BY MONTH OF YEAR ===")
    logger.info("Starting CO2 by month of year analysis")
    
    month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June',
                   7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}
    
    # Yellow taxi by month
    yellow_months = con.execute("""
        SELECT month_of_year, AVG(trip_co2_kgs) as avg_co2
        FROM yellow_trips_transformed 
        WHERE trip_co2_kgs IS NOT NULL AND month_of_year IS NOT NULL
        GROUP BY month_of_year
        ORDER BY month_of_year
    """).fetchall()
    
    # Green taxi by month
    green_months = con.execute("""
        SELECT month_of_year, AVG(trip_co2_kgs) as avg_co2
        FROM green_trips_transformed 
        WHERE trip_co2_kgs IS NOT NULL AND month_of_year IS NOT NULL
        GROUP BY month_of_year
        ORDER BY month_of_year
    """).fetchall()
    
    # Find max/min for yellow
    yellow_max_month = max(yellow_months, key=lambda x: x[1])
    yellow_min_month = min(yellow_months, key=lambda x: x[1])
    
    # Find max/min for green
    green_max_month = max(green_months, key=lambda x: x[1])
    green_min_month = min(green_months, key=lambda x: x[1])
    
    print(f"YELLOW TAXI - Most carbon heavy month: {month_names[yellow_max_month[0]]} ({yellow_max_month[1]:.4f} kg CO2 avg)")
    print(f"YELLOW TAXI - Least carbon heavy month: {month_names[yellow_min_month[0]]} ({yellow_min_month[1]:.4f} kg CO2 avg)")
    print(f"GREEN TAXI - Most carbon heavy month: {month_names[green_max_month[0]]} ({green_max_month[1]:.4f} kg CO2 avg)")
    print(f"GREEN TAXI - Least carbon heavy month: {month_names[green_min_month[0]]} ({green_min_month[1]:.4f} kg CO2 avg)")
    
    logger.info(f"Yellow peak month: {month_names[yellow_max_month[0]]}, Green peak month: {month_names[green_max_month[0]]}")
    
    return yellow_months, green_months

def create_co2_plot(con):
    """
    Generate time-series plot with YEAR on X-axis and CO2 totals on Y-axis.
    
    This function creates a matplotlib plot with:
    - YEAR along the X-axis (2015-2024)
    - CO2 totals along the Y-axis  
    - Two lines/plots: one for YELLOW and one for GREEN taxi trip CO2 totals
    - Output as PNG image file committed to the project
    
    Args:
        con: Active DuckDB database connection (read-only)
    """
    print("\n=== GENERATING CO2 BY YEAR PLOT ===")
    logger.info("Creating CO2 by year plot")
    
    # Get yearly CO2 totals for both taxi types
    yellow_yearly = con.execute("""
        SELECT year, SUM(trip_co2_kgs) as total_co2
        FROM yellow_trips_transformed 
        WHERE trip_co2_kgs IS NOT NULL AND year IS NOT NULL
        GROUP BY year
        ORDER BY year
    """).fetchall()
    
    green_yearly = con.execute("""
        SELECT year, SUM(trip_co2_kgs) as total_co2
        FROM green_trips_transformed 
        WHERE trip_co2_kgs IS NOT NULL AND year IS NOT NULL
        GROUP BY year
        ORDER BY year
    """).fetchall()
    
    # Convert to pandas DataFrames for easier plotting
    yellow_df = pd.DataFrame(yellow_yearly, columns=['year', 'total_co2'])
    green_df = pd.DataFrame(green_yearly, columns=['year', 'total_co2'])
    
    # Create the plot
    plt.figure(figsize=(12, 8))
    plt.plot(yellow_df['year'], yellow_df['total_co2']/1000000, 'o-', 
             label='Yellow Taxi', linewidth=2, markersize=8, color='#FFD700')
    plt.plot(green_df['year'], green_df['total_co2']/1000000, 's-', 
             label='Green Taxi', linewidth=2, markersize=8, color='#32CD32')
    
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Total CO2 Emissions (Million kg)', fontsize=12)
    plt.title('NYC Taxi CO2 Emissions by Year (2015-2024)', fontsize=14, fontweight='bold')
    plt.legend(fontsize=11)
    plt.grid(True, alpha=0.3)
    plt.xticks(yellow_df['year'])
    
    # Add value annotations on the points
    for i, row in yellow_df.iterrows():
        plt.annotate(f'{row["total_co2"]/1000000:.1f}', 
                    (row['year'], row['total_co2']/1000000), 
                    textcoords="offset points", xytext=(0,10), ha='center', fontsize=9)
    
    for i, row in green_df.iterrows():
        plt.annotate(f'{row["total_co2"]/1000000:.1f}', 
                    (row['year'], row['total_co2']/1000000), 
                    textcoords="offset points", xytext=(0,-15), ha='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig('co2_emissions_by_year.png', dpi=300, bbox_inches='tight')
    print("Plot saved as 'co2_emissions_by_year.png'")
    logger.info("CO2 emissions plot saved successfully")
    
    # Print summary statistics
    print(f"\nYellow Taxi Total CO2 (2015-2024): {yellow_df['total_co2'].sum()/1000000:.1f} million kg")
    print(f"Green Taxi Total CO2 (2015-2024): {green_df['total_co2'].sum()/1000000:.1f} million kg")
    print(f"Combined Total CO2 (2015-2024): {(yellow_df['total_co2'].sum() + green_df['total_co2'].sum())/1000000:.1f} million kg")

def main():
    """
    Main analysis function that orchestrates comprehensive CO2 emissions analysis.
    
    This function:
    - Connects to DuckDB database containing transformed taxi trip data
    - Executes all required analysis functions per the project rubric
    - Provides comprehensive error handling and logging
    - Generates both text outputs and visualization plot
    - Ensures proper database connection cleanup
    """
    print("=" * 60)
    print("NYC TAXI CO2 EMISSIONS ANALYSIS (2015-2024)")
    print("=" * 60)
    logger.info("Starting NYC taxi CO2 emissions analysis")
    
    con = None
    try:
        # Connect to database
        con = duckdb.connect(database='emissions.duckdb', read_only=True)
        logger.info("Connected to DuckDB database")
        
        # Verify data availability
        yellow_count = con.execute("SELECT COUNT(*) FROM yellow_trips_transformed WHERE trip_co2_kgs IS NOT NULL").fetchone()[0]
        green_count = con.execute("SELECT COUNT(*) FROM green_trips_transformed WHERE trip_co2_kgs IS NOT NULL").fetchone()[0]
        
        print(f"Analyzing {yellow_count:,} yellow taxi trips and {green_count:,} green taxi trips")
        logger.info(f"Processing {yellow_count:,} yellow and {green_count:,} green trips")
        
        # Run all analyses
        largest_co2_trip_analysis(con)
        co2_by_hour_analysis(con)
        co2_by_day_analysis(con)
        co2_by_week_analysis(con)
        co2_by_month_analysis(con)
        create_co2_plot(con)
        
        print("\n" + "=" * 60)
        print("ANALYSIS COMPLETED SUCCESSFULLY")
        print("=" * 60)
        logger.info("NYC taxi CO2 emissions analysis completed successfully")
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        logger.error(f"Error during analysis: {e}")
    finally:
        if con:
            con.close()

if __name__ == "__main__":
    main()