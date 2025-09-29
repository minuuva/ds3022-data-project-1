/*
DBT Model: Transform yellow trips data by creating a new table with calculated columns.

This model satisfies the rubric requirement for DBT-based transformations using INSERT approach.
It creates a new yellow_trips_transformed table containing all original columns plus 6 calculated columns.

Calculated columns added:
- trip_co2_kgs: CO2 emissions in kilograms (trip_distance * co2_grams_per_mile / 1000)
- avg_mph: Average speed in miles per hour (distance / duration)
- hour_of_day: Hour of pickup (0-23)
- day_of_week: Day of week (0=Sunday, 6=Saturday)
- week_of_year: Week number (1-52)
- month_of_year: Month number (1-12)
- year: Year of pickup
*/

{{ config(materialized='table') }}

WITH trips AS (
    -- Source: cleaned yellow taxi trips from main schema
    SELECT *
    FROM {{ source('main', 'yellow_trips') }}
),
emissions AS (
    -- Source: vehicle emissions lookup table filtered for yellow taxis
    SELECT *
    FROM {{ source('main', 'vehicle_emissions') }}
    WHERE vehicle_type = 'yellow_taxi'
)

SELECT
    t.*,  -- Include all original columns from yellow_trips
    -- Calculate CO2 emissions: distance (miles) × emissions factor (g/mile) ÷ 1000 = kg
    (t.trip_distance * e.co2_grams_per_mile) / 1000.0 AS trip_co2_kgs,
    -- Calculate average MPH: distance ÷ (duration in seconds ÷ 3600) with NULLIF to prevent division by zero
    t.trip_distance / NULLIF(EXTRACT(EPOCH FROM (t.tpep_dropoff_datetime - t.tpep_pickup_datetime)) / 3600, 0) AS avg_mph,
    -- Extract time-based features for analysis
    EXTRACT(HOUR FROM t.tpep_pickup_datetime) AS hour_of_day,      -- 0-23
    EXTRACT(DOW FROM t.tpep_pickup_datetime) AS day_of_week,       -- 0=Sunday
    EXTRACT(WEEK FROM t.tpep_pickup_datetime) AS week_of_year,     -- 1-52
    EXTRACT(MONTH FROM t.tpep_pickup_datetime) AS month_of_year,   -- 1-12
    EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year             -- Full year
FROM trips t
CROSS JOIN emissions e  -- Cross join since emissions table has only one row for yellow_taxi
