-- Databricks notebook source
-- 1 . Ingest raw events into a streaming Bronze table.
-- 2. Apply a DLT expectation to drop records with non-positive trip_distance.
CREATE OR REFRESH STREAMING TABLE taxi_raw_records
(CONSTRAINT valid_distance EXPECT (trip_distance > 0.0) ON VIOLATION DROP ROW)
AS SELECT *
FROM STREAM(samples.nyctaxi.trips);
-- the taxi_raw_records consists of records with positive trip_distance
---------------------------------------------------------------------------
-- 3. Build a Silver table that flags suspicious rides
CREATE OR REFRESH STREAMING TABLE flagged_rides 
AS SELECT
  date_trunc("week", tpep_pickup_datetime) as week,
  pickup_zip as zip, 
  fare_amount, trip_distance
FROM
  STREAM(LIVE.taxi_raw_records)
WHERE ((pickup_zip = dropoff_zip AND fare_amount > 50) OR
       (trip_distance < 5 AND fare_amount > 50));

-- and another Silver table with weekly aggregates.
CREATE OR REFRESH MATERIALIZED VIEW weekly_aggregates
AS SELECT
  date_trunc("week", tpep_pickup_datetime) as week,
  AVG(fare_amount) as avg_amount,
  AVG(trip_distance) as avg_distance
FROM
 live.taxi_raw_records
GROUP BY week
ORDER BY week ASC;
-----------------------------------------------------------------------

--4. Produce a Gold materialized view showing top-3 highest fare rides with passenger info and expected schema.
CREATE OR REPLACE MATERIALIZED VIEW top_3
AS SELECT
  weekly_aggregates.week,
  ROUND(avg_amount,2) as avg_amount, 
  ROUND(avg_distance,3) as avg_distance,
  fare_amount, trip_distance, zip 
FROM live.flagged_rides
LEFT JOIN live.weekly_aggregates ON weekly_aggregates.week = flagged_rides.week
ORDER BY fare_amount DESC
LIMIT 3;
-- top_3 table consists of top-3 highest fare rides with passenger info and expected schema.
