-- Databricks notebook source
-- 5. Provide a short notebook showing sample queries and validation that the pipeline produces correct results and lineage is visible.
-- To validate records of bronze[taxi_raw_records] table
select * from gayatri_48.default.taxi_raw_records;

-- COMMAND ----------

-- To validate table that flags suspicious rides
select * from gayatri_48.default.flagged_rides;

-- COMMAND ----------

-- to validate Silver table with weekly aggregates.
select * from gayatri_48.default.weekly_aggregates;

-- COMMAND ----------

-- to validate table showing top-3 highest fare rides
SELECT * FROM gayatri_48.default.top_3
ORDER BY avg_amount DESC, fare_amount DESC;

-- COMMAND ----------

-- list recent lineage events (requires system catalog access & privileges)
SELECT * 
FROM system.lineage_events
WHERE target_table_name LIKE '%top_3%'
ORDER BY event_time DESC
LIMIT 50;
-- Lineage is visible in the DLT graph attached along.