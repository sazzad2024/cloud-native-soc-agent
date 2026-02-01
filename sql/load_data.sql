-- 0. Set Context
USE CATALOG workspace;
USE SCHEMA default;

-- 1. Create Bronze (Optional - Only run if you haven't already)
-- CREATE OR REPLACE TABLE workspace.default.bronze_traffic_logs
-- USING DELTA
-- TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
-- AS SELECT * FROM read_files(
--   '/Volumes/workspace/default/raw_data',
--   format => 'csv',
--   header => true,
--   inferSchema => true
-- );

-- 2. Create Gold (NATURAL MODE)
-- We Group by the REAL 'Src IP' from the logs.
-- This gives us a profile for every single IP in the dataset.
CREATE OR REPLACE TABLE workspace.default.gold_network_telemetry AS
SELECT 
    `Src IP` as source_ip, 
    count(*) as total_packets,
    sum(try_cast(`TotLen Fwd Pkts` as BIGINT) + try_cast(`TotLen Bwd Pkts` as BIGINT)) as total_bytes, 
    85 as risk_score
FROM workspace.default.bronze_traffic_logs
WHERE Label != 'Benign'
GROUP BY `Src IP`;

-- 3. Verify
-- Show the top attackers we captured
SELECT * FROM workspace.default.gold_network_telemetry ORDER BY total_bytes DESC LIMIT 10;
