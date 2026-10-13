-- Run in Snowsight AFTER benchmarks (ACCOUNT_USAGE views lag up to ~3 hours).
-- 1) Compute credits per warehouse per hour during your benchmark window
SELECT start_time, warehouse_name, credits_used_compute
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name = 'QSS_LAB_WH' AND start_time >= DATEADD('day', -2, CURRENT_TIMESTAMP())
ORDER BY start_time;

-- 2) Per-query detail for everything the harness ran (matches sf_query_id in benchmark_raw.csv)
SELECT query_id, query_tag, total_elapsed_time, execution_time, bytes_scanned,
       partitions_scanned, partitions_total, percentage_scanned_from_cache
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag = 'qss_benchmark' AND start_time >= DATEADD('day', -2, CURRENT_TIMESTAMP())
ORDER BY start_time;

-- 3) Clustering maintenance cost (Experiment 3)
SELECT table_name, SUM(credits_used) AS credits, SUM(num_rows_reclustered) AS rows_reclustered
FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
WHERE start_time >= DATEADD('day', -2, CURRENT_TIMESTAMP())
GROUP BY table_name;
