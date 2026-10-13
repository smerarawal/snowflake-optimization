# Medium-only Snowflake POC: status and finish plan

Reviewed 13 October 2026 from C:\snowflake-optimization-clean. No live Snowflake queries were run during this review.

## Scope

Demonstrate query design, physical data layout, and aggregate refresh tradeoffs using a synthetic multi-tenant appointment platform. Use only M: 1,000,000 appointments, 100 tenants, 600 services, 2,299,982 queue events, 1,464,037 notifications, and 3,000,017 audit events. Benchmark evidence currently focuses on appointments and service joins; generated event tables are not evidence of tested workloads.

Describe the work as a Snowflake optimization POC inspired by the QSS architecture. MySQL-versus-Snowflake performance, production contention reduction, CDC, concurrency isolation, and ROI have not been demonstrated by these files. Keep these as future work.

## Completed evidence

The saved benchmark_summary.csv contains experiments 2, 3, and 4. It has 10 warm and 3 cold measurements per query configuration for experiments 2 and 3, and 10 warm measurements per refresh configuration for experiment 4. These differ from the earlier 2-warm/1-cold console output pasted into the chat. Use one coherent saved result set in the final presentation.

Results use an X-Small warehouse with result caching disabled. Client elapsed time includes execution and fetching; cold results include warehouse resume overhead. Warehouse local caching may affect warm results.

| Comparison | Saved warm median | Interpretation |
|---|---|---|
| Full date range vs one-month daily counts | 1,968.8 vs 318.6 ms | 6.18x lower end-to-end latency for a narrower request; results differ, so this is not an equivalent-query speedup. Both scan 5/5 partitions. |
| All columns vs required columns | 4,336.6 vs 1,550.1 ms | 2.80x lower end-to-end latency; less result data is fetched. Saved scanned bytes are equal, so do not claim reduced scan bytes. |
| Join then aggregate vs aggregate then join | 216.2 vs 193.9 ms | Small 1.12x difference; cold runs slightly favor the original join. No decisive improvement. |
| Unsorted vs date-sorted clustered copy, date query | 107.0 vs 124.7 ms | Scan improves from 5/5 to 1/8 partitions and 47,140,864 to 5,486,080 bytes (~88.4% fewer bytes), but warm latency does not improve. |
| Full refresh vs incremental affected-key MERGE | 605.8 vs 726.8 ms | Incremental refresh is about 20% slower at this scale. It scans more bytes; do not present it as a speedup. |

The clustered copy was built with ORDER BY created_at and a clustering key, then automatic reclustering was suspended. This tests a physically sorted copy; it does not establish ongoing automatic clustering benefit or maintenance cost.

exp4_correctness.json reports 57,537 delta rows and 2,939 affected tenant/date keys. Both full and incremental outputs contain 67,743 rows, with zero differences in either direction. This validates the simulated cutoff refresh for this dataset, not a live CDC pipeline or arbitrary updates/deletes.

## Fastest finish

1. Keep existing benchmark evidence; no larger dataset or further cold runs are required for this POC.
2. Include the results table above and a screenshot of the zero-difference correctness result.
3. Add one simple tenant/date business report from the existing daily metrics: appointment volume, cancellations, and no-show rate. Export or screenshot the result; do not invent findings before running it.
4. Prepare a short presentation: business problem; proposed MySQL-to-Snowflake architecture; synthetic medium dataset and method; measured results; business report; limitations and future work.

Optional business-report SQL (read-only, but resumes compute if suspended):

```sql
SELECT tenant_id,
       SUM(total_appointments) AS appointments,
       SUM(cancelled_appointments) AS cancellations,
       SUM(no_show_appointments) AS no_shows,
       ROUND(100.0 * SUM(no_show_appointments)
             / NULLIF(SUM(total_appointments), 0), 2) AS no_show_pct
FROM QSS_SNOWFLAKE_LAB.ANALYTICS.DAILY_TENANT_METRICS_FULL
GROUP BY tenant_id
ORDER BY appointments DESC
LIMIT 10;
```

If you run this report, suspend QSS_LAB_WH afterward.

## Panel claim

“On a synthetic one-million-appointment workload, I measured how query projection, date ordering, and aggregate refresh affect Snowflake performance. Date ordering reduced scanned bytes substantially but did not improve warm latency at this scale. Incremental refresh matched full recomputation exactly but was slower. The POC shows which optimizations are justified by measurements, rather than assuming every optimization helps.”

## Measurement limitations

Some projection runs have large elapsed-time outliers. Report medians and ranges, and distinguish client elapsed time from server execution time. No p95 is supported by these small per-configuration samples. Compute usage is blank, so cost savings are unproven. The harness appends runs and groups summaries without dataset/warehouse keys: keep this result set restricted to the verified M/X-Small configuration; use a separate output directory for any future configuration. Repeatable validation for the join variants is not recorded by this harness.
