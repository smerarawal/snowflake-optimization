# Snowflake Optimization POC: Synthetic Appointment Platform

This medium-only POC benchmarks query design, date ordering, and aggregate refresh on a synthetic multi-tenant appointment platform inspired by the QSS architecture. It is not client data.

## Current status

Completed on an X-Small Snowflake warehouse using 1 million appointments:
- Query filtering, projection, and join formulation benchmarks.
- Unsorted versus date-sorted clustered table benchmarks.
- Full versus incremental affected-key aggregate refresh, with zero output differences.

See [POC_STATUS.md](POC_STATUS.md) for measured findings, limitations, and the finish plan. Saved evidence is in `results/`. The benchmark is ready as a scoped Snowflake POC. MySQL comparison, production contention reduction, live CDC, concurrency testing, and cost savings remain unproven. A business report and presentation are optional finishing deliverables.

## Dataset

M, seed 42: 100 tenants, 600 services, 1,000,000 appointments, 2,299,982 queue events, 1,464,037 notifications, and 3,000,017 audit events. Generated data is excluded from Git; the manifest is saved in `results/dataset_M_manifest.json`.

```powershell
pip install numpy pandas snowflake-connector-python
python generate.py --scale M --out out
python validate.py out/M
```

## Load and benchmark

Run `snowflake/01_setup.sql`, `snowflake/02_raw_schema.sql`, then `snowflake/03_load_M.sql` using SnowSQL. Update the local PUT paths if needed. These setup scripts create or replace lab objects; use a dedicated lab database. Keep auto-suspend enabled.

Set SNOWFLAKE_PASSWORD through your local environment; never commit credentials. SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER can override the harness defaults.

```powershell
# Inspect SQL without connecting
python benchmark/run_experiments.py --exp 2 3 4 --dry-run

# Optional fresh quick run; existing saved evidence already covers these tests
python benchmark/run_experiments.py --exp 2 3 4 --runs 2 --cold-runs 0 --outdir results/quick_poc
```

Result caching is disabled. Warm warehouse caching may still apply. Client timing includes fetching, and cold runs include resume overhead. The harness suspends the warehouse on exit. Experiment 4 uses the warm run count and performs no cold measurements.

The saved reference set uses 10 warm and 3 cold runs per query configuration for experiments 2 and 3, and 10 warm runs per refresh configuration for experiment 4. Use separate output directories for future runs because raw files append and summary grouping does not separate datasets or warehouse sizes. `benchmark/collect_metrics.sql` provides optional follow-up metrics; compute-cost evidence is not currently recorded.

## Scope and assumptions

Only M is needed for this POC; L is out of scope. Tenant skew, demand patterns, cancellation/no-show rates, queue timings, notification failures, and late arrivals are synthetic assumptions. No p95 claims are made from the small measurement samples.

The MySQL schema and loader are retained for future comparison. The loader's /dev/stdin dependency requires Linux/macOS/WSL; MySQL performance is not part of the completed evidence.
