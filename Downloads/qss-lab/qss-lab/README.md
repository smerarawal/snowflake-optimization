# Snowflake vs MySQL Benchmark: Synthetic Appointment/Queue Platform

All data here is **synthetic**, modeled on the architecture of a multi-tenant appointment and
queue-management platform. It is not client data and must not be described as such.

## Scope decision
- **S (100k appointments):** correctness validation only.
- **M (1M appointments):** all reported experiments.
- **L (10M):** out of scope (time/resource constraint). State this in the report.

## Quick start
```bash
pip install numpy pandas
python generate.py --scale S --out out && python validate.py out/S     # ~20 s
python generate.py --scale M --out out && python validate.py out/M     # a few minutes
```
Same `--seed` => byte-identical files (verified for S). Use the same files for MySQL and Snowflake.

### MySQL
```bash
mysql -u root -p < mysql/01_schema.sql
MYSQL_PWD=... python mysql/load_mysql.py out/S     # validate first, then out/M
```
(Needs `local_infile=1`; loader uses /dev/stdin so Linux/macOS/WSL.)

### Snowflake
Run `snowflake/01_setup.sql`, `02_raw_schema.sql`, then `03_load.sql` in SnowSQL (set the path).
Keep auto-suspend on and suspend the warehouse after every session.

## Tunable parameters (generate.py)
`--tenants` (100), `--skew` Zipf exponent (0.9), `--audit-per-appt` (3.0),
`--msgs-extra` (0.3), `--seed` (42), `--chunk`, `--appointments` for custom sizes.

## What the S dataset looks like (measured, seed 42)
| Table | Rows |
|---|---|
| appointments | 100,000 |
| queue_events | 229,554 |
| notifications | 146,456 |
| audit_events | 300,064 |

Status mix 82.1% completed / 10.6% cancelled / 7.3% no-show; top tenant 15.3% of volume,
top 10 tenants 50%; ~1.9% of appointment updates arrive >1 day late (for incremental-load tests).
Queue events are lower-bounded by the 60% of tenants with queue enabled, so the queue count is
higher than the 2:1 ratio in the plan (it is 4 events per completed appointment); report the actual M counts from `manifest.json`.

## Synthetic assumptions to disclose
Tenant count/skew, hourly and weekday demand profile, cancellation/no-show probabilities (depend on
service category, lead time and hour), wait/service-time distributions, notification failure rates,
and the late-arrival rate are all invented parameters, not measurements.

## Next steps
1. Generate and load S into both systems; confirm equal row counts and identical results for the same aggregate SQL.
2. Generate and load M; record hardware/config of MySQL and the Snowflake warehouse size.
3. Build `benchmark/run_experiments.py` (Experiments 1-6 from the plan), saving raw runs to CSV.
