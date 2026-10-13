#!/usr/bin/env python3
"""Snowflake benchmark harness: Experiments 2 (pruning/query shape), 3 (clustering), 4 (aggregates).

Credentials come from environment variables only (never commit them):
  PowerShell:  $env:SNOWFLAKE_PASSWORD = "..."
  Optional:    $env:SNOWFLAKE_ACCOUNT (default BSDYRUL-ZO11495), $env:SNOWFLAKE_USER (default SMERARAWAL),
               $env:SNOWFLAKE_PASSCODE (only if your login requires an MFA passcode)

Usage:
  python benchmark/run_experiments.py --dry-run              # print every SQL, no connection
  python benchmark/run_experiments.py --exp 2 --runs 10      # one experiment
  python benchmark/run_experiments.py                        # experiments 2, 3, 4

Raw rows are appended to results/benchmark_raw.csv; summary is rebuilt in results/benchmark_summary.csv.
Result cache is OFF for every measured run. 'warm_wh' = warehouse local cache may be warm;
'cold_wh' = warehouse suspended first, so its local cache is dropped.
Unavailable metrics are left empty (never invented). compute_usage is empty: see collect_metrics.sql.
"""
import argparse
import csv
import datetime as dt
import json
import os
import statistics
import sys
import time

DB = "QSS_SNOWFLAKE_LAB"
RAW = f"{DB}.RAW"
ANA = f"{DB}.ANALYTICS"
FIELDS = ["experiment_id", "dataset_version", "platform", "query_id", "configuration", "warehouse_size",
          "run_number", "elapsed_ms", "server_elapsed_ms", "execution_ms", "bytes_scanned",
          "partitions_scanned", "partitions_total", "cache_condition", "rows_returned",
          "compute_usage", "sf_query_id", "run_at"]


def month_bounds(m):
    y, mo = map(int, m.split("-"))
    start = dt.date(y, mo, 1)
    end = dt.date(y + (mo == 12), mo % 12 + 1, 1)
    return start.isoformat(), end.isoformat()


def metrics_select(src, where=""):
    return (f"SELECT tenant_id, TO_DATE(scheduled_at) AS metric_date, COUNT(*) AS total_appointments, "
            f"COUNT_IF(status='COMPLETED') AS completed_appointments, "
            f"COUNT_IF(status='CANCELLED') AS cancelled_appointments, "
            f"COUNT_IF(status='NO_SHOW') AS no_show_appointments FROM {src} {where} GROUP BY 1, 2")


def exp2_plan(a):
    m0, m1 = month_bounds(a.month)
    f = f"created_at >= '{m0}' AND created_at < '{m1}'"
    A = f"{RAW}.APPOINTMENTS"
    return [
        ("Q1_daily_counts", "full_scan",
         f"SELECT tenant_id, TO_DATE(created_at) d, COUNT(*) c FROM {A} GROUP BY 1, 2"),
        ("Q1_daily_counts", "date_filtered",
         f"SELECT tenant_id, TO_DATE(created_at) d, COUNT(*) c FROM {A} WHERE {f} GROUP BY 1, 2"),
        ("Q2_projection", "select_star", f"SELECT * FROM {A} WHERE {f}"),
        ("Q2_projection", "needed_columns",
         f"SELECT appointment_id, tenant_id, status, created_at FROM {A} WHERE {f}"),
        ("Q3_join_shape", "join_then_aggregate",
         f"SELECT s.category, a.status, COUNT(*) c FROM {A} a JOIN {RAW}.DIM_SERVICE s "
         f"ON a.service_id = s.service_id WHERE a.{f} GROUP BY 1, 2"),
        ("Q3_join_shape", "aggregate_then_join",
         f"SELECT s.category, x.status, SUM(x.c) c FROM (SELECT service_id, status, COUNT(*) c FROM {A} "
         f"WHERE {f} GROUP BY 1, 2) x JOIN {RAW}.DIM_SERVICE s ON x.service_id = s.service_id GROUP BY 1, 2"),
    ]


def exp3_plan(a):
    m0, m1 = month_bounds(a.month)
    out = []
    for cfg, tbl in (("unclustered", f"{RAW}.APPOINTMENTS"), ("clustered_date", f"{RAW}.APPOINTMENTS_CLUSTERED")):
        out.append(("Q4_date_selective", cfg,
                    f"SELECT status, COUNT(*) c FROM {tbl} WHERE created_at >= '{m0}' AND created_at < '{m1}' GROUP BY 1"))
        out.append(("Q5_tenant_selective", cfg,
                    f"SELECT DATE_TRUNC('month', created_at) m, COUNT(*) c FROM {tbl} "
                    f"WHERE tenant_id = '{a.tenant}' GROUP BY 1"))
    return out


EXP3_SETUP = [
    f"CREATE OR REPLACE TABLE {RAW}.APPOINTMENTS_CLUSTERED CLUSTER BY (TO_DATE(created_at)) AS "
    f"SELECT * FROM {RAW}.APPOINTMENTS ORDER BY created_at",
    f"ALTER TABLE {RAW}.APPOINTMENTS_CLUSTERED SUSPEND RECLUSTER",
]


def exp4_sql(a):
    A = f"{RAW}.APPOINTMENTS"
    full = f"{ANA}.DAILY_TENANT_METRICS_FULL"
    inc = f"{ANA}.DAILY_TENANT_METRICS_INC"
    reset = f"CREATE OR REPLACE TABLE {inc} AS " + metrics_select(A, f"WHERE ingested_at < '{a.cutoff}'")
    merge = (
        f"MERGE INTO {inc} t USING (SELECT a.tenant_id, TO_DATE(a.scheduled_at) AS metric_date, COUNT(*) AS total_appointments, "
        f"COUNT_IF(a.status='COMPLETED') AS completed_appointments, COUNT_IF(a.status='CANCELLED') AS cancelled_appointments, "
        f"COUNT_IF(a.status='NO_SHOW') AS no_show_appointments FROM {A} a JOIN "
        f"(SELECT DISTINCT tenant_id, TO_DATE(scheduled_at) AS metric_date FROM {A} WHERE ingested_at >= '{a.cutoff}') k "
        f"ON a.tenant_id = k.tenant_id AND TO_DATE(a.scheduled_at) = k.metric_date GROUP BY 1, 2) s "
        f"ON t.tenant_id = s.tenant_id AND t.metric_date = s.metric_date "
        f"WHEN MATCHED THEN UPDATE SET total_appointments = s.total_appointments, completed_appointments = s.completed_appointments, "
        f"cancelled_appointments = s.cancelled_appointments, no_show_appointments = s.no_show_appointments "
        f"WHEN NOT MATCHED THEN INSERT (tenant_id, metric_date, total_appointments, completed_appointments, cancelled_appointments, no_show_appointments) "
        f"VALUES (s.tenant_id, s.metric_date, s.total_appointments, s.completed_appointments, s.cancelled_appointments, s.no_show_appointments)")
    fullsql = f"CREATE OR REPLACE TABLE {full} AS " + metrics_select(A)
    diff = (f"SELECT (SELECT COUNT(*) FROM (SELECT * FROM {full} MINUS SELECT * FROM {inc})) AS full_minus_inc, "
            f"(SELECT COUNT(*) FROM (SELECT * FROM {inc} MINUS SELECT * FROM {full})) AS inc_minus_full, "
            f"(SELECT COUNT(*) FROM {full}) AS full_rows, (SELECT COUNT(*) FROM {inc}) AS inc_rows")
    delta = (f"SELECT COUNT(*) AS delta_rows, COUNT(DISTINCT tenant_id || TO_VARCHAR(TO_DATE(scheduled_at))) AS affected_keys, "
             f"(SELECT COUNT(*) FROM {A}) AS total_rows FROM {A} WHERE ingested_at >= '{a.cutoff}'")
    return reset, merge, fullsql, diff, delta


class Bench:
    def __init__(self, a):
        self.a = a
        self.rows = []
        self.size = ""
        os.makedirs(a.outdir, exist_ok=True)
        self.raw_path = os.path.join(a.outdir, "benchmark_raw.csv")
        self.dataset = "unknown"
        if os.path.exists(a.manifest):
            m = json.load(open(a.manifest))
            self.dataset = f"{m['name']}_seed{m['seed']}_{m['row_counts']['appointments']}appts"

    def connect(self):
        import snowflake.connector as sc
        pwd = os.environ.get("SNOWFLAKE_PASSWORD")
        if not pwd:
            sys.exit("Set SNOWFLAKE_PASSWORD first (PowerShell: $env:SNOWFLAKE_PASSWORD = \"...\")")
        kw = dict(account=os.environ.get("SNOWFLAKE_ACCOUNT", "BSDYRUL-ZO11495"),
                  user=os.environ.get("SNOWFLAKE_USER", "SMERARAWAL"), password=pwd,
                  warehouse=self.a.warehouse, database=DB, schema="RAW")
        if os.environ.get("SNOWFLAKE_PASSCODE"):
            kw["passcode"] = os.environ["SNOWFLAKE_PASSCODE"]
        self.conn = sc.connect(**kw)
        self.cur = self.conn.cursor()
        self.cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
        self.cur.execute("ALTER SESSION SET QUERY_TAG = 'qss_benchmark'")
        d = self.conn.cursor(sc.DictCursor)
        d.execute(f"SHOW WAREHOUSES LIKE '{self.a.warehouse}'")
        r = d.fetchall()
        self.size = r[0]["size"] if r else ""
        print(f"connected; warehouse {self.a.warehouse} size={self.size}; dataset={self.dataset}")

    def run(self, sql, fetch=True):
        t0 = time.perf_counter()
        self.cur.execute(sql)
        rows = self.cur.fetchall() if fetch else []
        ms = (time.perf_counter() - t0) * 1000
        return self.cur.sfqid, ms, rows

    def server_metrics(self, qid):
        """Returns (total_elapsed_ms, execution_ms, bytes_scanned, partitions_scanned, partitions_total, rows_produced).
        Timing/bytes come from INFORMATION_SCHEMA.QUERY_HISTORY; partition counts come from the largest
        TableScan operator in GET_QUERY_OPERATOR_STATS (the Query Profile data). Missing values stay None."""
        import re
        if not re.fullmatch(r"[0-9a-fA-F-]{36}", str(qid)):
            return (None,) * 6
        q = ("SELECT total_elapsed_time, execution_time, bytes_scanned, rows_produced "
             "FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 300)) "
             f"WHERE query_id = '{qid}'")
        base = (None,) * 4
        for _ in range(8):
            self.cur.execute(q)
            r = self.cur.fetchall()
            if r:
                base = r[0]
                break
            time.sleep(2)
        ps = pt = None
        try:
            self.cur.execute(
                "SELECT operator_statistics:pruning:partitions_scanned::NUMBER, "
                "operator_statistics:pruning:partitions_total::NUMBER "
                f"FROM TABLE(GET_QUERY_OPERATOR_STATS('{qid}')) "
                "WHERE operator_type = 'TableScan' ORDER BY 2 DESC NULLS LAST LIMIT 1")
            r = self.cur.fetchall()
            if r:
                ps, pt = r[0]
        except Exception as e:
            if not getattr(self, "_warned", False):
                print("  note: partition stats unavailable for some queries:", str(e)[:120])
                self._warned = True
        return (base[0], base[1], base[2], ps, pt, base[3])

    def record(self, exp, name, cfg, run_no, cache, qid, ms, rows_ret):
        m = self.server_metrics(qid)
        row = dict(experiment_id=exp, dataset_version=self.dataset, platform="snowflake", query_id=name,
                   configuration=cfg, warehouse_size=self.size, run_number=run_no, elapsed_ms=round(ms, 1),
                   server_elapsed_ms=m[0], execution_ms=m[1], bytes_scanned=m[2], partitions_scanned=m[3],
                   partitions_total=m[4], cache_condition=cache, rows_returned=rows_ret, compute_usage="",
                   sf_query_id=qid, run_at=dt.datetime.now().isoformat(timespec="seconds"))
        self.rows.append(row)
        new = not os.path.exists(self.raw_path)
        with open(self.raw_path, "a", newline="") as f:
            w = csv.DictWriter(f, FIELDS)
            if new:
                w.writeheader()
            w.writerow(row)
        print(f"  {exp} {name}/{cfg} run{run_no} [{cache}] {ms:,.0f} ms  scanned {m[3]}/{m[4]} partitions")

    def cold(self):
        try:
            self.cur.execute(f"ALTER WAREHOUSE {self.a.warehouse} SUSPEND")
        except Exception:
            pass  # already suspended
        time.sleep(2)

    def measure(self, exp, name, cfg, sql):
        a = self.a
        self.run(sql)  # warm-up, not recorded
        for i in range(1, a.runs + 1):
            qid, ms, rows = self.run(sql)
            self.record(exp, name, cfg, i, "result_cache_off|warm_wh", qid, ms, len(rows))
        for i in range(1, a.cold_runs + 1):
            self.cold()
            qid, ms, rows = self.run(sql)
            self.record(exp, name, cfg, i, "result_cache_off|cold_wh", qid, ms, len(rows))

    def exp2(self):
        for name, cfg, sql in exp2_plan(self.a):
            self.measure("exp2", name, cfg, sql)

    def exp3(self):
        info = {}
        for i, sql in enumerate(EXP3_SETUP):
            try:
                qid, ms, _ = self.run(sql)
                if i == 0:
                    self.record("exp3", "clustered_table_build", "clustered_date", 1, "setup", qid, ms, 0)
            except Exception as e:
                print("  setup note:", e)
        for t in ("APPOINTMENTS", "APPOINTMENTS_CLUSTERED"):
            _, _, r = self.run(f"SELECT SYSTEM$CLUSTERING_INFORMATION('{RAW}.{t}', '(TO_DATE(created_at))')")
            info[t] = json.loads(r[0][0])
        json.dump(info, open(os.path.join(self.a.outdir, "exp3_clustering_info.json"), "w"), indent=2)
        for name, cfg, sql in exp3_plan(self.a):
            self.measure("exp3", name, cfg, sql)
        print("  NOTE: also check AUTOMATIC_CLUSTERING_HISTORY later for maintenance credits (see collect_metrics.sql).")

    def exp4(self):
        reset, merge, fullsql, diff, delta = exp4_sql(self.a)
        _, _, r = self.run(delta)
        stats = dict(zip(["delta_rows", "affected_keys", "total_rows"], map(int, r[0])))
        stats["cutoff"] = self.a.cutoff
        print("  delta:", stats)
        for i in range(1, self.a.runs + 1):
            qid, ms, _ = self.run(fullsql)
            self.record("exp4", "daily_metrics_refresh", "full_recompute", i, "result_cache_off|warm_wh", qid, ms, 0)
        for i in range(1, self.a.runs + 1):
            self.run(reset)  # untimed: restore pre-delta state
            qid, ms, _ = self.run(merge)
            self.record("exp4", "daily_metrics_refresh", "incremental_merge", i, "result_cache_off|warm_wh", qid, ms, 0)
        _, _, r = self.run(diff)
        stats.update(dict(zip(["full_minus_inc", "inc_minus_full", "full_rows", "inc_rows"], map(int, r[0]))))
        stats["correct"] = stats["full_minus_inc"] == 0 and stats["inc_minus_full"] == 0
        json.dump(stats, open(os.path.join(self.a.outdir, "exp4_correctness.json"), "w"), indent=2)
        print("  correctness:", stats)

    def summarize(self):
        import collections
        rows = list(csv.DictReader(open(self.raw_path)))
        g = collections.defaultdict(list)
        for r in rows:
            if r["cache_condition"].startswith("result_cache_off") and r["elapsed_ms"]:
                g[(r["experiment_id"], r["query_id"], r["configuration"], r["cache_condition"])].append(r)
        out = []
        for k, v in g.items():
            e = sorted(float(x["elapsed_ms"]) for x in v)
            p95 = e[min(len(e) - 1, int(round(0.95 * len(e) + 0.5)) - 1)] if len(e) >= 20 else ""
            def med(col):
                xs = [float(x[col]) for x in v if x[col] not in ("", "None")]
                return statistics.median(xs) if xs else ""
            out.append(dict(experiment_id=k[0], query_id=k[1], configuration=k[2], cache_condition=k[3], n=len(e),
                            median_ms=round(statistics.median(e), 1), min_ms=e[0], max_ms=e[-1],
                            stdev_ms=round(statistics.stdev(e), 1) if len(e) > 1 else "", p95_ms=p95,
                            median_bytes_scanned=med("bytes_scanned"),
                            median_partitions_scanned=med("partitions_scanned"),
                            median_partitions_total=med("partitions_total")))
        base = {}
        for r in out:  # first config seen per (exp, query, cache) is the baseline
            base.setdefault((r["experiment_id"], r["query_id"], r["cache_condition"]), r["median_ms"])
        for r in out:
            b = base[(r["experiment_id"], r["query_id"], r["cache_condition"])]
            r["speedup_vs_first_config"] = round(b / r["median_ms"], 2) if r["median_ms"] else ""
        path = os.path.join(self.a.outdir, "benchmark_summary.csv")
        if out:
            with open(path, "w", newline="") as f:
                w = csv.DictWriter(f, list(out[0].keys()))
                w.writeheader()
                w.writerows(out)
        for r in sorted(out, key=lambda r: (r["experiment_id"], r["query_id"], r["cache_condition"])):
            print(f"{r['experiment_id']} {r['query_id']:<22}{r['configuration']:<20}{r['cache_condition'][17:]:<8} "
                  f"n={r['n']:<3} median={r['median_ms']:>9} ms  parts={r['median_partitions_scanned']}/{r['median_partitions_total']}"
                  f"  x{r['speedup_vs_first_config']}")
        print(f"\nwrote {self.raw_path} and {path}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--exp", type=int, nargs="+", default=[2, 3, 4], choices=[2, 3, 4])
    p.add_argument("--runs", type=int, default=10, help="measured warm-warehouse runs per config")
    p.add_argument("--cold-runs", type=int, default=3, help="runs after suspending warehouse (each resume bills >= 60 s)")
    p.add_argument("--warehouse", default="QSS_LAB_WH")
    p.add_argument("--month", default="2026-01", help="YYYY-MM window for date-filtered queries")
    p.add_argument("--tenant", default="T0050", help="tenant for tenant-selective query")
    p.add_argument("--cutoff", default="2026-09-01", help="ingested_at watermark for incremental test")
    p.add_argument("--manifest", default="out/M/manifest.json")
    p.add_argument("--outdir", default="results")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()

    if a.dry_run:
        for name, cfg, sql in (exp2_plan(a) if 2 in a.exp else []) + (exp3_plan(a) if 3 in a.exp else []):
            print(f"-- {name} / {cfg}\n{sql};\n")
        if 3 in a.exp:
            print("-- exp3 setup\n" + ";\n".join(EXP3_SETUP) + ";\n")
        if 4 in a.exp:
            for label, s in zip(["reset", "merge", "full", "diff", "delta"], exp4_sql(a)):
                print(f"-- exp4 {label}\n{s};\n")
        return

    b = Bench(a)
    b.connect()
    try:
        for e in a.exp:
            print(f"== experiment {e}")
            getattr(b, f"exp{e}")()
        b.summarize()
    finally:
        try:
            b.cur.execute(f"ALTER WAREHOUSE {a.warehouse} SUSPEND")
            print("warehouse suspended")
        except Exception:
            pass
        b.conn.close()


if __name__ == "__main__":
    main()
