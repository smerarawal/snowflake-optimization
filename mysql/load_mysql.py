#!/usr/bin/env python3
"""Load generated CSV.GZ parts into MySQL via LOAD DATA LOCAL INFILE (Linux/macOS).
Usage: python mysql/load_mysql.py out/M [--user root] [--host 127.0.0.1] [--db qss_lab]
Password: set MYSQL_PWD env var (never commit credentials).
Requires the mysql client and local_infile enabled on server (SET GLOBAL local_infile=1)."""
import argparse, glob, os, subprocess, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from generate import COLUMNS

ap = argparse.ArgumentParser()
ap.add_argument("root"); ap.add_argument("--user", default="root")
ap.add_argument("--host", default="127.0.0.1"); ap.add_argument("--db", default="qss_lab")
a = ap.parse_args()

ORDER = ["dim_tenant", "dim_service", "appointments", "queue_events", "notifications", "audit_events"]
for t in ORDER:
    cols = COLUMNS[t]
    vars_ = ",".join(f"@{c}" for c in cols)
    sets = ",".join(f"{c}=NULLIF(@{c},'')" for c in cols)
    sql = (f"SET unique_checks=0; LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE {t} "
           f"FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' "
           f"IGNORE 1 LINES ({vars_}) SET {sets};")
    for f in sorted(glob.glob(os.path.join(a.root, t, "part_*.csv.gz"))):
        print(f"loading {f}", flush=True)
        cmd = f"gzip -dc '{f}' | mysql --local-infile=1 -h {a.host} -u {a.user} {a.db} -e \"{sql}\""
        subprocess.run(cmd, shell=True, check=True)
print("done. Now run: ANALYZE TABLE for each table before benchmarking.")
