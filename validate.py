#!/usr/bin/env python3
"""Validate a generated dataset folder: python validate.py out/S"""
import glob
import json
import os
import sys

import pandas as pd

root = sys.argv[1]


def load(t):
    files = sorted(glob.glob(os.path.join(root, t, "part_*.csv.gz")))
    return pd.concat((pd.read_csv(f) for f in files), ignore_index=True)


m = json.load(open(os.path.join(root, "manifest.json")))
T = {t: load(t) for t in m["row_counts"]}
ok = True


def check(name, cond, detail=""):
    global ok
    ok &= bool(cond)
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")


for t, n in m["row_counts"].items():
    check(f"row count {t}", len(T[t]) == n, f"({len(T[t]):,})")

pk = {"dim_tenant": "tenant_id", "dim_service": "service_id", "appointments": "appointment_id",
      "queue_events": "event_id", "notifications": "notification_id", "audit_events": "audit_id"}
for t, c in pk.items():
    check(f"unique PK {t}.{c}", T[t][c].is_unique)

a = T["appointments"]
tenants = set(T["dim_tenant"].tenant_id)
services = T["dim_service"].set_index("service_id").tenant_id
for t in ("appointments", "queue_events", "notifications", "audit_events"):
    check(f"no orphan tenant in {t}", T[t].tenant_id.isin(tenants).all())
check("service belongs to same tenant", (a.service_id.map(services) == a.tenant_id).all())
appt_ids = set(a.appointment_id)
check("queue_events -> appointments", T["queue_events"].appointment_id.isin(appt_ids).all())
check("notifications -> appointments", T["notifications"].appointment_id.isin(appt_ids).all())
check("audit entity_id -> appointments", T["audit_events"].entity_id.isin(appt_ids).all())

for c in ("created_at", "scheduled_at", "checked_in_at", "completed_at", "cancelled_at", "updated_at", "ingested_at"):
    a[c] = pd.to_datetime(a[c])
comp = a[a.status == "COMPLETED"]
check("created < scheduled", (a.created_at < a.scheduled_at).all())
check("completed has check-in/completed, others don't",
      comp.completed_at.notna().all() and a[a.status != "COMPLETED"].completed_at.isna().all())
check("check-in <= completed", (comp.checked_in_at <= comp.completed_at).all())
check("cancelled only for CANCELLED", (a.cancelled_at.notna() == (a.status == "CANCELLED")).all())
check("ingested_at >= updated_at", (a.ingested_at >= a.updated_at).all())

print("\nStatus mix:\n", a.status.value_counts(normalize=True).round(3).to_string())
share = a.tenant_id.value_counts(normalize=True)
print(f"\nTenant skew: top1={share.iloc[0]:.1%} top5={share.iloc[:5].sum():.1%} "
      f"top10={share.iloc[:10].sum():.1%} smallest={share.iloc[-1]:.2%}")
print("Late-arriving (>1 day ingest lag): %.2f%%" % (100 * ((a.ingested_at - a.updated_at).dt.days >= 1).mean()))
n = T["notifications"]
print("Notification status:", n.delivery_status.value_counts(normalize=True).round(3).to_dict())
print("\nALL CHECKS PASSED" if ok else "\nSOME CHECKS FAILED")
sys.exit(0 if ok else 1)
