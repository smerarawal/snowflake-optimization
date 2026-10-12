#!/usr/bin/env python3
"""Seeded synthetic data generator for the QSS-inspired appointment/queue benchmark.

All data is synthetic. It models an appointment + queue-management platform
(multi-tenant) and is NOT client data.

Usage:
    python generate.py --scale S --out out
    python generate.py --scale M --out out
    python generate.py --appointments 500000 --tenants 150 --out out --name custom

Output: out/<scale>/<table>/part_0001.csv.gz ... plus out/<scale>/manifest.json
Empty CSV field = NULL.
"""
import argparse
import json
import os
import time

import numpy as np
import pandas as pd

SCALES = {"S": 100_000, "M": 1_000_000, "L": 10_000_000}

# Column order of every CSV (also used by the loaders)
COLUMNS = {
    "dim_tenant": ["tenant_id", "tenant_name", "region", "timezone", "queue_enabled",
                   "staff_capacity", "created_at"],
    "dim_service": ["service_id", "tenant_id", "category", "avg_duration_min"],
    "appointments": ["appointment_id", "tenant_id", "service_id", "customer_id", "status",
                     "created_at", "scheduled_at", "checked_in_at", "completed_at",
                     "cancelled_at", "updated_at", "ingested_at"],
    "queue_events": ["event_id", "appointment_id", "tenant_id", "event_type", "event_ts",
                     "counter_id", "ingested_at"],
    "notifications": ["notification_id", "appointment_id", "tenant_id", "message_type",
                      "channel", "attempt_no", "retry_count", "delivery_status",
                      "created_at", "ingested_at"],
    "audit_events": ["audit_id", "tenant_id", "entity_type", "entity_id", "event_type",
                     "actor_type", "event_ts", "ingested_at"],
}

CATEGORIES = ["CONSULTATION", "BILLING", "DOCUMENTATION", "FOLLOW_UP", "DIAGNOSTICS", "SUPPORT"]
NC = len(CATEGORIES)
AVG_DURATION = np.array([20, 10, 15, 25, 30, 12])
CANCEL_BASE = np.array([0.08, 0.12, 0.10, 0.07, 0.06, 0.15])
NOSHOW_BASE = np.array([0.06, 0.04, 0.05, 0.10, 0.07, 0.05])

START = np.datetime64("2024-10-01", "s")
NDAYS = 730  # 24 months
HOURS = np.arange(8, 18)
HOUR_W = np.array([3, 6, 9, 9, 6, 5, 8, 9, 7, 4], dtype=float)
HOUR_W /= HOUR_W.sum()
WEEKDAY_W = np.array([1.0, 1.05, 1.05, 1.0, 0.95, 0.35, 0.10])  # Mon..Sun
NAT = np.datetime64("NaT", "s")

AUDIT_EVENTS = [("APPOINTMENT", "CREATED"), ("APPOINTMENT", "UPDATED"),
                ("APPOINTMENT", "STATUS_CHANGED"), ("QUEUE", "TICKET_ISSUED"),
                ("CUSTOMER", "PROFILE_UPDATED"), ("NOTIFICATION", "SENT"),
                ("SETTINGS", "CONFIG_CHANGED"), ("APPOINTMENT", "RESCHEDULED")]
AUDIT_W = np.array([0.22, 0.18, 0.22, 0.10, 0.08, 0.12, 0.03, 0.05])
AUDIT_W /= AUDIT_W.sum()
ACTORS = ["CUSTOMER", "STAFF", "SYSTEM"]
ACTOR_W = [0.35, 0.40, 0.25]


def tdelta(seconds):
    return np.asarray(seconds).astype("int64").astype("timedelta64[s]")


def write_part(df, root, table, part):
    d = os.path.join(root, table)
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, f"part_{part:04d}.csv.gz")
    df[COLUMNS[table]].to_csv(path, index=False, compression="gzip",
                              date_format="%Y-%m-%d %H:%M:%S")


def build_dimensions(rng, n_tenants, n_total, skew):
    ranks = np.arange(1, n_tenants + 1)
    w = 1.0 / ranks ** skew
    w /= w.sum()
    regions = np.array(["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"])
    tz = {"NORTH": "Asia/Kolkata", "SOUTH": "Asia/Kolkata", "EAST": "Asia/Kolkata",
          "WEST": "Asia/Kolkata", "CENTRAL": "Asia/Kolkata"}
    reg = rng.choice(regions, n_tenants)
    queue_enabled = rng.random(n_tenants) < 0.6
    staff = np.clip((2 + 18 * w / w.max()).round().astype(int), 2, 20)
    tenants = pd.DataFrame({
        "tenant_id": [f"T{i:04d}" for i in range(1, n_tenants + 1)],
        "tenant_name": [f"Synthetic Org {i:04d}" for i in range(1, n_tenants + 1)],
        "region": reg,
        "timezone": [tz[r] for r in reg],
        "queue_enabled": queue_enabled.astype(int),
        "staff_capacity": staff,
        "created_at": (START - tdelta(rng.integers(30, 900, n_tenants) * 86400)),
    })
    services = pd.DataFrame({
        "service_id": np.arange(n_tenants * NC) + 1,
        "tenant_id": np.repeat(tenants.tenant_id.values, NC),
        "category": np.tile(CATEGORIES, n_tenants),
        "avg_duration_min": np.tile(AVG_DURATION, n_tenants),
    })
    cat_w = rng.dirichlet(np.array([4, 3, 2, 3, 2, 2]), size=n_tenants)
    cat_cum = np.cumsum(cat_w, axis=1)
    congestion = rng.lognormal(0, 0.25, n_tenants) * (1 + 1.5 * w / w.max())
    pool = np.maximum(300, (n_total * w * 0.25).astype(int))
    cust_off = np.concatenate([[0], np.cumsum(pool)[:-1]])
    return dict(w=w, tenants=tenants, services=services, cat_cum=cat_cum,
                congestion=congestion, pool=pool, cust_off=cust_off,
                queue_enabled=queue_enabled, staff=staff)


def day_weights():
    days = (START.astype("datetime64[D]") + np.arange(NDAYS)).astype("int64")
    weekday = (days + 3) % 7  # 1970-01-01 was a Thursday
    trend = np.linspace(0.6, 1.4, NDAYS)
    dw = WEEKDAY_W[weekday] * trend
    return dw / dw.sum()


def gen_chunk(rng, n, appt_start, dim, dw, args):
    N = len(dim["w"])
    t = rng.choice(N, size=n, p=dim["w"])
    day = rng.choice(NDAYS, size=n, p=dw)
    hour = rng.choice(HOURS, size=n, p=HOUR_W)
    minute = rng.integers(0, 12, n) * 5
    sched = START + day.astype("timedelta64[D]") + tdelta(hour * 3600 + minute * 60)
    lead_h = np.clip(rng.lognormal(3.5, 1.4, n), 1, 24 * 60)
    created = sched - tdelta(lead_h * 3600)

    u = rng.random(n)
    cat = (u[:, None] > dim["cat_cum"][t]).sum(axis=1).clip(max=NC - 1)
    service_id = t * NC + cat + 1
    customer = dim["cust_off"][t] + rng.integers(0, 1 << 30, n) % dim["pool"][t] + 1

    p_c = CANCEL_BASE[cat] + 0.10 * (lead_h > 168)
    p_n = NOSHOW_BASE[cat] + 0.08 * (lead_h > 336) + 0.04 * (hour >= 16)
    u2 = rng.random(n)
    canc = u2 < p_c
    nosh = (~canc) & (u2 < p_c + p_n)
    comp = ~(canc | nosh)
    status = np.where(canc, "CANCELLED", np.where(nosh, "NO_SHOW", "COMPLETED"))

    cancelled_at = np.where(canc, created + tdelta(lead_h * 3600 * rng.uniform(0.05, 0.95, n)), NAT)
    checkin_t = sched + tdelta(rng.normal(-5, 6, n) * 60)
    peak = np.isin(hour, [10, 11, 14, 15])
    wait_min = rng.lognormal(np.log(10), 0.6, n) * dim["congestion"][t] * (1 + 0.3 * peak)
    called_t = checkin_t + tdelta(wait_min * 60)
    start_t = called_t + tdelta(rng.uniform(0, 3, n) * 60)
    dur_min = rng.lognormal(np.log(AVG_DURATION[cat]), 0.4)
    done_t = start_t + tdelta(dur_min * 60)

    checked_in = np.where(comp, checkin_t, NAT)
    completed = np.where(comp, done_t, NAT)
    noshow_t = sched + tdelta(30 * 60)
    updated = np.where(comp, done_t, np.where(canc, cancelled_at, noshow_t))
    late = rng.random(n) < 0.02
    delay = np.where(late, rng.integers(1, 6, n) * 86400, rng.integers(10, 600, n))
    ingested = updated + tdelta(delay)

    appt_id = appt_start + np.arange(n) + 1
    appts = pd.DataFrame({
        "appointment_id": appt_id, "tenant_id": [f"T{i + 1:04d}" for i in t],
        "service_id": service_id, "customer_id": customer, "status": status,
        "created_at": created, "scheduled_at": sched, "checked_in_at": checked_in,
        "completed_at": completed, "cancelled_at": cancelled_at, "updated_at": updated,
        "ingested_at": ingested})
    tid = appts["tenant_id"].values

    # ---- queue events (queue-enabled tenants only)
    qe = dim["queue_enabled"][t]
    parts = []

    def qparts(mask, etype, ts, offset_name=None):
        m = mask & qe
        if m.any():
            parts.append(pd.DataFrame({
                "appointment_id": appt_id[m], "tenant_id": tid[m], "event_type": etype,
                "event_ts": ts[m], "t": t[m], "ingested_at": ingested[m]}))
    qparts(comp, "CHECKED_IN", checkin_t)
    qparts(comp, "CALLED", called_t)
    qparts(comp, "SERVICE_STARTED", start_t)
    qparts(comp, "COMPLETED", done_t)
    qparts(nosh, "CALLED", sched + tdelta(10 * 60))
    qparts(nosh, "NO_SHOW", sched + tdelta(15 * 60))
    q = pd.concat(parts, ignore_index=True)
    q["counter_id"] = (q["t"].values * 20 + rng.integers(0, 1 << 20, len(q)) % dim["staff"][q["t"].values] + 1)
    q = q.drop(columns="t")

    # ---- audit events
    k = rng.poisson(args.audit_per_appt - 1, n) + 1
    idx = np.repeat(np.arange(n), k)
    span = (updated - created).astype("int64")[idx]
    a_ts = created[idx] + tdelta(span * rng.random(len(idx)))
    ev = rng.choice(len(AUDIT_EVENTS), size=len(idx), p=AUDIT_W)
    audit = pd.DataFrame({
        "tenant_id": tid[idx],
        "entity_type": np.array([AUDIT_EVENTS[i][0] for i in range(len(AUDIT_EVENTS))])[ev],
        "entity_id": appt_id[idx],
        "event_type": np.array([AUDIT_EVENTS[i][1] for i in range(len(AUDIT_EVENTS))])[ev],
        "actor_type": rng.choice(ACTORS, size=len(idx), p=ACTOR_W),
        "event_ts": a_ts,
        "ingested_at": a_ts + tdelta(rng.integers(10, 600, len(idx)))})

    # ---- notifications: messages per appointment, then delivery attempts
    m_per = 1 + (rng.random(n) < args.msgs_extra)
    midx = np.repeat(np.arange(n), m_per)
    nm = len(midx)
    mtype = np.where(np.arange(nm) == 0, "CONFIRMATION", "REMINDER")
    first = np.concatenate([[True], midx[1:] != midx[:-1]])
    mtype = np.where(first, "CONFIRMATION", "REMINDER")
    chan = rng.choice(["EMAIL", "SMS", "PUSH"], size=nm, p=[0.45, 0.40, 0.15])
    pf = np.where(chan == "SMS", 0.12, 0.06)
    f1 = rng.random(nm) < pf
    f2 = f1 & (rng.random(nm) < 0.5)
    attempts = 1 + f1.astype(int) + f2.astype(int)
    all_fail = f2 & (rng.random(nm) < 0.5)
    ridx = np.repeat(np.arange(nm), attempts)
    starts = np.cumsum(attempts) - attempts
    attempt_no = np.arange(len(ridx)) - np.repeat(starts, attempts) + 1
    last = attempt_no == attempts[ridx]
    status_n = np.where(last & ~all_fail[ridx], "DELIVERED", "FAILED")
    base_ts = np.where(mtype[ridx] == "CONFIRMATION", created[midx[ridx]] + tdelta(60),
                       sched[midx[ridx]] - tdelta(86400))
    n_ts = base_ts + tdelta((attempt_no - 1) * 300)
    notif = pd.DataFrame({
        "appointment_id": appt_id[midx[ridx]], "tenant_id": tid[midx[ridx]],
        "message_type": mtype[ridx], "channel": chan[ridx], "attempt_no": attempt_no,
        "retry_count": attempt_no - 1, "delivery_status": status_n, "created_at": n_ts,
        "ingested_at": n_ts + tdelta(rng.integers(10, 600, len(ridx)))})
    return appts, q, audit, notif


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", choices=SCALES.keys())
    ap.add_argument("--appointments", type=int)
    ap.add_argument("--tenants", type=int, default=100)
    ap.add_argument("--skew", type=float, default=0.9, help="Zipf exponent for tenant size")
    ap.add_argument("--audit-per-appt", type=float, default=3.0)
    ap.add_argument("--msgs-extra", type=float, default=0.3,
                    help="probability of a second message (reminder) per appointment")
    ap.add_argument("--chunk", type=int, default=250_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="out")
    ap.add_argument("--name", help="output folder name (default: scale)")
    args = ap.parse_args()
    total = args.appointments or SCALES.get(args.scale)
    if not total:
        ap.error("give --scale or --appointments")
    name = args.name or args.scale or f"custom_{total}"
    root = os.path.join(args.out, name)
    os.makedirs(root, exist_ok=True)

    t0 = time.time()
    rng0 = np.random.default_rng(args.seed)
    dim = build_dimensions(rng0, args.tenants, total, args.skew)
    dw = day_weights()
    write_part(dim["tenants"], root, "dim_tenant", 1)
    write_part(dim["services"], root, "dim_service", 1)
    counts = {"dim_tenant": len(dim["tenants"]), "dim_service": len(dim["services"]),
              "appointments": 0, "queue_events": 0, "notifications": 0, "audit_events": 0}
    off = {"queue_events": 0, "notifications": 0, "audit_events": 0}
    part = 0
    done = 0
    while done < total:
        n = min(args.chunk, total - done)
        part += 1
        rng = np.random.default_rng([args.seed, part])  # reproducible per chunk
        a, q, au, no = gen_chunk(rng, n, done, dim, dw, args)
        for tbl, df, idcol in (("queue_events", q, "event_id"),
                               ("notifications", no, "notification_id"),
                               ("audit_events", au, "audit_id")):
            df.insert(0, idcol, off[tbl] + np.arange(len(df)) + 1)
            off[tbl] += len(df)
        for tbl, df in (("appointments", a), ("queue_events", q),
                        ("notifications", no), ("audit_events", au)):
            write_part(df, root, tbl, part)
            counts[tbl] += len(df)
        done += n
        print(f"  chunk {part}: {done:,}/{total:,} appointments", flush=True)

    manifest = {"name": name, "seed": args.seed, "tenants": args.tenants, "skew": args.skew,
                "audit_per_appt": args.audit_per_appt, "msgs_extra": args.msgs_extra,
                "date_range": [str(START), str(START + np.timedelta64(NDAYS, "D"))],
                "row_counts": counts, "seconds": round(time.time() - t0, 1),
                "note": "Synthetic data modeled on an appointment/queue platform; not client data."}
    with open(os.path.join(root, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
