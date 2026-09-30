# Runbook

Operational notes for the transit lakehouse: what runs, what "healthy" means,
what to do when it isn't, and what has already gone wrong.

---

## What runs, and when

| job | schedule | tasks |
|---|---|---|
| `rt_collectors` | every 15 min | vehicle positions, alerts — independent tasks, no dependency between them |
| `transit_pipeline` | daily 04:00 UTC | 8 tasks, bronze -> silver -> gold -> quality |

```
ingest_static -> silver_dimensions -> silver_stop_times -> gold_dimensions -+
                                                                            +-> gold_fact -> quality_checks
bronze_rt -> silver_vehicle_positions -------------------------------------+
```

The two branches are independent until `gold_fact`, which is why they run in
parallel: the realtime branch does not wait on a 30 MB schedule download it does
not need. Splitting them took the run from 8m43s to 6m00s.

`quality_checks` runs last so that an `error`-severity rule failure fails the
whole run rather than leaving bad data in gold looking finished.

**The collectors are deliberately a separate job.** A pipeline failure must never
stop history being collected: the realtime feed keeps no archive, so every missed
interval is gone permanently. The two collectors are also independent of each
other, so a failure fetching one feed cannot stop the other.

---

## Freshness targets

| signal | target | where it is checked |
|---|---|---|
| newest snapshot file | under 35 min | `00_health_check`, reads the landing zone |
| silver vehicle positions | under 24 h | `data_is_fresh` rule, warn severity |
| bronze static feed version | current publication | `00_health_check` |

35 minutes is two 15-minute intervals plus serverless start-up time. A 20-minute
threshold fired on a healthy collector; an alert that cries wolf gets ignored,
which is worse than no alert.

The health check reads the **landing zone**, not the bronze table. Bronze only
changes when the pipeline runs, so from bronze a dead collector and an idle
pipeline look identical. That distinction cost ~100 snapshots before it was made.

---

## When something fails

**A pipeline task failed.** Open the run and read the failing task's output. Most
failures are `NameError` from something that only existed in an interactive
session — a job starts a fresh Python process every time. Reproduce locally with
**Clear state -> Run all** on that notebook; if it passes that way it will pass in
the job.

**A quality rule failed the run.** `quality_checks` fails on any `error`-severity
rule. Read `ops.quality_results` for the newest `run_id`: `rows_failed` and
`failed_pct` say how bad it is, `params` says what the rule expected. Then decide
whether the data is wrong or the rule is wrong — both have happened. The
occupancy cap was the rule's fault (GTFS-RT allows values above 100); the
out-of-region coordinates were the data's.

**No new snapshots arriving.** Check the collector job's **Runs** tab before
debugging any code.

**Ingestion suddenly fails.** Databricks Free Edition outbound access can be
intermittent. Check network access before assuming a code problem.

**A module edit seems to have no effect.** Python caches imported modules. After
editing anything in `src/`, use **Clear state** before running. Editing `src/`
files in the browser editor has corrupted them more than once — edit locally and
push.

---

## Recovery procedures

All four have been used at least once.

**Rebuild bronze from the landing zone.** Landing files are never modified or
deleted, so bronze is always reproducible. Set `RUN_BACKFILL = True` in
`01_ingest_static_gtfs` and run it: every feed version in landing is replayed
with per-version `replaceWhere`, so the operation is idempotent. Used after a
migration deleted a feed version.

**Reprocess all realtime history.**

```sql
UPDATE transit.ops.watermarks SET watermark = 0
WHERE pipeline = 'silver_vehicle_positions'
```

then run `09_silver_vehicle_positions`. The merge is idempotent, so a full
reprocess of every snapshot inserts and updates nothing. To rebuild silver from
scratch, drop `transit.silver.vehicle_positions` and
`transit.silver.vehicle_carriages` first — a merge never deletes rows that are
already there.

**Repoint a job after renaming a notebook.** Job tasks reference notebooks by
path, and a rename breaks them silently. Jobs & Pipelines -> the job -> Tasks ->
update the path. Prefer not renaming notebooks that a job references.

**Re-examine quarantined rows.** `90_rejects_inspection` re-evaluates
`ops.rejects` against the current rules and reports which rows would now pass.
Rules change; rejects deserve a second look rather than being written off.

---

## Operational tables

| table | what it records |
|---|---|
| `ops.ingestion_log` | row counts per table per feed version per ingestion |
| `ops.watermarks` | how far each pipeline has read, plus last run's row counts |
| `ops.pipeline_runs` | one row per run: files read, rows read, quarantined, inserted, updated |
| `ops.quality_results` | one row per rule per run: checked, failed, pass/fail, duration |
| `ops.rejects` | quarantined rows with reasons and the full original row as JSON |

---

## Incidents

**20–21 Sep 2026: collector stopped for 25 hours.** Renaming the collector
notebook broke the scheduled job, which referenced it by path. Every run failed
and nothing alerted. About 100 snapshots were lost permanently — the feed keeps
no archive. Visible afterwards in bronze as 40 snapshots on the 20th instead of
96.

*Fixed:* repointed the task, added on-failure email notification, and moved the
health check to read the landing zone instead of the bronze table.

*Follow-up:* the first freshness threshold of 20 minutes produced a false alarm at
24 minutes on a healthy collector. Raised to 35.

**Migration deleted a feed version.** The one-time migration that added
`_feed_version` partitioning used plain `overwrite` against only the newest
landing directory, silently dropping the 2026-09-15 version from bronze — exactly
the loss `replaceWhere` exists to prevent. Recovered by replaying every version
from the landing zone. This is why raw files are kept unmodified.