# MBTA Transit Lakehouse

A medallion-architecture lakehouse over Boston's public transit feeds, built on
Databricks with PySpark, Spark SQL and Delta Lake.

---

## The problem

The MBTA publishes two feeds that do not naturally talk to each other.

**GTFS Static** is the published schedule — flat CSVs in a zip covering stops,
routes, trips and a 3.2M-row timetable, republished every few weeks.

**GTFS-Realtime** is where every vehicle is right now — deeply nested JSON,
refreshed continuously, with **no history**. It reports the present moment and
keeps no archive.

Answering anything that spans both, such as how actual service compares to
scheduled service, means landing them reliably, reconciling the places where they
disagree, and modelling them into something queryable.

---

## What this builds

- **Bronze** — the source as it arrived. Every column string, no cleaning, with
  ingestion metadata on every row. Partitioned by feed version and written with
  `replaceWhere`, so re-running is safe and history across publications survives.
- **Silver** — typed, deduplicated, conformed. Failed rows quarantined with a
  reason rather than dropped. Realtime loaded incrementally via `MERGE` with
  watermarking.
- **Gold** — a star schema with integer surrogate keys, unknown members for
  unresolved foreign keys, and an explicitly stated grain on every fact table.
- **Ops** — the pipeline's own records: ingestion log, watermarks, quality
  results, rejects.

---

## Design decisions

**Bronze stores everything as string.** Typing is a decision that can fail, and a
failed cast in Spark produces a silent null rather than an error. Typing on
ingest means you can never distinguish "missing at source" from "we broke it".
Casting happens in silver, where failures are quarantined and visible.

**Bad rows are quarantined, not filtered.** Every silver transform splits rather
than filters — valid rows go forward, invalid rows go to `ops.rejects` with a
reason code, and each run asserts `valid + rejected = input`. Filtering hides
problems; quarantining surfaces them.

**Explicit schemas, never inference.** Spark infers a JSON schema by sampling.
If an optional field is absent from the sampled files, the column does not exist
— so the schema depends on which files happened to be present at read time, and
the same code produces different results on different days.

**Realtime history is collected, not simulated.** A scheduled collector has been
snapshotting the vehicle feed every 15 minutes since day one of the build. The
accumulated snapshots contain genuine duplicates, gaps and late-arriving records,
which is what makes the incremental-loading work real rather than a demonstration
against fabricated input. It was built first because it is the only part of the
project that cannot be caught up later.

Full reasoning in [`docs/design-decisions.md`](docs/design-decisions.md).

---

## Problems worth describing

**GTFS times legitimately exceed 24 hours.** A train departing 1:15am on a
service day that began the previous morning is published as `25:15:00`, because
the spec defines times relative to the service day rather than the calendar day.
Cast that to a timestamp naively and you get null — silently, with no error,
across the entire late-night network. The fix parses to seconds-since-service-
start as the canonical value and derives the timestamp by adding to the service
date.

**The two feeds disagree with each other.** Realtime vehicles report trip IDs
that no longer exist in the current static schedule, because the timetable was
republished mid-week. Unresolved foreign keys route to an unknown-member row
rather than being dropped, and the rate is tracked as a quality metric.

**Snapshot overlap creates duplicate observations.** Polling every 15 minutes
against vehicles that report less often means the same `(vehicle_id, timestamp)`
observation appears in multiple snapshots. A Delta `MERGE` errors when two source
rows match one target row, so the source is deduplicated before merging.

---

## Stack

Databricks (Free Edition, serverless) · PySpark · Spark SQL · Delta Lake ·
Unity Catalog · Databricks Workflows · pytest · GitHub Actions

---

## Layout

```
src/transforms/   pure transformation functions — (DataFrame, ...) -> DataFrame
src/quality/      rule evaluation
notebooks/        thin: read, call, write, validate
tests/            pytest against src/
jobs/             workflow definitions
docs/             architecture, design decisions, build notes
```

I/O lives at the edges; logic lives in the middle. Notebooks contain almost no
business logic, which is what makes the transformations unit-testable.

---

## Status

Work in progress.

**Built** — catalog and schema layout · scheduled realtime collector ·
static GTFS ingestion into six bronze tables · idempotent bronze writes

**Next** — realtime JSON into bronze with an explicit schema · silver
dimensions · service-day time parsing · incremental MERGE · star schema ·
quality rules engine and quarantine · pytest suite · orchestration

See [`docs/architecture.md`](docs/architecture.md) for the full design and
[`docs/notes.md`](docs/notes.md) for what the real data turned out to look like.
