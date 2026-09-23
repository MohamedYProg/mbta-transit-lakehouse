# Design decisions

A running log of choices made during the build and the reasoning behind them.
Entries are added as decisions are taken, not reconstructed afterwards.

---

## Catalog and schema layout

`transit` catalog with four schemas:

| schema | contents |
|---|---|
| `bronze` | raw as-landed, every source column string |
| `silver` | typed, deduplicated, conformed |
| `gold` | star schema — facts and conformed dimensions |
| `ops` | the pipeline's own records |

`ops` is deliberately not a data tier. It holds `ingestion_log`, `watermarks`,
`quality_results` and `rejects` — metadata about pipeline execution rather than
transit data. Keeping it in its own schema means a consumer browsing `gold`
never sees operational tables, and a quality dashboard has one obvious place to
read from.

---

## Bronze stores everything as string

`inferSchema=False` on every CSV read. `stop_lat` and `stop_lon` are obviously
numbers and they still land as text.

Typing is a decision that can fail, and a failed cast in Spark produces a silent
`null` rather than an error. If bronze types on ingest, a bad value becomes a
null and there is no longer any way to distinguish "missing at source" from "we
broke it during ingestion". The original bytes are gone.

Casting happens in silver instead, where a value that will not cast is routed to
`ops.rejects` with a reason code rather than quietly becoming null.

---

## CSV parsing: explicit quote and escape

Every GTFS read sets `.option("quote", '"').option("escape", '"')`.

MBTA's `feed_info.txt` contains `"Fall 2026, 2026-09-17T20:41:44+00:00, version D"`
— a quoted field containing commas. Without the escape option that field splits
across columns and every column after it shifts by one. The failure is silent and
affects only the rows that happen to contain a comma, which makes it hard to
notice and easy to ship.

Stop names have the same problem (`Harvard Square, Lower Busway`).

---

## GTFS blanks land as null, but only because they are unquoted

Checked rather than assumed: of 10,269 stops, `stop_desc` was null on 6,908 rows
and empty-string on zero.

Spark's CSV reader converts an *unquoted* empty field to null by default. A
*quoted* empty field (`a,"",b`) stays an empty string. MBTA writes unquoted
blanks, so this works out — but it is a property of this publisher's export, not
of GTFS or of Spark.

The distinction still matters downstream: trimming whitespace in silver turns
`"   "` into `""`, not null. So the empty-string-to-null conversion earns its
place in the silver transform, applied after trimming.

---

## Feed version: derived from `Last-Modified`, not from the feed

`_feed_version` comes from the HTTP `Last-Modified` header, formatted `YYYY-MM-DD`.

MBTA publishes its own version string in `feed_info.txt`, but it is free text
with commas and spaces (`Fall 2026, 2026-09-17T20:41:44+00:00, version D`) and
its format is entirely at the agency's discretion. It is unusable as a partition
key and unsafe to sort on.

The agency string is still captured as `_agency_feed_version` for traceability —
it is the identifier MBTA themselves would use if you asked them about a specific
publication.

---

## Bronze idempotency: `replaceWhere` on `_feed_version`

Static GTFS tables are partitioned by `_feed_version` and written with
`mode("overwrite")` plus `replaceWhere` scoped to the version being ingested.

**Why not `append`** — re-running duplicates every row. Verified directly: two
appends of the stops file produced 20,538 rows from a 10,269-row source.

**Why not plain `overwrite`** — row counts stay stable across re-runs, which
looks like correctness. It is idempotency by destruction: every previous feed
version is thrown away. MBTA republished on 2026-09-15 and again on 2026-09-17,
with three stops differing between them. Under plain overwrite that change would
be undetectable, and SCD2 on `dim_stop` needs exactly that comparison to have
anything to track.

**`replaceWhere`** deletes only rows matching the predicate, then writes. Same
version in, exact replacement. New version in, lands alongside untouched history.

**Migration cost.** Partitioning is physical directory layout, not metadata, so
adding it to an existing table is a full rewrite. `replaceWhere` refuses to alter
partitioning at all (`DELTA_METADATA_MISMATCH`), so the six tables had to be
rebuilt once with `partitionBy` before the idempotent write would work. At 3.2M
rows that was about a minute. At production scale it is a maintenance window and
a backfill plan, which is the argument for deciding partition strategy before a
table grows.

---

## Realtime idempotency: deterministic filenames

Different mechanism, same goal. Snapshot filenames derive from the feed's own
`header.timestamp` (`snapshot_<epoch>.json`), so fetching the same publication
twice writes to the same path — an overwrite, not a second near-identical file.

Static idempotency uses a Delta feature; realtime uses a naming convention. Both
are the same property achieved at different layers.

---

## Realtime history is collected, never simulated

The GTFS-Realtime feed is a current-state endpoint. It reports where vehicles are
now and keeps no archive. Any project that starts processing it in its final week
has a few minutes of data and has to fabricate the rest.

A scheduled collector has been snapshotting every 15 minutes since day one of the
build. The accumulated files contain genuine duplicates, genuine gaps, and
genuine late-arriving records — which is what makes the incremental-loading work
an actual exercise rather than a demonstration against synthetic input.

This was the first thing built, before the repo was finished, because it is the
only part of the plan that cannot be caught up later.

---

## Snapshot overlap produces duplicate observations

Polling every 15 minutes against vehicles that report less frequently means the
same `(vehicle_id, vehicle_timestamp)` observation appears in multiple snapshots.

This matters because a Delta `MERGE` errors when two source rows match one target
row — the engine refuses to guess which of two conflicting updates was intended.
So the source is deduplicated before merging, keeping the copy from the most
recently published feed (`ORDER BY feed_ts DESC`) on the principle that later
publication reflects later knowledge.

---

## Cache versus materialise on serverless

Databricks Free Edition runs serverless compute, which does not support
`.cache()` / `PERSIST` — there is no long-lived executor memory to pin a
DataFrame into, because work can move between machines between queries.

The replacement is to write expensive intermediates down as Delta tables rather
than caching them in memory. Slower on first access, but the result survives
across cells, notebooks and job tasks, which in-memory cache never does.

Framed that way, the medallion architecture is this idea taken seriously: bronze
and silver are materialised intermediates, written once so nothing downstream has
to recompute them.

---

## Explicit schemas, validated on every load

Spark infers a JSON schema by sampling files. If an optional field is absent from
the sampled set, the column does not exist in the resulting schema — so the
schema depends on which files happened to be present at read time, and the same
code produces different results on different days.

Realtime ingestion uses an explicit `StructType`. But a declared schema has the
opposite failure: it silently discards any field it does not declare. The first
version dropped `trip.revenue` and `trip.last_trip` this way.

So every load compares leaf paths between an inferred read and the declared
schema, and `assert`s that nothing in the data is undeclared. Declared-but-absent
fields are allowed — they arrive as null, which is the point of declaring them.
The inference pass costs ~40–86s over ~670 files; the explicit read, explode and
write cost ~7s. The guard is worth the cost until the file count makes it
expensive, at which point it moves to a sample-plus-daily-full check.

`bearing` is declared `DoubleType` although every observed value is a whole
number, because bearings are angles and a feed that starts sending `183.5`
should not break the pipeline.

---

## Bronze is a pure function of the landing zone

Landing files are never modified or deleted after ingest. Every bronze table can
be rebuilt from them. This was tested the hard way: a migration deleted a feed
version from bronze, and it was restored by replaying landing.

The replay is a cell in the static ingest guarded by `RUN_BACKFILL = False`.
Normal runs write only the version just downloaded; a full replay is a deliberate
act, because its cost grows with every publication MBTA makes.

---

## Ingestion log counts are per version

With several feed versions in one partitioned table, a plain `count()` reports
every version combined. Each `ops.ingestion_log` row filters to the version it
describes, so row-count history compares like with like.

---

## Parse times, never compare them as strings

GTFS permits unpadded hours (`9:05:00`), and `'9:05:00' >= '24:00:00'` is true as
a string comparison. Any logic about GTFS times splits on `:` and works in
integer seconds. MBTA happens to pad every value, which makes string comparison
look correct on this feed while being wrong in general.

---

## Monitor what the collector writes, not what is derived from it

The health check reads the landing zone — file counts per day and the age of the
newest file — rather than the bronze table. Bronze only changes when a notebook
runs, so a dead collector and an idle pipeline look identical from bronze. That
distinction cost ~100 snapshots before it was made.

The freshness threshold is 35 minutes: two 15-minute intervals plus slack for
serverless start-up. A 20-minute threshold fired on a healthy collector. Alerts
must tolerate normal jitter, or they stop being read.

---

## Collectors are independent tasks

Vehicle positions and alerts run as separate tasks in one job with no dependency
between them. A failure fetching one feed must not stop the other from
collecting, because each lost interval is unrecoverable.

---

## Realtime bronze is a full reload, for now

`04` currently re-reads every snapshot and replaces the table (`replaceWhere
"_dt IS NOT NULL"`). At ~670 files that takes ~7 seconds, so it is acceptable
short-term. It grows linearly with history, and is replaced by an incremental,
watermarked load in story 2.3.

---

## Unscheduled service routes to the unknown member

4.9% of realtime rows are replacement shuttles (`Shuttle-Generic*`,
`schedule_relationship = ADDED`), added to service rather than scheduled. Their
trips are not expected in the static timetable (to be confirmed by the join in
story 2.6). They are kept, resolved to the `-1` unknown member in gold, and counted as a quality
metric. Dropping them would erase the vehicles that exist precisely because
scheduled service failed.

---

## Silver reads one feed version, not the latest copy of every row

Silver dimensions are built from the newest `_feed_version` only, after
asserting that stops, routes and trips all agree on what the newest version is.

Deduplicating across every version and keeping each key's latest row sounds
equivalent, but is not: a stop removed in a later publication would survive from
an earlier one, because its latest row is the old one. Filtering to a single
version reproduces the published schedule exactly. Change across versions is
SCD2's job.

The agreement check exists because a half-finished ingest could leave `stops`
on one version and `trips` on another, and silver would join them without error.

---

## Casting uses `try_cast`, and counts what fails

Serverless runs with ANSI mode on. A plain `cast` on a malformed value throws,
so one bad row fails the whole run. With ANSI off, the same cast silently returns
null — the failure bronze is designed to prevent.

`try_cast` returns null without throwing, and because the original string is
still present, failures are countable: a value existed and the cast produced
null. Each typed table asserts zero failures today; story 3.2 turns failures into
quarantined rows instead of a failed run.

The cast spec skips columns absent from the data and reports them, rather than
assuming every documented GTFS field is published.

---

## Codes are conformed through lookup joins, not `when` chains

`route_type` and `location_type` names come from small lookup tables joined in.
Unmapped codes are reported. A `when(...).otherwise("Unknown")` chain would
absorb a new code silently; the join makes it visible.

An empty `location_type` is coalesced to 0, because the GTFS spec defines empty
as "stop or platform". Leaving it null would drop those stops from any filter on
type 0.

---

## Version diffs use null-safe equality

Comparing a stop across versions uses `<=>`, not `=`. With `=`, two nulls compare
as null rather than true, so every stop without wheelchair data would count as
changed.

---

## Surrogate keys are hashes of the natural key

`route_key`, `trip_key` and `stop_key` are `xxhash64` of the natural key.
`monotonically_increasing_id()`, suggested in the original plan, was tested and
rejected: rebuilding the same 402 routes with a different partition layout
changed every single key. Because gold dimensions are rebuilt with `overwrite`,
unstable keys would silently re-point every existing fact row.

Hash keys are deterministic, need no lookup table and no state, and survive any
rebuild. Costs: they are large and unordered, collisions are possible in
principle (each dimension asserts key uniqueness on every write), and changing
the key recipe changes every key. The usual production alternative is a Delta
identity column with dimensions loaded by `MERGE`, which gives compact stable
integers at the price of merge-based loading.

`dim_stop`'s recipe will change to `stop_id + valid_from` when SCD2 arrives,
giving one key per version of a stop. Facts are rebuilt once at that point.

## `date_key` is a smart key, deliberately

`dim_date` uses `yyyyMMdd` integers rather than hashes. A date never changes
meaning, and a readable, sortable key lets facts be partitioned and filtered by
date without a join. This is the conventional exception to "keys carry no
meaning".

`day_of_week` is ISO (`weekday() + 1`, Monday = 1). Spark's `dayofweek()` makes
Sunday 1, which silently breaks any weekend logic built on 6 and 7.

## Every dimension has an unknown member

Key `-1`, descriptive columns set to `Unknown`. Facts whose foreign key cannot be
resolved point there instead of being dropped, and the count of `-1` references
becomes a quality metric. Each dimension asserts exactly one unknown member, which
also catches the theoretical case of a hash landing on `-1`.

## `dim_date` coverage is asserted

The notebook fails if any realtime date or scheduled service date falls outside
`dim_date`'s range, rather than letting those facts resolve silently to `-1`.

---

## GTFS times are stored as seconds since service start

`silver.stop_times` holds `arrival_seconds` and `departure_seconds` as the
canonical values, alongside the original strings, a `day_offset` (0 or 1) and a
24-hour `departure_clock`. It deliberately has no timestamp column: a stop time
belongs to a trip, and a trip runs on many dates. A timestamp only exists once a
trip is paired with a service date, which happens in `fact_scheduled_stop_time`.

## Timestamps anchor at noon minus 12 hours, per the spec

`service_time_to_ts` converts local noon on the service date to UTC, subtracts 12
hours, then adds the seconds. On the day clocks go back this anchor sits one hour
after local midnight; anchoring at midnight would shift every trip that day by an
hour. The session time zone is pinned to UTC because the conversion depends on it.

## The time parser never throws

`gtfs_time_to_seconds` validates the shape with a regex before any arithmetic,
so malformed input becomes null instead of an exception under ANSI mode. Nulls it
creates are counted separately from nulls published at source, and must be zero.

## Shared transforms live in `src/`, as plain files

`clean_strings`, `cast_columns`, `guard_unique` and the GTFS time functions are
importable modules in `src/transforms/`, not notebook cells. The first attempt
created them as notebooks named `common.py`, which Python cannot import —
notebooks are stored with a Databricks header and a doubled extension. They must
be workspace files.

---

## Realtime silver: one row per observation, loaded by incremental MERGE

Grain: one row per `(vehicle_id, vehicle_ts)` — one row per report a vehicle
made, however many snapshots carried it. Carriages: one row per
`(vehicle_id, vehicle_ts, carriage_sequence)`.

**The source is always collapsed to the grain before merging.** A MERGE into an
empty target inserts every duplicate source row silently; into a populated target
it fails. Both were reproduced on real data.

**Collapsing keeps the latest snapshot's values plus `first_seen_snapshot_ts` and
`last_seen_snapshot_ts`.** Min and max are idempotent: re-reading overlapping
snapshots cannot change them. A count of appearances would not be — every
overlapping re-read would inflate it — so none is stored.

**Two conditional update rules.** Update values only when the incoming copy comes
from a later snapshot; pull `first_seen` back only when the incoming copy was seen
earlier. An identical re-read matches neither and writes nothing, so re-runs are
free.

**Watermark with a one-hour lookback.** Each run reads snapshots newer than the
watermark minus an hour. Correctness never depends on files arriving in order,
and the overlap costs nothing because the merge is idempotent. The watermark
moves forward only after both merges commit: a crash in between causes a harmless
re-read next run, whereas advancing it first would skip data permanently.

**Grain keys are asserted non-null before every merge.** `NULL = NULL` is not
true, so a null key never matches and would be re-inserted on every run.

**The dedup is deterministic.** Ties break on `_source_file`, because MERGE may
scan its source more than once, and a non-deterministic source can differ
between scans.

Every run appends to `ops.pipeline_runs`, and the table's own `DESCRIBE HISTORY`
metrics supply the inserted and updated counts.