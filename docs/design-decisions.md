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

## Explicit schemas, never inference (planned — story 1.7)

Spark infers a JSON schema by sampling files. If an optional field is absent from
the sampled set, the column does not exist in the resulting schema — so the
schema depends on which files happened to be present at read time, and the same
code produces different results on different days.

Realtime ingestion will define an explicit `StructType` rather than inferring.
Inference also costs a full extra pass over the data to determine types, but
non-determinism is the real argument.
