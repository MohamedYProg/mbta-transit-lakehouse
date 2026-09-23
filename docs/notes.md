# Build notes

Things found by looking at the actual data, rather than by reading about it.
Kept as they happen — most of this is unrecoverable a week later. Where an early
note turned out to be wrong, the correction is kept rather than the note deleted.

---

## The feed's shape

A walk of the GTFS-Realtime schema found exactly two array positions, which is
the complete list of places an `explode` is required:

```
entity[]
entity[].vehicle.multi_carriage_details[]
```

Everything else is dot-notation into structs.

Note the struct-inside-same-named-struct at `entity[].vehicle.vehicle.id` — the
outer `vehicle` is the entity's payload, the inner one is the physical vehicle's
identity. Easy to misread.

---

## A declared schema silently dropped two fields

The first explicit `StructType` omitted `trip.revenue` and `trip.last_trip`. A
declared schema does not warn about undeclared fields — it discards them. All
270,493 rows of the first bronze load were missing both columns.

Caught by comparing leaf paths: the set of fields in an inferred read minus the
set declared. The fix brought the schema to 29 of 29 fields. The comparison now
runs on every load with an `assert`, so a field MBTA adds in future fails the run
instead of vanishing.

---

## Carriage fields are not what the docs imply

A carriage in `multi_carriage_details` has:

```
carriage_sequence, label, occupancy_percentage, occupancy_status, orientation
```

There is **no `id`** field. `label` is the identifier — the car number painted on
the side. Discovered by writing `c.id` and getting `FIELD_NOT_FOUND`. Lesson:
introspect the schema first, write the flatten second.

`orientation` is **76% null** (163,284 of 214,389 carriage rows). An earlier note
guessed it was entirely null, because every row in a 20-row sample was. Sampled
rows are not evidence; an aggregate is. It stays in the schema.

`carriage_sequence` equals array position + 1 on every row — the feed's own
numbering is trustworthy.

Train lengths are 1, 2, 4 or 6 cars and never 5: Green Line trolleys run 1–2
cars, heavy rail runs 6-car sets.

---

## Empty strings, and two assumptions that turned out wrong

Expected GTFS blanks to arrive as empty strings. They arrived as true nulls:
6,908 null `stop_desc`, zero empty-string `stop_desc`, and zero empty strings in
any stops column.

Spark's CSV reader nulls *unquoted* empty fields by default. MBTA writes unquoted
blanks. A publisher who quotes their blanks would produce empty strings and every
`IS NULL` check would silently find nothing.

**Correction.** An earlier note said "`parent_station` was non-empty on every
row". That was wrong. The check compared `parent_station == ""` — which can never
match once blanks land as null — and returned 0. The real figure, using
`IS NULL`: **7,105 of 10,279 stops (69%) have no parent station.** A finding from
one cell invalidated a check written minutes later in the next. Any zero from a
quality check is something to verify, not celebrate.

---

## Dedup on the wrong grain throws away 94% of the data

`row_number()` partitioned by `vehicle_id` alone: 16,962 rows in, 966 out.

That is correct for "where is each vehicle now" and completely wrong for a fact
table, whose grain is one row per vehicle per snapshot. Partitioning by
`(vehicle_id, vehicle_ts)` keeps the history and removes only the duplicates.

Getting this wrong would not have errored. It would have produced a fact table
that silently contained only the most recent observation per vehicle.

---

## Duplicate observations: two different phenomena with the same shape

At the correct grain, 1.4% of rows are duplicates (328,740 -> 324,223). Grouped by
vehicle-id prefix:

| prefix | fleet | dupe groups | avg repeats | max repeats |
|---|---|---|---|---|
| `y` | buses | 1,142 | 2.1 | 4 |
| `G` | Green Line | 246 | 4.8 | 40 |
| `R` | Red Line | 223 | 7.5 | 17 |
| `O` | Orange Line | 75 | 8.8 | 16 |
| `B` | Blue Line | 49 | 7.8 | 16 |

Buses duplicate often but briefly — benign overlap between a 15-minute poll and a
slightly slower reporting interval. Rail vehicles repeat 7–9 times on average,
meaning roughly two hours at one timestamp: trains sitting at terminals or yards.
The Green Line holds the extreme: one vehicle reported the same timestamp across
40 consecutive snapshots, ten hours of a frozen position.

An earlier note called this "Green Line staleness". It is a rail pattern; the
Green Line only has the worst single case. The dedup treats both phenomena
identically, which is correct, but staleness is a signal in its own right: a
freshness rule comparing `vehicle_ts` against `_snapshot_ts` surfaces it.

---

## 4.9% of vehicles are on routes the schedule has never heard of

`direction_id`, `trip_start_date` and `trip_start_time` are null on the same 4.9%
of rows, while `trip_id` and `route_id` are never null. Every one of those rows
is a shuttle bus with `schedule_relationship = ADDED`:

```
Shuttle-Generic                      14,883
Shuttle-Generic-Red                   1,196
Shuttle-Generic-Blue                     31
Shuttle-Generic-Green                    31
Shuttle-Generic-Orange                    5
Shuttle-Generic-CommuterRail-South        4
Shuttle-Generic-CommuterRail              2
```

These are replacement shuttles running during disruptions — service added live
rather than published in advance. Story 2.6 confirmed the picture, with one
correction: the shuttle **routes** do exist in the static schedule
(`Shuttle-Generic` resolves in `dim_route`), but their **trips** never do. See
"Why 7.8% of trips don't resolve" below. The disruptions that cause
them are announced in the Alerts feed, now being collected — a natural join once
alerts are modelled.

---

## Non-revenue movement is small

`trip.revenue = false` on 3,707 of 328,740 rows (1.1%), never null. Vehicles
repositioning without passengers barely move occupancy averages. Filtering them in
silver is still correct, but it is a small effect, not a large one.

---

## `speed` is 91% null

Effectively unusable as a network measure — a mean over 9% of rows is a sample of
whichever vehicles report, not network speed. Kept, never aggregated, and a
`warn`-severity rule rather than `error`.

---

## Weekend service is visible in collected data

Vehicles per snapshot at identical snapshot coverage (96 a day):

```
Tue 09-15  515    Fri 09-18  515
Wed 09-16  507    Sat 09-19  378
Thu 09-17  509
```

Saturday runs about 27% below weekdays. A row-count rule calibrated on weekdays
would false-alarm every weekend; it needs to be day-of-week aware.

---

## 157,270 stop times fall after midnight of their service day

In the 2026-09-21 feed version, **157,270 of 3,179,748 stop times (4.9%)** have a
departure hour of 24 or more, ranging `24:00:00` to `27:05:00`.

GTFS defines times relative to the *service day*, so a train departing 1:15am on
a service day that began the previous morning is published as `25:15:00`. A naive
cast to timestamp returns null for every one of these — silently, across the
whole late-night network.

The count deliberately splits on `:` rather than comparing strings. GTFS allows
unpadded hours (`9:05:00`), and as strings `'9:05:00' >= '24:00:00'` is true
because `'9'` sorts after `'2'`. MBTA pads every time (0 unpadded, 0 unparseable),
so string comparison would have happened to work here — but only by luck of this
publisher's formatting.

Fixed properly in silver: seconds-since-service-start as the canonical value, the
timestamp derived by adding it to the service date.

---

## MBTA republishes often, and each version differs

Three publications landed in one week:

| version | stops | routes | trips | stop_times | calendar | calendar_dates | feed_start_date |
|---|---|---|---|---|---|---|---|
| 2026-09-15 | 10,272 | — | — | — | — | — | 20260908 |
| 2026-09-17 | 10,269 | 399 | 125,827 | 3,194,798 | 153 | 140 | 20260910 |
| 2026-09-21 | 10,279 | 402 | 124,265 | 3,179,748 | 156 | 128 | 20260914 |

`feed_end_date` stayed at 20261212 throughout; the start date rolls forward with
each publication. Real change between real publications is exactly what SCD2
needs, and it did not have to be manufactured.

It also means bronze grows by ~3.3M rows per publication. Silver reads the latest
version (or tracks change across versions); it never reads every version as if
they were one.

---

## Serverless does not support `cache()`

`[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless
compute.`

Three `count()` calls on an uncached chain took 2.4 seconds — all the work done
three times over. The serverless answer is to materialise to a Delta table. Which
is what bronze and silver already are.

---

## Inference is the slow part of reading JSON

Over 671 snapshot files: a schema-inferred read took **86 seconds**; the
explicit-schema read, explode and Delta write together took **7 seconds**. Single
runs on serverless vary widely (earlier inferred reads ranged 40–86s), so measure
several times before quoting a ratio.

---

## Notebook session state is not reproducible

Hit `NameError` four times across sessions for variables defined in another
notebook or a cell not re-run after a restart. Once, it hid a genuinely missing
cell: `FEED_SCHEMA` had been overwritten out of the notebook, and everything kept
working because the object was still in memory. A fresh run the next morning
exposed it.

A notebook that only works top-to-bottom in one unbroken session cannot be run by
a scheduler. Fixes adopted: derive state from the filesystem unconditionally,
always verify with **Clear state -> Run all**, and move logic into functions whose
inputs are visible in the signature.

---

## Duplication showed up on its own

`read_gtfs()` ended up defined identically in two notebooks, and `FEED_SCHEMA`
was lost from one entirely. That is the concrete case for extracting transforms
into `src/`: not "good practice", but two copies of the same logic with no single
place to fix a bug.

---

## Incident: a rename silently stopped the collector

**20 Sep ~10:00 UTC to 21 Sep ~10:45 UTC.** Renaming the collector notebook broke
the scheduled job, which referenced it by path. Every run failed; nothing alerted.
Visible afterwards in bronze as 40 snapshots on the 20th instead of 96, and 53 on
the 21st. About 100 snapshots of realtime history were lost and cannot be
recovered — the feed keeps no archive.

It went unnoticed because the health check read the bronze table, which only
changes when a notebook is run by hand, rather than the landing zone the
collector writes to.

Fixed by repointing the task and adding an on-failure email. The health check now
reads the landing zone directly and asserts freshness.

**Follow-up.** The first freshness threshold was 20 minutes and fired a false
alarm at 24 minutes on a healthy collector: a 15-minute schedule plus serverless
start-up jitter regularly exceeds 20. Raised to 35 minutes — two intervals plus
slack. An alert that cries wolf on normal jitter gets ignored, which is worse
than no alert.

---

## Incident: a migration deleted a feed version

The one-time migration adding `_feed_version` partitioning rewrote each table with
plain `overwrite` from only the newest landing directory, silently deleting the
2026-09-15 version from bronze — the exact loss `replaceWhere` exists to prevent.

Recovered by replaying every version from the landing zone with per-version
`replaceWhere`. The landing zone is the source of truth; bronze is derived from it
and can always be rebuilt. This is why raw files are kept unmodified.

---

## Silver dimensions: the schedule is clean, and the diff is one station

Built from feed version 2026-09-21. Zero cast failures across all 11 typed
columns, zero duplicate natural keys, zero trips pointing at a missing route,
zero stops whose `parent_station` doesn't exist, zero stops outside the
service-area box. Bronze and silver row counts match exactly on all three tables.

**Every null coordinate is explained by the spec.** GTFS requires coordinates for
stops, stations and entrances, and allows them to be absent on generic nodes:

| location_type | stops | null lat |
|---|---|---|
| 0 Stop / Platform | 7,746 | 0 |
| 1 Station | 276 | 0 |
| 2 Entrance / Exit | 333 | 0 |
| 3 Generic Node | 1,924 | 605 |

No boarding areas (type 4) exist in MBTA's feed.

**The network by mode:** 371 bus routes, 14 commuter rail, 9 ferry, 5 light rail
(Mattapan and the four Green Line branches), 3 subway (Red, Orange, Blue).

**What changed between 2026-09-17 and 2026-09-21:** 10 stops added, 0 removed,
72 changed — and the visible changes are concentrated at Downtown Crossing.
Platforms that previously all shared the station's coordinate
(`42.355518`) were given individual positions up to ~45 m away; generic nodes
that had no coordinates gained them; entrances were renamed with numbered
prefixes (`Franklin St` -> `7 | Franklin St`), one fixing a typo
(`Lafaytette`). Several other entrances moved by 0.000001–0.000015 degrees —
between 0.1 m and about 1.5 m.

That matters for SCD2. Hashing raw coordinates would create a new history
version of a stop for a 30-centimetre survey adjustment. The tracked attributes
need a tolerance (for example, coordinates rounded to 5 decimal places, ~1 m)
or coordinates treated as Type 1, overwritten without history, while names and
accessibility stay Type 2.

---

## `monotonically_increasing_id()` gave every route a new key on rebuild

Built the same 402 routes twice, once in one partition and once in eight, and
assigned keys with `monotonically_increasing_id()`. **0 of 402 routes kept the
same key.** Route `106` was `6` in the first build and `0` in the second; route
`105` went from `5` to `60,129,542,144`.

The function packs the partition index into the upper bits — each step is
2^33 = 8,589,934,592 — so the key reflects how Spark happened to split the data,
not the data. A dimension rebuilt with `overwrite` would silently re-point every
fact written before the rebuild.

`xxhash64(route_id)` gave identical keys on both builds for all 402 routes, and a
rebuild check confirms zero keys moved across all three hashed dimensions.

## Gold dimensions

| table | rows | notes |
|---|---|---|
| `dim_date` | 1,462 | 2024-01-01 to 2027-12-31 plus unknown; `date_key` is `yyyyMMdd` |
| `dim_route` | 403 | 402 routes plus unknown |
| `dim_trip` | 124,266 | 124,265 trips plus unknown |
| `dim_stop` | 10,280 | 10,279 stops plus unknown; parent station name flattened in |

`dim_date` covers the realtime data (2026-09-14 onward) and the schedule's
service window (to 2026-12-12) with room to spare.

---

## The `25:15:00` problem, measured

Feed version 2026-09-21: 3,179,748 stop times, **157,270 (4.9%) after midnight**,
latest `27:05:00`. Zero blank times at source.

Two naive parses, two failure modes:

- `try_to_timestamp(departure_time, 'HH:mm:ss')` returned **157,270 nulls** and
  no error — the entire late-night network silently gone. This is what a
  pipeline with ANSI mode off does with a plain cast.
- `to_timestamp` under ANSI mode failed the whole query on the first late trip:
  `[CANNOT_PARSE_TIMESTAMP] Text '25:11:00' could not be parsed: Invalid value
  for HourOfDay (valid values 0 - 23): 25`.

Silent or loud, both wrong: the data is valid; the assumption that a time of day
stays under 24 hours is what breaks.

The fix parses to seconds since service start. Zero parse failures, zero rows
lost, all 157,270 late rows preserved with `day_offset = 1`. Sanity rules on the
parsed timetable: zero stops where a vehicle departs before it arrives, zero
trips that go backwards in time, and no trip runs past the next calendar day.

## Daylight saving hides a second bug behind the first

GTFS measures times from "noon minus 12 hours" on the service day, not midnight.
Measured gap between the two anchors:

| service date | anchor minus midnight |
|---|---|
| 2026-09-22 | 0 s |
| 2026-11-01 (clocks go back) | 3,600 s |

The latest real departure, trip `79230141` at `27:05:00`, resolved both ways:

| service date | spec-correct | midnight-based |
|---|---|---|
| 2026-09-22 | 03:05 on the 23rd | 03:05 on the 23rd |
| 2026-11-01 | 03:05 on the 2nd | **02:05** on the 2nd |

Midnight-plus-seconds is correct 364 days a year and an hour wrong for every trip
on the DST service day. The feed's validity window (to 2026-12-12) includes that
day, so this is live, not hypothetical.

## Other stop_times facts

- `shape_dist_traveled` is not published by MBTA at all. The original plan listed
  it as a measure on `fact_scheduled_stop_time`; it will not exist.
- `silver.stop_times` wrote in 17 s as 2 files, 25 MB. That is the pre-optimisation
  baseline — already compact, so the Phase 2 optimisation story needs the planned
  deliberately-fragmented build to have anything to fix.

---

## MERGE without deduplication: silent first, loud second

Worst real duplicate: vehicle `G-10100` at `1789468017` — one observation present
in 40 snapshots. MERGEd without deduplicating the source:

- **Run 1, empty target: 40 rows inserted for one observation, no error.**
  Duplicate source rows that match nothing are all inserted.
- **Run 2: `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`.** Now 40
  source rows match 40 target rows and Delta refuses to choose.

The well-known error only appears once the target already holds the key. The
first load of a pipeline — the moment the target is empty — duplicates silently.

## Incremental MERGE into silver: the numbers

| run | snapshots read | rows read | positions inserted | carriages inserted |
|---|---|---|---|---|
| first (watermark 0) | 671 | 328,740 | 324,223 | 199,344 |
| re-runs x3 (1 h lookback) | 4 | 2,586 | 0 | 0 |
| full re-read (watermark reset to 0) | 671 | 328,740 | 0 | 0 |
| real incremental (after new bronze) | 7 | 4,280 | 1,683 (1 updated) | 1,168 (1 updated) |

The real incremental run read 7 snapshots (the one-hour lookback plus the new
ones) and inserted only new observations. Its single update is rule 1 working as
designed: one observation first seen before the watermark was still being
published, unchanged, in a newer snapshot, so its `last_seen` moved forward.

Zero updates on every re-run: identical observations match neither update rule
and write nothing. Silver equals the distinct observations in bronze exactly
(324,223 positions, 199,344 carriages), with zero duplicate keys, zero carriages
without a parent observation, and the watermark exactly at the newest snapshot.

The incremental re-run reads 0.8% of the rows, but its wall time (~28 s) is close
to the full load's (32 s). At 330k rows, fixed per-query overhead — history
lookups, counts, serverless round-trips — dominates. The benefit of incremental
loading is that per-run cost stays flat as history grows; at this size it does
not yet show up as speed.

## How old is a report, and how long does it stay frozen?

From `first_seen_snapshot_ts - vehicle_ts` and
`last_seen_snapshot_ts - first_seen_snapshot_ts`:

| fleet (id prefix) | observations | median age when published | longest frozen | frozen > 1 h |
|---|---|---|---|---|
| `y` buses | 252,277 | 7 s | 0.5 h | 0 |
| `G` Green Line | 29,311 | 22 s | 9.7 h | 29 |
| `1` numeric ids | 16,601 | 16 s | 0.0 h | 0 |
| `R` Red Line | 10,967 | 27 s | 4.0 h | 121 |
| `O` Orange Line | 7,973 | 24 s | 4.2 h | 53 |
| `B` Blue Line | 5,318 | 21 s | 3.8 h | 21 |

(`d` and `2` prefixes are small groups — 1,617 and 159 — not yet identified.)

"Age" here is measured against the feed's own header timestamp, so it means how
stale a vehicle's report already is when MBTA publishes it, not our polling delay.
Buses publish nearly live; rail reports are three to four times older.

Freezing is a rail phenomenon. No bus observation stayed frozen over an hour; 224
rail observations did. The Red Line has the most frozen observations, the Green
Line the longest single case. This is the freshness rule for the quality engine,
already measured: `last_seen - first_seen > 1 h` flags a vehicle that has stopped
updating.


---

## `fact_vehicle_position`: the star schema works end to end

325,906 observations, one row per vehicle report. Written in 16 s, 9 files
(one per day), 11.7 MB. Zero orphan keys across all four dimensions — every
foreign key resolves to a real member or the unknown member, so plain inner joins
never drop rows. Row count equals silver exactly, so no join multiplied rows.

## Why 7.8% of trips don't resolve — and why it isn't 7.8% bad data

| foreign key | blank at source | not in dimension | unknown |
|---|---|---|---|
| route_key | 0 | 0 | 0.00% |
| trip_key | 0 | 25,507 | 7.83% |
| stop_key | 16,969 | 4 | 5.21% |
| date_key | 0 | 0 | 0.00% |

The hypothesis going in was that old schedule versions caused the trip mismatches,
since dimensions are built from the newest version while observations span older
ones. Tested against all three bronze versions, that explains only 3% of it:

| cause | distinct trips | fact rows |
|---|---|---|
| never in any published schedule | 4,997 | 24,731 |
| only in 2026-09-15, 2026-09-17 | 208 | 776 |

97% of unresolved rows are trips MBTA **added live** (`schedule_relationship =
ADDED`) that no published schedule will ever contain: `Shuttle-Generic` (13,748
rows), extra Red, Orange, Green and Blue Line trips, and a few bus routes. The
unknown rate spikes at the weekend — 21.4% on Saturday 19th, 40.6% on Sunday 20th
(a small, outage-affected sample) — against 4.6–6.3% on weekdays, consistent with
weekend service diversions run with added trips.

So the genuine mismatch between the two feeds — a trip the schedule once had and
no longer does — is 776 rows, **0.24%**. A version-aware `dim_trip` (SCD2) would
fix that 0.24%. It would do nothing for the other 7.6%, which need their own
special member rather than being counted as "unknown".

Stops: 16,969 observations report no stop at all (vehicle between assignments —
"not applicable", not an error). Only 4 report a stop the schedule lacks.

## Rail occupancy isn't at vehicle level

The first star-schema query — average occupancy by mode and weekday, revenue
trips only — returned **only buses** (about 19–21% on weekdays, 20.2% on
Saturday). Rail vehicles report occupancy per carriage in
`multi_carriage_details`, not on the vehicle. Rail occupancy analysis needs the
carriage grain (`silver.vehicle_carriages`), which argues for a carriage-level
fact.

Sunday's bus figure (8.1% over 1,483 observations) comes from the outage day,
when the few snapshots collected were mostly early morning — not a real Sunday
average.

**After adding the `-2` member:** `trip_key = -1` fell from 25,507 rows (7.83%)
to **776 (0.24%)**, and `-2` holds the 24,731 added trips. Every remaining `-1`
is a trip present in an older schedule version only, and all 776 fall on one
weekend: 736 on Saturday 19th and 40 on Sunday 20th, with zero on every weekday.
The schedule-change mismatch is not spread across the data — it is that weekend's
trips, which the 2026-09-21 publication no longer contains.