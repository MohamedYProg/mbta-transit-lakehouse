# Build notes

Things found by looking at the actual data, rather than by reading about it.
Kept as they happen — most of this is unrecoverable a week later.

---

## The feed's shape

`paths()` walk of the GTFS-Realtime schema found exactly two array positions,
which is the complete list of places an `explode` is required:

```
entity[]
entity[].vehicle.multi_carriage_details[]
```

Everything else is dot-notation into structs.

Note the struct-inside-same-named-struct at `entity[].vehicle.vehicle.id` — the
outer `vehicle` is the entity's payload, the inner one is the physical vehicle's
identity. Easy to misread.

---

## Carriage fields are not what the docs imply

A carriage in `multi_carriage_details` has:

```
carriage_sequence, label, occupancy_percentage, occupancy_status, orientation
```

There is **no `id`** field. `label` is the identifier — the car number painted on
the side. Discovered by writing `c.id` and getting `FIELD_NOT_FOUND`, which at
least fails loudly. The same gap between assumed and actual schema produces
silent failures elsewhere.

Lesson taken: introspect the schema first, write the flatten second. Reversing
that order cost a debugging cycle.

`orientation` is a small enumeration (AB / BA) describing which way the car faces.

---

## Empty strings, and the assumption that turned out wrong

Expected GTFS blanks to arrive as empty strings. They arrived as true nulls:
10,269 stops, 6,908 null `stop_desc`, zero empty-string `stop_desc`.

Spark's CSV reader nulls *unquoted* empty fields by default. MBTA writes unquoted
blanks. A publisher who quotes their blanks would produce empty strings and every
`IS NULL` check would silently find nothing — which is exactly the kind of
assumption a quality rule should pin down rather than trust.

`parent_station` was non-empty on every row, which was also unexpected. MBTA
models even standalone bus stops as children of something.

---

## Dedup on the wrong grain throws away 94% of the data

`row_number()` partitioned by `vehicle_id` alone: 16,962 rows in, 966 out.

That is correct for "where is each vehicle now" and completely wrong for a fact
table, whose grain is one row per vehicle per snapshot. The 966 is the count of
distinct vehicles seen at any point since collection began — higher than the ~680
in any single snapshot, because the fleet rotates through the day.

Partitioning by `(vehicle_id, vehicle_ts)` instead keeps the history and removes
only the genuine duplicates caused by snapshot overlap.

Getting this wrong would not have errored. It would have produced a fact table
that silently contained only the most recent observation per vehicle.

---

## Vehicle report gaps

`lag()` over `vehicle_ts` partitioned by `vehicle_id` gives the interval between
consecutive reports. Most cluster around 900 seconds, matching the 15-minute
poll. The ones that do not are real: vehicles leaving service, feed hiccups,
reports arriving out of order.

Those irregularities are the reason the incremental load has to be genuinely
idempotent rather than merely append-only.

---

## Times above 24:00:00 exist in the real feed

`SELECT count(*) FROM bronze.gtfs_stop_times WHERE departure_time >= '24:00:00'`
returns a non-zero count.

GTFS defines times relative to the *service day*, not the calendar day, so a
train departing 1:15am on a service day that began the previous morning is
published as `25:15:00`. A naive cast to timestamp returns null for every one of
these — silently, with no error, across the entire late-night network.

String comparison finds them here precisely because bronze keeps the column as
text. Typing on ingest would have destroyed the evidence before anyone looked.

To be fixed properly in silver: parse to seconds-since-service-start as the
canonical value, then derive the timestamp by adding to the service date.

---

## Feed validity window

`feed_start_date=20260908`, `feed_end_date=20261212`.

Service dates cannot be generated outside this window — `calendar.txt` does not
describe them. Constrains `fact_scheduled_stop_time`.

---

## MBTA republishes often

Two feed versions landed within three days (2026-09-15 and 2026-09-17), with
three stops differing between them. Row counts: 10,272 then 10,269.

Useful rather than annoying: real change between real publications is exactly
what SCD2 needs, and it did not have to be manufactured.

---

## Table sizes

```
gtfs_stop_times      3,194,798
gtfs_trips             125,827
gtfs_stops              10,269
gtfs_routes                399
gtfs_calendar              153
gtfs_calendar_dates        140
```

About 25 stops per trip, 315 trips per route. That ratio is the case for
separating facts from dimensions: a query filtering to one route reads 399 rows
in the small table to locate the relevant slice of 3.2M in the large one.

---

## Serverless does not support `cache()`

`[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless
compute.`

Three `count()` calls on an uncached chain took 2.4 seconds — all the work done
three times over, because transformations are lazy and nothing is retained
between actions.

The serverless answer is to materialise to a Delta table instead of caching.
Which is what bronze and silver already are.

---

## Notebook session state is not reproducible

Hit `NameError` three times in one session for variables defined in a different
notebook or a cell that had not been re-run after a restart.

A notebook that only works when its cells are run top-to-bottom in one unbroken
session cannot be run by a scheduler — and a scheduler is precisely the situation
where nobody is present to run them in the right order.

Two fixes, both adopted: derive state from the filesystem unconditionally rather
than inheriting it, and move logic into functions whose inputs are visible in the
signature.

---

## Duplication showed up on its own

`read_gtfs()` ended up defined identically in two notebooks. Nobody predicted it
— it appeared because the same read logic was needed in two places and there was
nowhere shared to put it.

That is the concrete case for extracting transforms into `src/`: not "good
practice", but two copies of the same function with no single place to fix a bug.
