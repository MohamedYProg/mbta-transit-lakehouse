"""Realtime flattening and grain collapsing.

collapse_to_grain is what makes the incremental MERGE safe: a MERGE fails when
two source rows match one target row, and duplicate observations are normal
because the poll interval is finer than some vehicles' reporting interval.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType,
                               LongType, StringType, StructField, StructType)

from src.transforms.vehicle_positions import (CARRIAGE_KEY, VP_KEY,
                                              collapse_to_grain,
                                              flatten_carriages,
                                              flatten_vehicle_positions)

CARRIAGE = StructType([
    StructField("carriage_sequence", IntegerType()),
    StructField("label", StringType()),
    StructField("occupancy_status", StringType()),
    StructField("occupancy_percentage", IntegerType()),
    StructField("orientation", StringType()),
])

VEHICLE = StructType([
    StructField("current_status", StringType()),
    StructField("current_stop_sequence", IntegerType()),
    StructField("timestamp", LongType()),
    StructField("occupancy_status", StringType()),
    StructField("occupancy_percentage", IntegerType()),
    StructField("stop_id", StringType()),
    StructField("position", StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("bearing", DoubleType()),
        StructField("speed", DoubleType()),
    ])),
    StructField("trip", StructType([
        StructField("route_id", StringType()),
        StructField("trip_id", StringType()),
        StructField("direction_id", IntegerType()),
        StructField("start_date", StringType()),
        StructField("start_time", StringType()),
        StructField("schedule_relationship", StringType()),
        StructField("last_trip", BooleanType()),
        StructField("revenue", BooleanType()),
    ])),
    StructField("vehicle", StructType([
        StructField("id", StringType()),
        StructField("label", StringType()),
    ])),
    StructField("multi_carriage_details", ArrayType(CARRIAGE)),
])

BRONZE = StructType([
    StructField("_source_file", StringType()),
    StructField("_snapshot_ts", LongType()),
    StructField("_dt", StringType()),
    StructField("entity", StructType([
        StructField("id", StringType()),
        StructField("vehicle", VEHICLE),
    ])),
])


def entity(vehicle_id, ts, *, lat=42.35, lon=-71.06, carriages=None,
           route="Red", status="IN_TRANSIT_TO"):
    return {
        "id": f"e-{vehicle_id}-{ts}",
        "vehicle": {
            "current_status": status, "current_stop_sequence": 3,
            "timestamp": ts, "occupancy_status": "MANY_SEATS_AVAILABLE",
            "occupancy_percentage": 25, "stop_id": "70061",
            "position": {"latitude": lat, "longitude": lon,
                         "bearing": 180.0, "speed": None},
            "trip": {"route_id": route, "trip_id": "t1", "direction_id": 0,
                     "start_date": "20260922", "start_time": "08:00:00",
                     "schedule_relationship": "SCHEDULED",
                     "last_trip": False, "revenue": True},
            "vehicle": {"id": vehicle_id, "label": vehicle_id.upper()},
            "multi_carriage_details": carriages,
        }}


def bronze(spark, rows):
    """rows: list of (source_file, snapshot_ts, entity dict)"""
    return spark.createDataFrame(
        [{"_source_file": f, "_snapshot_ts": s, "_dt": "2026-09-22", "entity": e}
         for f, s, e in rows], BRONZE)


class TestFlattenVehiclePositions:

    def test_nested_fields_become_columns(self, spark):
        df = bronze(spark, [("f1", 1000, entity("v1", 990))])
        row = flatten_vehicle_positions(df).first()
        assert row.vehicle_id == "v1"
        assert row.latitude == 42.35
        assert row.route_id == "Red"
        assert row.is_revenue is True

    def test_one_row_per_entity(self, spark):
        df = bronze(spark, [("f1", 1000, entity("v1", 990)),
                            ("f1", 1000, entity("v2", 995))])
        assert flatten_vehicle_positions(df).count() == 2

    def test_observation_date_is_local_not_utc(self, spark):
        """03:00 UTC is still the previous evening in Boston."""
        ts = 1790046000        # 2026-09-22 03:00:00 UTC
        df = bronze(spark, [("f1", ts, entity("v1", ts))])
        row = flatten_vehicle_positions(df, "America/New_York").first()
        assert str(row.obs_date_local) == "2026-09-21"


class TestFlattenCarriages:

    def test_one_row_per_carriage(self, spark):
        cars = [{"carriage_sequence": i, "label": f"c{i}",
                 "occupancy_status": "FULL", "occupancy_percentage": 90,
                 "orientation": None} for i in (1, 2, 3)]
        df = bronze(spark, [("f1", 1000, entity("v1", 990, carriages=cars))])
        assert flatten_carriages(df).count() == 3

    def test_vehicles_without_carriages_are_dropped(self, spark):
        """Buses have no carriage array. explode drops them, which is intended:
        a bus has no place in a carriage table."""
        df = bronze(spark, [("f1", 1000, entity("bus1", 990, carriages=None)),
                            ("f1", 1000, entity("bus2", 990, carriages=[]))])
        assert flatten_carriages(df).count() == 0


class TestCollapseToGrain:

    def test_one_row_per_observation(self, spark):
        """The same observation seen in three snapshots collapses to one row."""
        df = bronze(spark, [("f1", 1000, entity("v1", 990)),
                            ("f2", 1900, entity("v1", 990)),
                            ("f3", 2800, entity("v1", 990))])
        flat = flatten_vehicle_positions(df)
        assert flat.count() == 3
        assert collapse_to_grain(flat, VP_KEY).count() == 1

    def test_first_and_last_seen_span_every_snapshot(self, spark):
        df = bronze(spark, [("f1", 1000, entity("v1", 990)),
                            ("f2", 1900, entity("v1", 990)),
                            ("f3", 2800, entity("v1", 990))])
        row = collapse_to_grain(flatten_vehicle_positions(df), VP_KEY).first()
        assert (row.first_seen_snapshot_ts, row.last_seen_snapshot_ts) == (1000, 2800)

    def test_values_come_from_the_latest_snapshot(self, spark):
        df = bronze(spark, [("f1", 1000, entity("v1", 990, status="STOPPED_AT")),
                            ("f2", 1900, entity("v1", 990, status="IN_TRANSIT_TO"))])
        row = collapse_to_grain(flatten_vehicle_positions(df), VP_KEY).first()
        assert row.current_status == "IN_TRANSIT_TO"

    def test_different_timestamps_are_different_observations(self, spark):
        """Grain is (vehicle_id, vehicle_ts). Collapsing on vehicle alone would
        keep only the newest position and throw the history away."""
        df = bronze(spark, [("f1", 1000, entity("v1", 990)),
                            ("f2", 1900, entity("v1", 1890))])
        assert collapse_to_grain(flatten_vehicle_positions(df), VP_KEY).count() == 2

    def test_is_idempotent(self, spark):
        """Collapsing an already-collapsed frame changes nothing, which is what
        makes re-reading overlapping snapshots safe."""
        df = bronze(spark, [("f1", 1000, entity("v1", 990)),
                            ("f2", 1900, entity("v1", 990))])
        once = collapse_to_grain(flatten_vehicle_positions(df), VP_KEY)
        twice = collapse_to_grain(once.withColumn("_snapshot_ts",
                                                  F.col("last_seen_snapshot_ts")), VP_KEY)
        assert once.count() == twice.count() == 1
        assert (twice.first().first_seen_snapshot_ts,
                twice.first().last_seen_snapshot_ts) == (1000, 1900)

    def test_carriage_grain_includes_the_sequence(self, spark):
        cars = [{"carriage_sequence": i, "label": f"c{i}",
                 "occupancy_status": "FULL", "occupancy_percentage": 90,
                 "orientation": None} for i in (1, 2)]
        df = bronze(spark, [("f1", 1000, entity("v1", 990, carriages=cars)),
                            ("f2", 1900, entity("v1", 990, carriages=cars))])
        out = collapse_to_grain(flatten_carriages(df), CARRIAGE_KEY)
        assert out.count() == 2
