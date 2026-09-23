"""Realtime vehicle-position transforms. Pure: DataFrame in, DataFrame out."""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

VP_KEY = ["vehicle_id", "vehicle_ts"]
CARRIAGE_KEY = ["vehicle_id", "vehicle_ts", "carriage_sequence"]


def flatten_vehicle_positions(df: DataFrame, tz: str = "America/New_York") -> DataFrame:
    """bronze.rt_vehicle_positions -> one flat row per (snapshot file, vehicle)."""
    v = "entity.vehicle"
    ts = F.timestamp_seconds(F.col(f"{v}.timestamp"))
    return df.select(
        "_source_file", "_snapshot_ts",
        F.col("entity.id").alias("entity_id"),
        F.col(f"{v}.vehicle.id").alias("vehicle_id"),
        F.col(f"{v}.vehicle.label").alias("vehicle_label"),
        F.col(f"{v}.timestamp").alias("vehicle_ts"),
        ts.alias("vehicle_ts_utc"),
        F.to_date(F.from_utc_timestamp(ts, tz)).alias("obs_date_local"),
        F.col(f"{v}.current_status").alias("current_status"),
        F.col(f"{v}.current_stop_sequence").alias("stop_sequence"),
        F.col(f"{v}.stop_id").alias("stop_id"),
        F.col(f"{v}.occupancy_status").alias("occupancy_status"),
        F.col(f"{v}.occupancy_percentage").alias("occupancy_pct"),
        F.col(f"{v}.position.latitude").alias("latitude"),
        F.col(f"{v}.position.longitude").alias("longitude"),
        F.col(f"{v}.position.bearing").alias("bearing"),
        F.col(f"{v}.position.speed").alias("speed"),
        F.col(f"{v}.trip.trip_id").alias("trip_id"),
        F.col(f"{v}.trip.route_id").alias("route_id"),
        F.col(f"{v}.trip.direction_id").alias("direction_id"),
        F.col(f"{v}.trip.start_date").alias("trip_start_date"),
        F.col(f"{v}.trip.start_time").alias("trip_start_time"),
        F.col(f"{v}.trip.schedule_relationship").alias("schedule_relationship"),
        F.col(f"{v}.trip.revenue").alias("is_revenue"),
        F.col(f"{v}.trip.last_trip").alias("is_last_trip"),
    )


def flatten_carriages(df: DataFrame) -> DataFrame:
    """bronze.rt_vehicle_positions -> one row per (snapshot file, vehicle, carriage).
    explode drops vehicles with no carriage array (buses), which is intended."""
    v = "entity.vehicle"
    return (df
        .select("_source_file", "_snapshot_ts",
                F.col(f"{v}.vehicle.id").alias("vehicle_id"),
                F.col(f"{v}.timestamp").alias("vehicle_ts"),
                F.explode(f"{v}.multi_carriage_details").alias("c"))
        .select("_source_file", "_snapshot_ts", "vehicle_id", "vehicle_ts",
                F.col("c.carriage_sequence").alias("carriage_sequence"),
                F.col("c.label").alias("carriage_label"),
                F.col("c.occupancy_status").alias("carriage_occupancy_status"),
                F.col("c.occupancy_percentage").alias("carriage_occupancy_pct"),
                F.col("c.orientation").alias("carriage_orientation")))


def collapse_to_grain(df: DataFrame, keys: list) -> DataFrame:
    """One row per grain key. Values come from the latest snapshot carrying the
    observation; first/last_seen_snapshot_ts span every snapshot it appeared in.
    min and max are idempotent, so re-reading overlapping snapshots is harmless.
    Ties are broken on _source_file so the result is deterministic: MERGE may
    scan its source more than once, and a non-deterministic source can differ
    between scans."""
    latest = Window.partitionBy(*keys).orderBy(F.desc("_snapshot_ts"), F.desc("_source_file"))
    span = Window.partitionBy(*keys)
    return (df
        .withColumn("first_seen_snapshot_ts", F.min("_snapshot_ts").over(span))
        .withColumn("last_seen_snapshot_ts",  F.max("_snapshot_ts").over(span))
        .withColumn("_rn", F.row_number().over(latest))
        .filter("_rn = 1")
        .drop("_rn", "_snapshot_ts"))