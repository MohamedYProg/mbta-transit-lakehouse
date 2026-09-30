"""GTFS service-day time handling.

The behaviour under test is the reason silver stores seconds rather than
timestamps: GTFS times may exceed 24 hours, and they are measured from noon
minus 12 hours on the service day, not from midnight.
"""
import pytest
from pyspark.sql import functions as F

from src.transforms.gtfs_time import (day_offset, gtfs_time_to_seconds,
                                      seconds_to_clock, service_time_to_ts)

TZ = "America/New_York"


def parse(spark, values):
    df = spark.createDataFrame([(v,) for v in values], "t string")
    return [r[0] for r in df.select(gtfs_time_to_seconds("t")).collect()]


class TestGtfsTimeToSeconds:

    def test_ordinary_times(self, spark):
        assert parse(spark, ["00:00:00", "08:15:30", "23:59:59"]) == [0, 29730, 86399]

    def test_times_past_midnight(self, spark):
        """25:15:00 is 1:15am on the next calendar day, still the same service day."""
        assert parse(spark, ["24:00:00", "25:15:00", "27:05:00"]) == [86400, 90900, 97500]

    def test_unpadded_hour(self, spark):
        """The spec allows H:MM:SS. String comparison gets this wrong:
        '9:05:00' >= '24:00:00' is true as text."""
        assert parse(spark, ["9:05:00"]) == [32700]

    def test_null_in_null_out(self, spark):
        assert parse(spark, [None]) == [None]

    @pytest.mark.parametrize("bad", ["", "abc", "12:60:00", "12:00:60",
                                     "12:00", "12:00:00:00", "-1:00:00"])
    def test_malformed_returns_null_and_never_raises(self, spark, bad):
        """Under ANSI mode a bare cast would throw and fail the whole job."""
        assert parse(spark, [bad]) == [None]

    def test_whitespace_is_tolerated(self, spark):
        assert parse(spark, ["  08:15:30  "]) == [29730]


class TestDayOffset:

    def test_offsets(self, spark):
        df = spark.createDataFrame([(0,), (86399,), (86400,), (90900,)], "s long")
        assert [r[0] for r in df.select(day_offset("s")).collect()] == [0, 0, 1, 1]


class TestSecondsToClock:

    def test_wraps_past_midnight(self, spark):
        df = spark.createDataFrame([(0,), (29730,), (90900,), (97500,)], "s long")
        got = [r[0] for r in df.select(seconds_to_clock("s")).collect()]
        assert got == ["00:00:00", "08:15:30", "01:15:00", "03:05:00"]


class TestServiceTimeToTs:
    """The spec anchors at noon minus 12 hours. That equals midnight on every
    day except when the clocks change."""

    def resolve(self, spark, service_date, seconds):
        df = spark.createDataFrame([(service_date, seconds)], "d string, s long")
        return (df.select(F.date_format(
                    F.from_utc_timestamp(
                        service_time_to_ts(F.to_date("d"), "s", TZ), TZ),
                    "yyyy-MM-dd HH:mm:ss"))
                  .first()[0])

    def test_ordinary_day(self, spark):
        assert self.resolve(spark, "2026-09-22", 29730) == "2026-09-22 08:15:30"

    def test_past_midnight_lands_on_the_next_calendar_day(self, spark):
        assert self.resolve(spark, "2026-09-22", 97500) == "2026-09-23 03:05:00"

    def test_dst_day_is_not_shifted(self, spark):
        """1 Nov 2026 is the US DST fallback. A midnight-based conversion would
        return 02:05 here; the spec-correct anchor returns 03:05."""
        assert self.resolve(spark, "2026-11-01", 97500) == "2026-11-02 03:05:00"

    def test_dst_day_ordinary_time_unaffected(self, spark):
        assert self.resolve(spark, "2026-11-01", 29730) == "2026-11-01 08:15:30"
