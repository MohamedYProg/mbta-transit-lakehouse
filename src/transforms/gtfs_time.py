"""GTFS service-day time handling.

GTFS times are measured from "noon minus 12 hours" on the service day. That equals
midnight on every day except daylight-saving changes. Times may exceed 24:00:00 for
trips running past midnight: 25:15:00 is 01:15 the next calendar day.
"""
from pyspark.sql import Column, functions as F

_TIME = r"^\d{1,2}:[0-5]\d:[0-5]\d$"


def _col(c):
    return c if isinstance(c, Column) else F.col(c)


def gtfs_time_to_seconds(c) -> Column:
    """'HH:MM:SS' -> seconds since service-day start. Hours may exceed 23.
    Null or malformed input -> null. Never throws, even under ANSI mode."""
    t = F.trim(_col(c))
    p = F.split(t, ":")
    return F.when(t.rlike(_TIME),
                  p[0].cast("int") * 3600 + p[1].cast("int") * 60 + p[2].cast("int"))


def day_offset(seconds) -> Column:
    """Whole days past the service date: 0 for 08:00:00, 1 for 25:15:00."""
    return F.floor(_col(seconds) / 86400).cast("int")


def seconds_to_clock(seconds) -> Column:
    """Seconds since service start -> 'HH:MM:SS' on a 24-hour clock (wraps past midnight)."""
    s = _col(seconds) % 86400
    return F.format_string("%02d:%02d:%02d",
                           F.floor(s / 3600).cast("int"),
                           F.floor((s % 3600) / 60).cast("int"),
                           (s % 60).cast("int"))


def service_time_to_ts(service_date, seconds, tz: str = "America/New_York") -> Column:
    """UTC instant of a GTFS time on a given service date.
    Anchored at local noon minus 12h, as the spec defines, not at midnight.
    Requires spark.sql.session.timeZone = UTC."""
    noon_local = F.concat(F.date_format(_col(service_date), "yyyy-MM-dd"),
                          F.lit(" 12:00:00")).cast("timestamp")
    noon_utc = F.to_utc_timestamp(noon_local, tz)
    return F.timestamp_seconds(F.unix_timestamp(noon_utc) - 43200 + _col(seconds))