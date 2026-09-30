"""Shared fixtures. One local SparkSession for the whole test session."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (SparkSession.builder
         .master("local[1]")
         .appName("transit-tests")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.ansi.enabled", "true")   # matches serverless
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture
def evaluate_column(spark):
    """Evaluate a Column expression against rows of literals.

    rows: list of dicts. Returns the list of results for the expression."""
    def _run(rows, schema, build):
        df = spark.createDataFrame(rows, schema)
        return [r[0] for r in df.select(build().alias("out")).collect()]
    return _run
