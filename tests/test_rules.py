"""The quality rule engine.

failure_condition is the contract shared between the engine (which counts
failures) and the quarantine (which splits rows). If these drift apart, a row
can be reported as bad and still reach silver.
"""
import pytest

from src.quality.rules import (ROW_LEVEL, evaluate, evaluate_row_level_batch,
                               failure_condition, verdict)


@pytest.fixture
def stops(spark):
    return spark.createDataFrame(
        [("a", "Alewife",   42.39, -71.14, 1),
         ("b", "Davis",     42.39, -71.12, 1),
         ("c", None,        42.39, -71.12, 1),     # null name
         ("d", "Far Away",  99.00, -71.12, 1),     # impossible latitude
         ("e", "No Coords", None,  None,   3),     # allowed for generic nodes
         ("a", "Alewife",   42.39, -71.14, 1)],    # duplicate key
        "stop_id string, stop_name string, stop_lat double, stop_lon double, location_type int")


def failing(df, rule):
    return df.filter(failure_condition(rule)).count()


class TestFailureCondition:

    def test_not_null(self, stops):
        assert failing(stops, {"type": "not_null", "column": "stop_name"}) == 1

    def test_range_flags_only_out_of_range(self, stops):
        rule = {"type": "range", "column": "stop_lat",
                "params": {"min": 41.0, "max": 43.5}}
        assert failing(stops, rule) == 1          # 99.0 fails, null does not

    def test_range_ignores_nulls(self, spark):
        """A null is 'missing', not 'out of range'. not_null is a separate rule."""
        df = spark.createDataFrame([(None,), (5.0,)], "v double")
        rule = {"type": "range", "column": "v", "params": {"min": 0, "max": 10}}
        assert df.filter(failure_condition(rule)).count() == 0

    @pytest.mark.parametrize("value,fails", [(-1, True), (0, False), (100, False),
                                             (101, True)])
    def test_range_boundaries_are_inclusive(self, spark, value, fails):
        df = spark.createDataFrame([(value,)], "v int")
        rule = {"type": "range", "column": "v", "params": {"min": 0, "max": 100}}
        assert (df.filter(failure_condition(rule)).count() == 1) is fails

    def test_range_with_only_a_minimum(self, spark):
        df = spark.createDataFrame([(-5,), (500,)], "v int")
        rule = {"type": "range", "column": "v", "params": {"min": 0}}
        assert df.filter(failure_condition(rule)).count() == 1

    def test_accepted_values(self, stops):
        rule = {"type": "accepted_values", "column": "location_type",
                "params": {"values": [0, 1, 2]}}
        assert failing(stops, rule) == 1          # the type 3 row

    def test_expression(self, stops):
        rule = {"type": "expression",
                "params": {"expression": "stop_lon < -70"}}
        assert failing(stops, rule) == 0

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError):
            failure_condition({"type": "unique", "columns": ["stop_id"]})


class TestBatchEvaluation:
    """Row-level rules are evaluated in one pass. The results must match
    evaluating them one at a time."""

    def test_batch_matches_individual(self, stops):
        rules = [
            {"name": "name_not_null", "type": "not_null", "column": "stop_name"},
            {"name": "lat_in_region", "type": "range", "column": "stop_lat",
             "params": {"min": 41.0, "max": 43.5}},
        ]
        batch = evaluate_row_level_batch(stops, rules)
        for r in rules:
            assert batch[r["name"]] == evaluate(stops, r)

    def test_scoped_rules_are_grouped_separately(self, stops):
        """A rule with `where` only sees its own subset."""
        rules = [
            {"name": "coords_for_real_stops", "type": "not_null", "column": "stop_lat",
             "where": "location_type IN (0, 1, 2)"},
        ]
        checked, failed = evaluate_row_level_batch(stops, rules)["coords_for_real_stops"]
        assert (checked, failed) == (5, 0)   # the null-coord row is type 3, excluded


class TestSetLevelRules:

    def test_unique_counts_extra_rows(self, stops):
        assert evaluate(stops, {"type": "unique", "columns": ["stop_id"]}) == (6, 1)

    def test_referential_integrity(self, spark, stops):
        routes = spark.createDataFrame([("a",), ("b",)], "route_id string")
        rule = {"type": "referential_integrity", "column": "stop_id",
                "params": {"to": "routes", "to_column": "route_id"}}
        checked, failed = evaluate(stops, rule, {"routes": routes})
        assert (checked, failed) == (6, 3)    # c, d, e have no match

    def test_row_count_range(self, stops):
        assert evaluate(stops, {"type": "row_count_range",
                                "params": {"min": 1, "max": 10}})[1] == 0
        assert evaluate(stops, {"type": "row_count_range",
                                "params": {"min": 100}})[1] == 6


class TestVerdict:

    def test_zero_tolerance_by_default(self):
        assert verdict(100, 1, {})[0] is False
        assert verdict(100, 0, {})[0] is True

    def test_tolerance_allows_a_percentage(self):
        rule = {"params": {"max_failed_pct": 2.0}}
        assert verdict(1000, 19, rule)[0] is True     # 1.9%
        assert verdict(1000, 21, rule)[0] is False    # 2.1%

    def test_empty_input_passes(self):
        """No rows checked is not a failure - row_count_range covers that."""
        assert verdict(0, 0, {}) == (True, 0.0)

    def test_percentage_is_reported(self):
        assert verdict(200, 1, {})[1] == 0.5


def test_row_level_set_is_complete():
    """Every type failure_condition handles must be listed in ROW_LEVEL,
    or the batch evaluator will silently skip it."""
    for t in ROW_LEVEL:
        assert failure_condition({"type": t, "column": "x",
                                  "params": {"min": 0, "max": 1, "values": [1],
                                             "expression": "x > 0"}}) is not None
