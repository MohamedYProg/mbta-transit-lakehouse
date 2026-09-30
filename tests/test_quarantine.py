"""Quarantine.

The claim being tested is the one the pipeline asserts on every run:
no row is ever silently dropped. valid + rejected == input, always.
"""
import json

import pytest
from pyspark.sql import functions as F

from src.quality.quarantine import reconcile, reject_reasons, split, to_rejects

RULES = [
    {"name": "lat_in_region", "type": "range", "column": "latitude",
     "params": {"min": 41.0, "max": 43.5}},
    {"name": "lon_in_region", "type": "range", "column": "longitude",
     "params": {"min": -72.5, "max": -69.5}},
    {"name": "occupancy_plausible", "type": "range", "column": "occupancy_pct",
     "params": {"min": 0, "max": 400}},
]


@pytest.fixture
def positions(spark):
    return spark.createDataFrame(
        [("v1", 100, 42.35, -71.06, 40),      # fine
         ("v2", 200, 40.71, -74.00, 10),      # Manhattan: both coords out
         ("v3", 300, 41.75, -72.70, 20),      # Hartford: longitude only
         ("v4", 400, 42.35, -71.06, 900),     # impossible occupancy
         ("v5", 500, None,  None,   None)],   # nulls are not failures
        "vehicle_id string, vehicle_ts long, latitude double, "
        "longitude double, occupancy_pct int")


class TestSplit:

    def test_valid_and_rejected_counts(self, positions):
        valid, rejected = split(positions, RULES)
        assert valid.count() == 2        # v1 and v5
        assert rejected.count() == 3

    def test_nothing_is_lost(self, positions):
        valid, rejected = split(positions, RULES)
        reconcile(positions.count(), valid.count(), rejected.count(), "positions")

    def test_valid_rows_have_no_helper_column(self, positions):
        valid, _ = split(positions, RULES)
        assert "_reject_reasons" not in valid.columns

    def test_nulls_are_not_rejected(self, positions):
        """A missing value is not an invalid value. not_null would be a
        separate, explicit rule."""
        valid, _ = split(positions, RULES)
        assert "v5" in [r.vehicle_id for r in valid.collect()]

    def test_no_rules_rejects_nothing(self, positions):
        valid, rejected = split(positions, [])
        assert (valid.count(), rejected.count()) == (5, 0)


class TestRejectReasons:

    def reasons(self, df):
        return {r.vehicle_id: r._reject_reasons
                for r in reject_reasons(df, RULES).collect()}

    def test_a_row_can_fail_several_rules(self, positions):
        assert set(self.reasons(positions)["v2"]) == {"lat_in_region", "lon_in_region"}

    def test_a_row_can_fail_exactly_one(self, positions):
        assert self.reasons(positions)["v3"] == ["lon_in_region"]

    def test_valid_rows_get_an_empty_array(self, positions):
        assert self.reasons(positions)["v1"] == []


class TestToRejects:

    def shaped(self, positions):
        _, rejected = split(positions, RULES)
        return to_rejects(rejected, "silver.vehicle_positions", "silver",
                          ["vehicle_id", "vehicle_ts"], "run123")

    def test_shape(self, positions):
        out = self.shaped(positions)
        assert set(out.columns) == {"run_id", "rejected_at", "source_table", "layer",
                                    "natural_key", "reject_reasons", "reject_reason",
                                    "row_json"}
        assert out.count() == 3

    def test_natural_key_is_json(self, positions):
        keys = [json.loads(r.natural_key) for r in self.shaped(positions).collect()]
        assert {"vehicle_id": "v2", "vehicle_ts": 200} in keys

    def test_row_json_round_trips(self, positions):
        """The full original row is kept, so a reject can be reprocessed
        without going back to bronze."""
        row = [r for r in self.shaped(positions).collect()
               if json.loads(r.natural_key)["vehicle_id"] == "v4"][0]
        restored = json.loads(row.row_json)
        assert restored["occupancy_pct"] == 900
        assert restored["latitude"] == 42.35

    def test_reason_string_lists_every_rule(self, positions):
        row = [r for r in self.shaped(positions).collect()
               if json.loads(r.natural_key)["vehicle_id"] == "v2"][0]
        assert set(row.reject_reason.split(",")) == {"lat_in_region", "lon_in_region"}

    def test_helper_column_is_not_in_the_payload(self, positions):
        row = self.shaped(positions).first()
        assert "_reject_reasons" not in json.loads(row.row_json)


class TestReconcile:

    def test_passes_when_the_numbers_add_up(self):
        reconcile(100, 97, 3)

    def test_raises_when_a_row_vanishes(self):
        with pytest.raises(AssertionError):
            reconcile(100, 96, 3)

    def test_raises_when_a_row_is_duplicated(self):
        with pytest.raises(AssertionError):
            reconcile(100, 98, 3)
