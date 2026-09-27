"""Split rows on quality rules: valid forward, invalid to ops.rejects."""
from pyspark.sql import DataFrame, functions as F
from src.quality.rules import failure_condition

REJECTS = "transit.ops.rejects"


def reject_reasons(df: DataFrame, rules: list) -> DataFrame:
    """Adds _reject_reasons: array of names of the rules each row fails.
    Empty array = the row is valid. Rules are evaluated in one pass."""
    flags = [F.when(failure_condition(r), F.lit(r["name"])) for r in rules]
    return df.withColumn("_reject_reasons", F.array_compact(F.array(*flags)))


def split(df: DataFrame, rules: list) -> tuple:
    """Returns (valid, rejected). Rejected keeps _reject_reasons."""
    tagged = reject_reasons(df, rules)
    valid = tagged.filter(F.size("_reject_reasons") == 0).drop("_reject_reasons")
    rejected = tagged.filter(F.size("_reject_reasons") > 0)
    return valid, rejected


def to_rejects(rejected: DataFrame, source_table: str, layer: str,
               key_cols: list, run_id: str) -> DataFrame:
    """Shape rejected rows for ops.rejects. The whole row is kept as JSON so it
    can be inspected and reprocessed without going back to bronze."""
    payload = F.to_json(F.struct(*[c for c in rejected.columns
                                   if c != "_reject_reasons"]))
    return rejected.select(
        F.lit(run_id).alias("run_id"),
        F.current_timestamp().alias("rejected_at"),
        F.lit(source_table).alias("source_table"),
        F.lit(layer).alias("layer"),
        F.to_json(F.struct(*[F.col(c) for c in key_cols])).alias("natural_key"),
        F.col("_reject_reasons").alias("reject_reasons"),
        F.array_join("_reject_reasons", ",").alias("reject_reason"),
        payload.alias("row_json"))


def reconcile(n_input: int, n_valid: int, n_rejected: int, label: str = "") -> None:
    """valid + rejected must equal input. No row may vanish."""
    assert n_valid + n_rejected == n_input, (
        f"{label}: {n_valid} valid + {n_rejected} rejected != {n_input} input")
    
def write_rejects(spark, shaped):
    """Upsert rejected rows keyed on (source_table, natural_key).
    Re-rejecting the same row updates it instead of adding a duplicate."""
    shaped.createOrReplaceTempView("_rejects_src")
    spark.sql(f"""
      MERGE INTO {REJECTS} t
      USING (
        SELECT source_table, natural_key,
               max_by(run_id, rejected_at)         AS run_id,
               min(rejected_at)                    AS first_rejected_at,
               max(rejected_at)                    AS last_rejected_at,
               max_by(layer, rejected_at)          AS layer,
               max_by(reject_reasons, rejected_at) AS reject_reasons,
               max_by(reject_reason, rejected_at)  AS reject_reason,
               max_by(row_json, rejected_at)       AS row_json
        FROM _rejects_src GROUP BY source_table, natural_key) s
      ON t.source_table = s.source_table AND t.natural_key = s.natural_key
      WHEN MATCHED THEN UPDATE SET
        t.run_id = s.run_id, t.last_rejected_at = s.last_rejected_at,
        t.layer = s.layer, t.reject_reasons = s.reject_reasons,
        t.reject_reason = s.reject_reason, t.row_json = s.row_json
      WHEN NOT MATCHED THEN INSERT
        (run_id, first_rejected_at, last_rejected_at, source_table, layer,
         natural_key, reject_reasons, reject_reason, row_json)
      VALUES
        (s.run_id, s.first_rejected_at, s.last_rejected_at, s.source_table, s.layer,
         s.natural_key, s.reject_reasons, s.reject_reason, s.row_json)
    """)