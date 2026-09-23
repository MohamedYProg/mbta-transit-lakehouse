"""Shared silver-layer transforms."""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def clean_strings(df: DataFrame) -> DataFrame:
    """Trim every source string column; empty or whitespace-only becomes null.
    Metadata columns (leading underscore) are left alone."""
    out = []
    for f in df.schema.fields:
        if f.name.startswith("_") or f.dataType.simpleString() != "string":
            out.append(F.col(f.name))
        else:
            t = F.trim(F.col(f.name))
            out.append(F.when(t == "", None).otherwise(t).alias(f.name))
    return df.select(out)


def cast_columns(df: DataFrame, spec: dict):
    """Cast per {column: SQL type} with try_cast. Returns (df, failures, missing).
    A failure = the source had a value but the cast produced null."""
    present = {c: t for c, t in spec.items() if c in df.columns}
    missing = sorted(set(spec) - set(present))
    casted = df.select("*", *[F.expr(f"try_cast(`{c}` AS {t})").alias(f"__c_{c}")
                              for c, t in present.items()])
    if present:
        row = casted.select([
            F.sum((F.col(c).isNotNull() & F.col(f"__c_{c}").isNull()).cast("int")).alias(c)
            for c in present]).collect()[0].asDict()
    else:
        row = {}
    out = casted.select([F.col(f"__c_{c}").alias(c) if c in present else F.col(c)
                         for c in df.columns])
    return out, {c: int(v or 0) for c, v in row.items()}, missing


def guard_unique(df: DataFrame, keys: list, order_col: str = "_ingested_at"):
    """Keep one row per key, latest by order_col. Returns (df, rows_removed)."""
    w = Window.partitionBy(*keys).orderBy(F.desc(order_col))
    out = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return out, df.count() - out.count()