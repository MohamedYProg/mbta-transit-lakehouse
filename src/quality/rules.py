"""Config-driven data quality rules. Pure functions, no I/O."""
import time
from datetime import timezone
from pyspark.sql import Column, DataFrame, functions as F

ROW_LEVEL = {"not_null", "range", "accepted_values", "expression"}


def failure_condition(rule: dict) -> Column:
    """A Column that is True for rows that FAIL this rule.
    Row-level rules only. Story 3.2 reuses these to split valid from rejected."""
    t, col = rule["type"], rule.get("column")
    p = rule.get("params") or {}
    if t == "not_null":
        return F.col(col).isNull()
    if t == "range":
        lo, hi = p.get("min"), p.get("max")
        c = F.col(col)
        bad = F.lit(False)
        if lo is not None:
            bad = bad | (c < F.lit(lo))
        if hi is not None:
            bad = bad | (c > F.lit(hi))
        return c.isNotNull() & bad
    if t == "accepted_values":
        return F.col(col).isNotNull() & ~F.col(col).isin(p["values"])
    if t == "expression":
        # rows where the predicate is NULL are not counted as failures
        return F.expr(f"NOT ({p['expression']})")
    raise ValueError(f"{t} is not a row-level rule")


def evaluate_row_level_batch(df: DataFrame, rules: list) -> dict:
    """Every row-level rule sharing a scope, in one aggregate pass.
    Returns {rule_name: (rows_checked, rows_failed)}."""
    groups = {}
    for r in rules:
        groups.setdefault(r.get("where") or "", []).append(r)

    out = {}
    for where, rs in groups.items():
        scoped = df.filter(where) if where else df
        aggs = [F.count(F.lit(1)).alias("__total")] + [
            F.sum(failure_condition(r).cast("int")).alias(r["name"]) for r in rs]
        row = scoped.agg(*aggs).first()
        for r in rs:
            out[r["name"]] = (int(row["__total"]), int(row[r["name"]] or 0))
    return out


def evaluate(df: DataFrame, rule: dict, lookups: dict = None) -> tuple:
    """Returns (rows_checked, rows_failed) for a single rule of any type."""
    t = rule["type"]
    p = rule.get("params") or {}
    scoped = df.filter(rule["where"]) if rule.get("where") else df

    if t in ROW_LEVEL:
        total = scoped.count()
        return total, scoped.filter(failure_condition(rule)).count()

    if t == "unique":
        cols = rule["columns"]
        total = scoped.count()
        return total, total - scoped.select(*cols).distinct().count()

    if t == "referential_integrity":
        lk = (lookups or {})[p["to"]]
        col, to_col = rule["column"], p["to_column"]
        src = scoped.filter(F.col(col).isNotNull())
        right = lk.select(F.col(to_col).alias(col)).distinct()
        return src.count(), src.join(right, col, "left_anti").count()

    if t == "row_count_range":
        total = scoped.count()
        lo, hi = p.get("min"), p.get("max")
        outside = (lo is not None and total < lo) or (hi is not None and total > hi)
        return total, total if outside else 0

    if t == "freshness":
        total = scoped.count()
        mx = scoped.agg(F.max(rule["column"])).first()[0]
        if mx is None:
            return total, 1
        epoch = mx if isinstance(mx, (int, float)) else mx.replace(tzinfo=timezone.utc).timestamp()
        return total, 1 if (time.time() - epoch) > p["max_age_seconds"] else 0

    raise ValueError(f"unknown rule type: {t}")


def verdict(rows_checked: int, rows_failed: int, rule: dict) -> tuple:
    """Returns (passed, failed_pct). A rule may tolerate a percentage."""
    pct = 0.0 if rows_checked == 0 else 100.0 * rows_failed / rows_checked
    tolerance = float((rule.get("params") or {}).get("max_failed_pct", 0))
    return pct <= tolerance, round(pct, 4)