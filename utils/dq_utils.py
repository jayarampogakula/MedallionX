from typing import Dict, Any, List
from pyspark.sql import DataFrame, functions as F
from .logger import log

def _pct(n: int, d: int) -> float:
    return 0.0 if d == 0 else round(100.0 * n / d, 3)

def run_quality_checks(df: DataFrame,
                       checks: Dict[str, Any],
                       metrics_table: str = None,
                       fail_on_error: bool = True) -> Dict[str, Any]:
    """
    checks example:
    {
      "primary_key": ["customer_id"],
      "not_null": ["customer_id","email"],
      "unique": [["customer_id"]],
      "allowed_values": {"status": ["A","I"]},
      "ranges": {"age": {"min":0,"max":120}},
      "regex": {"email": "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"},
      "row_condition": {"expr":"OrderAmount>=0", "min_pass_pct": 99.0},
      "freshness": {"ts_col":"_event_ts", "max_lag_hours": 24}
    }
    """
    out = {}
    total = df.count()

    # not_null
    for c in checks.get("not_null", []):
        fails = df.filter(F.col(c).isNull()).count()
        out[f"not_null__{c}"] = {"failed": fails, "passed_pct": 100 - _pct(fails, total)}

    # unique
    for cols in checks.get("unique", []):
        dup = df.groupBy(*cols).count().filter("count>1").count()
        out[f"unique__{'_'.join(cols)}"] = {"duplicates": dup, "passed": dup == 0}

    # allowed_values
    for col, vals in checks.get("allowed_values", {}).items():
        bad = df.filter(~F.col(col).isin(vals)).count()
        out[f"allowed_values__{col}"] = {"failed": bad, "passed_pct": 100 - _pct(bad, total)}

    # ranges
    for col, spec in checks.get("ranges", {}).items():
        mi, ma = spec.get("min", None), spec.get("max", None)
        conds = []
        if mi is not None: conds.append(F.col(col) >= F.lit(mi))
        if ma is not None: conds.append(F.col(col) <= F.lit(ma))
        good = df.filter(F.reduce(lambda a,b: a & b, conds) if len(conds)>1 else conds[0]).count() if conds else total
        out[f"range__{col}"] = {"passed_pct": _pct(good, total)}

    # regex
    for col, pattern in checks.get("regex", {}).items():
        bad = df.filter(~F.col(col).rlike(pattern)).count()
        out[f"regex__{col}"] = {"failed": bad, "passed_pct": 100 - _pct(bad, total)}

    # row_condition
    rc = checks.get("row_condition")
    if rc:
        pass_rows = df.filter(F.expr(rc["expr"])).count()
        pass_pct = _pct(pass_rows, total)
        out["row_condition"] = {"passed_pct": pass_pct, "expr": rc["expr"], "min_pass_pct": rc.get("min_pass_pct", 100.0)}

    # freshness
    fr = checks.get("freshness")
    if fr:
        max_ts = df.agg(F.max(F.col(fr["ts_col"]))).first()[0]
        from datetime import datetime, timezone
        import pytz
        now = datetime.now(tz=timezone.utc)
        lag_hours = (now - max_ts.replace(tzinfo=timezone.utc)).total_seconds()/3600.0 if max_ts else 1e9
        out["freshness"] = {"lag_hours": round(lag_hours, 3), "max_lag_hours": fr.get("max_lag_hours", 24)}

    # write metrics
    if metrics_table:
        spark = df.sparkSession
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, MapType
        rows = [(k, str(v)) for k, v in out.items()]
        mdf = spark.createDataFrame(rows, ["check", "result_json"])
        (mdf.write.mode("append").format("delta").saveAsTable(metrics_table))

    # decide failure
    if fail_on_error:
        hard_fails = []
        for k, v in out.items():
            if "failed" in v and v["failed"] > 0:
                hard_fails.append(k)
            if "duplicates" in v and v["duplicates"] > 0:
                hard_fails.append(k)
            if k == "row_condition" and v["passed_pct"] < v.get("min_pass_pct", 100.0):
                hard_fails.append(k)
            if k == "freshness" and v["lag_hours"] > v.get("max_lag_hours", 24):
                hard_fails.append(k)
        if hard_fails:
            raise AssertionError(f"Data quality failures: {hard_fails}")

    log("DQ checks completed", checks=len(out))
    return out
