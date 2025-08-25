from typing import Dict, Any, List, Union
from pyspark.sql import DataFrame, functions as F, types as T
from .logger import log

class TransformationEngine:
    """
    Generic, config-driven transformation engine.
    """
    def __init__(self):
        pass

    def apply(self, df_map: Dict[str, DataFrame], steps: List[Dict[str, Any]]) -> DataFrame:
        """
        df_map: mapping of name->DataFrame (for joins & multi-source ops)
        steps: list of operations
        Returns last produced DataFrame (or the single df in df_map if no steps)
        """
        current: DataFrame = None
        # choose default current
        if len(df_map) == 1:
            current = list(df_map.values())[0]

        for i, step in enumerate(steps):
            op = step["type"].lower()
            log(f"Apply step {i+1}: {op}")

            if op == "select":
                current = self._op_select(current, step)
            elif op == "rename":
                current = self._op_rename(current, step)
            elif op == "drop":
                current = current.drop(*step["columns"])
            elif op == "drop_nulls":
                current = current.dropna(subset=step.get("columns"))
            elif op == "cast":
                current = self._op_cast(current, step)
            elif op == "with_columns":
                current = self._op_with_columns(current, step)
            elif op == "deduplicate":
                current = self._op_deduplicate(current, step)
            elif op == "filter":
                current = current.filter(step["expr"])
            elif op == "join":
                current = self._op_join(df_map, current, step)
            elif op == "aggregate":
                current = self._op_aggregate(current, step)
            elif op == "sql_expr":
                current = current.selectExpr(*step["exprs"])
            else:
                raise ValueError(f"Unsupported transformation type: {op}")

        return current if current is not None else list(df_map.values())[0]

    # ---- ops ----
    def _op_select(self, df: DataFrame, step: Dict[str, Any]) -> DataFrame:
        cols = step["columns"]
        return df.select(*[F.col(c) for c in cols])

    def _op_rename(self, df: DataFrame, step: Dict[str, Any]) -> DataFrame:
        mapping = step["columns"]
        for old, new in mapping.items():
            df = df.withColumnRenamed(old, new)
        return df

    def _op_cast(self, df: DataFrame, step: Dict[str, Any]) -> DataFrame:
        mapping = step["columns"]
        for col, dtype in mapping.items():
            df = df.withColumn(col, F.col(col).cast(dtype))
        return df

    def _op_with_columns(self, df: DataFrame, step: Dict[str, Any]) -> DataFrame:
        exprs = step["exprs"]  # {"new_col": "case when ... end", ...}
        for col, sql in exprs.items():
            df = df.withColumn(col, F.expr(sql))
        return df

    def _op_deduplicate(self, df: DataFrame, step: Dict[str, Any]) -> DataFrame:
        keys = step["keys"]
        order_by = step.get("order_by")  # e.g., "ingestion_timestamp desc"
        if order_by:
            w = F.window  # not used here; simpler approach with row_number
            from pyspark.sql.window import Window
            spec = Window.partitionBy(*keys).orderBy(F.expr(order_by))
            return df.withColumn("_rn", F.row_number().over(spec)).filter("_rn=1").drop("_rn")
        return df.dropDuplicates(keys)

    def _op_join(self, df_map: Dict[str, DataFrame], current: DataFrame, step: Dict[str, Any]) -> DataFrame:
        left_name = step.get("left")
        right_name = step.get("right")
        how = step.get("how", "inner")
        on = step["on"]  # str or list

        left_df = current if left_name in (None, "", "current") else df_map[left_name]
        right_df = df_map[right_name]
        return left_df.join(right_df, on=on, how=how)

    def _op_aggregate(self, df: DataFrame, step: Dict[str, Any]) -> DataFrame:
        group_by = step.get("group_by", [])
        metrics = step["metrics"]  # {"alias":"agg_sql", ...}
        agg_exprs = [F.expr(f"{expr}").alias(alias) for alias, expr in metrics.items()]
        return df.groupBy(*[F.col(c) for c in group_by]).agg(*agg_exprs)

# ---------- IO helpers for Delta
def write_delta_table(df: DataFrame, full_name: str, mode: str = "append", partition_by: List[str] = None, zorder_by: List[str] = None):
    log("Writing Delta", table=full_name, mode=mode, partition_by=partition_by, zorder_by=zorder_by)
    (df.write
        .format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .saveAsTable(full_name))
    if zorder_by:
        spark.sql(f"OPTIMIZE {full_name} ZORDER BY ({', '.join(zorder_by)})")
