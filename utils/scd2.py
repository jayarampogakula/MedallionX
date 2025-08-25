from typing import List, Dict, Any
from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable
from .logger import log

def _ensure_audit_cols(df: DataFrame,
                       ts_col: str,
                       eff_from: str,
                       eff_to: str,
                       current_col: str) -> DataFrame:
    return (df
            .withColumn(ts_col, F.col(ts_col))
            .withColumn(eff_from, F.col(ts_col))
            .withColumn(eff_to, F.lit(None).cast("timestamp"))
            .withColumn(current_col, F.lit(True)))

def scd2_merge(source_df: DataFrame,
               target_table: str,
               natural_keys: List[str],
               compare_cols: List[str],
               ts_col: str = "_event_ts",
               effective_from_col: str = "effective_from",
               effective_to_col: str = "effective_to",
               current_flag_col: str = "is_current",
               hard_delete: bool = False,
               delete_flag_col: str = "_is_deleted") -> Dict[str, Any]:
    """
    Implements SCD Type 2 with Delta MERGE:
      - Close out changed rows (set current=false, effective_to=ts)
      - Insert new versions for changed rows
      - Insert brand new keys
      - Optional hard deletes if delete_flag_col = True on source
    Assumes target table exists or will be created by DeltaTable.forName.
    """
    spark = source_df.sparkSession
    if not spark.catalog.tableExists(target_table):
        # First load is full insert (all as current)
        log("SCD2 init load -> creating table", table=target_table)
        init = _ensure_audit_cols(source_df, ts_col, effective_from_col, effective_to_col, current_flag_col)
        (init.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table))
        return {"inserted": init.count(), "updated": 0, "closed": 0, "deleted": 0}

    tgt = DeltaTable.forName(spark, target_table)

    # Hash for change detection
    src = source_df.withColumn("_chg_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in compare_cols]), 256))
    # current target only
    tgt_df = spark.table(target_table).filter(F.col(current_flag_col) == True) \
        .withColumn("_chg_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in compare_cols]), 256))

    # 1) Close out rows that changed
    cond = " AND ".join([f"t.{k} <=> s.{k}" for k in natural_keys])
    changed = (tgt_df.alias("t")
                  .join(src.alias("s"), on=[F.col(f"t.{k}") == F.col(f"s.{k}") for k in natural_keys], how="inner")
                  .where(F.col("t._chg_hash") != F.col("s._chg_hash")))

    updates = (tgt.alias("t").merge(
        changed.select(*[F.col(f"s.{c}").alias(c) for c in natural_keys + [ts_col]]),
        cond
    ).whenMatchedUpdate(
        condition=f"t.{current_flag_col} = true AND t.{effective_to_col} IS NULL",
        set={current_flag_col: "false", effective_to_col: f"s.{ts_col}"}
    ).execute)

    # 2) Inserts for NEW keys or CHANGED rows (new version)
    #   Left anti join current target to find new keys or changed hashes
    to_insert = (src.alias("s")
                   .join(tgt_df.alias("t"),
                         on=[F.col(f"s.{k}") == F.col(f"t.{k}") for k in natural_keys],
                         how="left")
                   .where(F.col("t._chg_hash").isNull() | (F.col("t._chg_hash") != F.col("s._chg_hash")))) \
                   .drop("t._chg_hash")

    to_insert = _ensure_audit_cols(to_insert, ts_col, effective_from_col, effective_to_col, current_flag_col)

    inserts = (tgt.alias("t")
                 .merge(to_insert.alias("s"), cond)
                 .whenNotMatchedInsert(values={c: f"s.{c}" for c in to_insert.columns})
                 .execute)

    deleted = 0
    if hard_delete and delete_flag_col in source_df.columns:
        # Hard delete keys flagged deleted in source (close out current + optionally delete all history)
        del_src = source_df.filter(F.col(delete_flag_col) == True).select(*natural_keys).dropDuplicates()
        tgt.alias("t").delete(f"""t.{current_flag_col} = true AND EXISTS (
            SELECT 1 FROM (SELECT {', '.join(['x.'+k for k in natural_keys])} FROM VALUES {',' .join(['(NULL)'])} ) as x
        )""")  # placeholder - we instead do a join delete because Delta SQL DELETE ... USING isn't in Python API
        # Use SQL for clean hard delete of current rows; keep history
        keys_cond = " OR ".join(["(" + " AND ".join([f"t.{k} = d.{k}" for k in natural_keys]) + ")" ])
        del_src.createOrReplaceTempView("_scd2_del")
        spark.sql(f"DELETE FROM {target_table} t WHERE t.{current_flag_col}=true AND EXISTS (SELECT 1 FROM _scd2_del d WHERE " +
                  " AND ".join([f"t.{k} = d.{k}" for k in natural_keys]) + ")")
        deleted = del_src.count()

    return {"inserted": 0, "updated": 0, "closed": 0, "deleted": deleted}  # merges don't return counts in API
