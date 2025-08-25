# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Bronze ➜ Silver (config-driven transformations)

# COMMAND ----------
from utils.config_loader import load_config
from utils.transformation_utils import TransformationEngine, write_delta_table
from utils.logger import log
from utils.scd2 import scd2_merge

dbutils.widgets.text("config_path", "./configs/transformations/bronze_to_silver.json")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("source_schema", "bronze")
dbutils.widgets.text("target_schema", "silver")

cfg_path = dbutils.widgets.get("config_path")
catalog = dbutils.widgets.get("catalog")
source_schema = dbutils.widgets.get("source_schema")
target_schema = dbutils.widgets.get("target_schema")

cfg = load_config(cfg_path)
datasets = cfg["datasets"]

if catalog:
    spark.sql(f"USE CATALOG {catalog}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
spark.sql(f"USE {target_schema}")

engine = TransformationEngine()

for ds in datasets:
    source = ds["source"]          # str like "bronze.customers"
    target = ds["target"]          # str like "silver.customers"
    steps = ds.get("transformations", [])

    # Load source df(s)
    df_map = {}
    if isinstance(source, str):
        tbl = source if "." in source else f"{source_schema}.{source}"
        df_map["current"] = spark.table(tbl)
    else:
        for s in source:
            tbl = s if "." in s else f"{source_schema}.{s}"
            df_map[s] = spark.table(tbl)

    log("Transforming bronze->silver", target=target)

    result_df = engine.apply(df_map, steps)
    full_target = target if "." in target else f"{target_schema}.{target}"
    
    schema_policy = ds.get("schema_policy")         # {"mode":"strict|evolve|relaxed","expected_schema":[...]}
    dq_checks     = ds.get("quality_checks")        # see examples below
    dq_metrics_tbl= ds.get("dq_metrics_table")      # e.g., "ops.dq_metrics"
    
    scd2_cfg = ds.get("scd2")  # optional: {"natural_keys":[...], "compare_cols":[...], "ts_col":"_event_ts", ...}
    
    if scd2_cfg:
        # Run DQ/schema before SCD2 so we don't land bad records
        if schema_policy or dq_checks:
            result_df = align_to_schema(result_df, schema_policy["expected_schema"], schema_policy.get("mode","evolve")) if schema_policy else result_df
            if dq_checks:
                run_quality_checks(result_df, dq_checks, metrics_table=dq_metrics_tbl, fail_on_error=dq_checks.get("fail_on_error", True))
    
        metrics = scd2_merge(
            source_df=result_df,
            target_table=full_target,
            natural_keys=scd2_cfg["natural_keys"],
            compare_cols=scd2_cfg["compare_cols"],
            ts_col=scd2_cfg.get("ts_col", "_event_ts"),
            effective_from_col=scd2_cfg.get("effective_from_col","effective_from"),
            effective_to_col=scd2_cfg.get("effective_to_col","effective_to"),
            current_flag_col=scd2_cfg.get("current_flag_col","is_current"),
            hard_delete=scd2_cfg.get("hard_delete", False),
            delete_flag_col=scd2_cfg.get("delete_flag_col","_is_deleted")
        )
        log("SCD2 merge done", **metrics)
    else:
        write_delta_table(result_df, full_target,
                          mode="overwrite",
                          schema_policy=schema_policy,
                          dq_checks=dq_checks,
                          dq_metrics_table=dq_metrics_tbl)


log("Bronze->Silver done")
