# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Silver ➜ Gold (config-driven transformations)

# COMMAND ----------
from utils.config_loader import load_config
from utils.transformation_utils import TransformationEngine, write_delta_table
from utils.logger import log

dbutils.widgets.text("config_path", "./configs/transformations/silver_to_gold.json")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("source_schema", "silver")
dbutils.widgets.text("target_schema", "gold")

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
    source = ds["source"]          # str or list[str]
    target = ds["target"]
    steps = ds.get("transformations", [])
    zorder = ds.get("zorder_by")

    # load inputs
    df_map = {}
    if isinstance(source, str):
        tbl = source if "." in source else f"{source_schema}.{source}"
        df_map["current"] = spark.table(tbl)
    else:
        for s in source:
            tbl = s if "." in s else f"{source_schema}.{s}"
            df_map[s] = spark.table(tbl)

    log("Transforming silver->gold", target=target)
    result_df = engine.apply(df_map, steps)

    full_target = target if "." in target else f"{target_schema}.{target}"
    write_delta_table(result_df, full_target, mode="overwrite", zorder_by=zorder)

log("Silver->Gold done")
