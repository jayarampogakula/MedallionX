# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Source ➜ Landing (Parquet @ external location)

# COMMAND ----------
from pyspark.sql import functions as F
from utils.config_loader import load_config
from utils.ingestion_utils import read_jdbc_to_df, read_file_source_to_df, write_parquet
from utils.logger import log

dbutils.widgets.text("config_path", "./configs/sources/example_sources.json")
dbutils.widgets.text("batch_id", "")
config_path = dbutils.widgets.get("config_path")
batch_id = dbutils.widgets.get("batch_id") or F.current_timestamp().cast("string")

cfg = load_config(config_path)
sources = cfg.get("sources", [])

log("Ingestion start", sources=len(sources))

for s in sources:
    src_type = s["source_type"].lower()
    landing_path = s["landing_path"]  # external path like abfss://landing/...

    if src_type in ("sqlserver", "jdbc"):
        df = read_jdbc_to_df(s)
    elif src_type in ("files", "file", "autoloader"):
        df = read_file_source_to_df(s)
    else:
        raise ValueError(f"Unsupported source_type: {src_type}")

    df = df.withColumn("_ingestion_ts", F.current_timestamp()) \
           .withColumn("_batch_id", F.lit(batch_id))

    # Optional partitioning by date
    part_cols = s.get("partition_by")
    write_parquet(df, path=landing_path, mode=s.get("mode", "append"), partition_by=part_cols)

log("Ingestion done")
