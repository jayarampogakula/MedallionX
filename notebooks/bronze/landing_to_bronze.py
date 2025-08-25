# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Landing ➜ Bronze (Delta)

# COMMAND ----------
from pyspark.sql import functions as F
from utils.config_loader import load_config
from utils.transformation_utils import write_delta_table
from utils.logger import log

dbutils.widgets.text("config_path", "./configs/bronze/bronze_config.json")
dbutils.widgets.text("catalog", "")    # e.g., main
dbutils.widgets.text("schema", "bronze")

cfg_path = dbutils.widgets.get("config_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

cfg = load_config(cfg_path)
datasets = cfg["datasets"]

if catalog:
    spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE {schema}")

for ds in datasets:
    name = ds["name"]
    landing_path = ds["landing_path"]
    bronze_table = ds["bronze_table"]  # e.g., bronze.customers or <schema>.<table>

    log("Processing landing->bronze", dataset=name)

    df = spark.read.format("parquet").load(landing_path)
    df = (df
        .withColumn("_bronze_ingestion_ts", F.current_timestamp())
        .withColumn("_source_path", F.input_file_name()))

    full_table = bronze_table if "." in bronze_table else f"{schema}.{bronze_table}"
    write_delta_table(df, full_table, mode="append")

log("Landing->Bronze done")
