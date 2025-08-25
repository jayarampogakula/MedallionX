# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC ### Pipeline Runner
# MAGIC Orchestrate any subset: landing, bronze, silver, gold.

# COMMAND ----------
dbutils.widgets.dropdown("stage", "landing", ["landing","bronze","silver","gold","all"])
dbutils.widgets.text("landing_cfg", "./configs/sources/example_sources.json")
dbutils.widgets.text("bronze_cfg", "./configs/bronze/bronze_config.json")
dbutils.widgets.text("b2s_cfg", "./configs/transformations/bronze_to_silver.json")
dbutils.widgets.text("s2g_cfg", "./configs/transformations/silver_to_gold.json")
dbutils.widgets.text("catalog", "")

stage = dbutils.widgets.get("stage")
catalog = dbutils.widgets.get("catalog")

if stage in ("landing", "all"):
    dbutils.notebook.run("../ingestion/source_to_landing", 0, {"config_path": dbutils.widgets.get("landing_cfg")})
if stage in ("bronze", "all"):
    dbutils.notebook.run("../bronze/landing_to_bronze", 0, {"config_path": dbutils.widgets.get("bronze_cfg"),
                                                            "catalog": catalog})
if stage in ("silver", "all"):
    dbutils.notebook.run("../silver/bronze_to_silver", 0, {"config_path": dbutils.widgets.get("b2s_cfg"),
                                                           "catalog": catalog})
if stage in ("gold", "all"):
    dbutils.notebook.run("../gold/silver_to_gold", 0, {"config_path": dbutils.widgets.get("s2g_cfg"),
                                                       "catalog": catalog})
