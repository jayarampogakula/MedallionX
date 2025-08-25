from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from .logger import log

# ---------- Helpers
def _with_optional_secrets(conn: Dict[str, Any]) -> Dict[str, Any]:
    """
    If connection dict contains secret placeholders, resolve via dbutils.secrets.
    Example:
      "password": {"scope":"kv-scope","key":"sql-pass"}
    """
    resolved = {}
    for k, v in conn.items():
        if isinstance(v, dict) and "scope" in v and "key" in v:
            resolved[k] = dbutils.secrets.get(v["scope"], v["key"])
        else:
            resolved[k] = v
    return resolved

# ---------- JDBC (SQL Server, Oracle, Postgres, etc.)
def read_jdbc_to_df(source_cfg: Dict[str, Any]) -> DataFrame:
    conn = _with_optional_secrets(source_cfg["connection"])
    url = source_cfg.get("jdbc_url") or f"jdbc:sqlserver://{conn['server']};databaseName={conn['database']}"
    user = conn.get("user")
    password = conn.get("password")
    driver = source_cfg.get("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    table = source_cfg["table"]  # or use query

    reader = spark.read.format("jdbc") \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver)

    if "query" in source_cfg:
        reader = reader.option("query", source_cfg["query"])
    else:
        reader = reader.option("dbtable", table)

    if "fetchsize" in source_cfg:
        reader = reader.option("fetchsize", int(source_cfg["fetchsize"]))

    log("Reading via JDBC", url=url, table=source_cfg.get("table", "query"))
    return reader.load()

# ---------- File sources (S3/ADLS/GCS/DBFS)
def read_file_source_to_df(source_cfg: Dict[str, Any]) -> DataFrame:
    fmt = source_cfg.get("format", "parquet")
    path = source_cfg["path"]
    options = source_cfg.get("options", {})
    log("Reading files", fmt=fmt, path=path)

    reader = spark.read.format(fmt)
    for k, v in options.items():
        reader = reader.option(k, v)
    return reader.load(path)

# ---------- Write Parquet to Landing (External Location)
def write_parquet(df: DataFrame, path: str, mode: str = "append", partition_by: Optional[List[str]] = None):
    log("Writing Parquet to landing", path=path, mode=mode, partition_by=partition_by)
    writer = df.write.mode(mode).format("parquet")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(path)
