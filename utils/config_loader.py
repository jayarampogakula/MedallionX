# Databricks notebook / Repo file
from typing import Any, Dict
import json
import os

def load_config(path: str) -> Dict[str, Any]:
    """
    Load JSON config from DBFS/Workspace/Repo relative paths.
    Supports: /Workspace/..., /Volumes/..., /dbfs/..., repo-relative (./configs/...)
    """
    if path.startswith("/dbfs/"):
        with open(path.replace("/dbfs/", "/dbfs/"), "r") as f:
            return json.load(f)
    elif path.startswith("/Workspace/") or path.startswith("/Volumes/"):
        # Use DBUtils to read file content
        data = dbutils.fs.head(path, 10_000_000)  # up to ~10MB
        return json.loads(data)
    else:
        # repo-relative path
        abs_path = path if os.path.isabs(path) else f"/Workspace/{path.lstrip('./')}"
        try:
            data = dbutils.fs.head(abs_path, 10_000_000)
            return json.loads(data)
        except Exception:
            # last resort: local open (when running as repo file in cluster mode)
            with open(path, "r") as f:
                return json.load(f)
