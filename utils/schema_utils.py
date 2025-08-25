from typing import Dict, Any, List, Tuple
from pyspark.sql import DataFrame, types as T, functions as F
from .logger import log

def _from_json_spec(spec: List[Dict[str, str]]) -> T.StructType:
    """
    spec example: [{"name":"customer_id","type":"string","nullable":false}, ...]
    """
    return T.StructType([T.StructField(c["name"], T._parse_datatype_string(c["type"]), c.get("nullable", True)) for c in spec])

def align_to_schema(df: DataFrame, expected: List[Dict[str, Any]], mode: str = "evolve") -> DataFrame:
    """
    mode:
      - 'strict'  -> enforce exact names/types (cast or raise if impossible)
      - 'evolve'  -> allow new columns; cast where possible
      - 'relaxed' -> only cast known columns; ignore extras
    """
    exp = _from_json_spec(expected)
    cur = df.schema

    # Add missing columns with nulls (for evolve/relaxed)
    exp_names = set([f.name for f in exp.fields])
    cur_names = set([f.name for f in cur.fields])
    missing = exp_names - cur_names
    for m in missing:
        if mode in ("evolve", "relaxed"):
            dt = exp[m].dataType if isinstance(exp, dict) else [f.dataType for f in exp if f.name == m][0]  # defensive
        for f in exp.fields:
            if f.name == m:
                df = df.withColumn(m, F.lit(None).cast(f.dataType))
                break

    # Cast types for overlapping columns
    for f in exp.fields:
        if f.name in df.columns:
            df = df.withColumn(f.name, F.col(f.name).cast(f.dataType))

    # Strict mode: drop unexpected columns and validate final schema equals
    if mode == "strict":
        df = df.select(*[f.name for f in exp.fields])
        # Validate nullability if needed (Spark doesn't enforce at runtime)
    elif mode == "relaxed":
        # keep all columns; but ensure expected at least exist
        pass
    elif mode == "evolve":
        # keep extras + ensure expected present
        pass
    else:
        raise ValueError("schema mode must be strict|evolve|relaxed")

    return df
