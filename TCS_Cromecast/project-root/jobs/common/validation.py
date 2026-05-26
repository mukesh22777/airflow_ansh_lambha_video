from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assert_required_columns(df: DataFrame, required: Iterable[str], table_name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {table_name}: {missing}")


def assert_non_empty(df: DataFrame, table_name: str) -> None:
    if df.rdd.isEmpty():
        raise ValueError(f"DataFrame for {table_name} is empty")


def assert_no_nulls(df: DataFrame, columns: Iterable[str], table_name: str) -> None:
    predicates = [F.col(c).isNull() for c in columns]
    if not predicates:
        return
    null_df = df.filter(predicates[0])
    for p in predicates[1:]:
        null_df = null_df.union(df.filter(p))
    if not null_df.rdd.isEmpty():
        raise ValueError(f"Nulls detected in {table_name} for columns: {list(columns)}")


def assert_no_duplicate_keys(df: DataFrame, key_cols: Iterable[str], table_name: str) -> None:
    key_cols = list(key_cols)
    dup_count = (
        df.groupBy(*key_cols)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if dup_count > 0:
        raise ValueError(f"Duplicate keys detected in {table_name} for keys: {key_cols}")
