from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_non_empty(df: DataFrame, source_name: str):
    if df.rdd.isEmpty():
        raise ValueError(f"Input dataset is empty for source={source_name}")


def select_columns(df: DataFrame, columns):
    return df.select(*columns)


def clean_dataframe(df: DataFrame, subset_for_dedup):
    return df.dropDuplicates(subset_for_dedup).fillna(0)


def add_load_date(df: DataFrame):
    return df.withColumn("Load_date", F.current_date())