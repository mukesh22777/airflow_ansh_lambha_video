from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.config import build_spark_session, load_yaml_config, parse_common_args
from common.hive_ddl import create_silver_table
from common.logger import get_logger, log_exception
from common.validation import assert_expected_schema, assert_required_columns


logger = get_logger("silver_transform")


RELEVANT_COLUMNS = [
    "Age",
    "Attrition",
    "BusinessTravel",
    "DailyRate",
    "Department",
    "DistanceFromHome",
    "Education",
    "EducationField",
    "EmployeeCount",
    "EmployeeNumber",
    "EnvironmentSatisfaction",
    "Gender",
    "HourlyRate",
    "JobInvolvement",
    "JobLevel",
    "JobRole",
    "JobSatisfaction",
    "MaritalStatus",
    "MonthlyIncome",
    "MonthlyRate",
    "NumCompaniesWorked",
    "Over18",
    "OverTime",
    "PercentSalaryHike",
    "PerformanceRating",
    "RelationshipSatisfaction",
    "StandardHours",
    "StockOptionLevel",
    "TotalWorkingYears",
    "TrainingTimesLastYear",
    "WorkLifeBalance",
    "YearsAtCompany",
    "YearsInCurrentRole",
    "YearsSinceLastPromotion",
    "YearsWithCurrManager",
]


def transform_payload_to_columns(bronze_df: DataFrame) -> DataFrame:
    json_schema = (
        "Age INT, Attrition STRING, BusinessTravel STRING, DailyRate INT, Department STRING, "
        "DistanceFromHome INT, Education INT, EducationField STRING, EmployeeCount INT, "
        "EmployeeNumber INT, EnvironmentSatisfaction INT, Gender STRING, HourlyRate INT, "
        "JobInvolvement INT, JobLevel INT, JobRole STRING, JobSatisfaction INT, "
        "MaritalStatus STRING, MonthlyIncome INT, MonthlyRate INT, NumCompaniesWorked INT, "
        "Over18 STRING, OverTime STRING, PercentSalaryHike INT, PerformanceRating INT, "
        "RelationshipSatisfaction INT, StandardHours INT, StockOptionLevel INT, "
        "TotalWorkingYears INT, TrainingTimesLastYear INT, WorkLifeBalance INT, "
        "YearsAtCompany INT, YearsInCurrentRole INT, YearsSinceLastPromotion INT, "
        "YearsWithCurrManager INT, Load_date TIMESTAMP"
    )
    return bronze_df.withColumn("json", F.from_json("payload", json_schema)).select("json.*", "_run_id")


def process_table(spark, cfg, run_id: str) -> None:
    bronze_table = cfg["tables"]["bronze_table"]
    silver_table = cfg["tables"]["silver_table"]
    bronze_df = spark.table(bronze_table).filter(F.col("_run_id") == F.lit(run_id))
    if bronze_df.rdd.isEmpty():
        logger.info("No Bronze rows for run_id=%s in %s", run_id, bronze_table)
        return

    create_silver_table(spark, silver_table)
    raw_df = transform_payload_to_columns(bronze_df)
    assert_required_columns(raw_df, ["EmployeeNumber"], bronze_table)
    curated = raw_df.select(*RELEVANT_COLUMNS, "Load_date", "_run_id").dropDuplicates().fillna(0)
    assert_expected_schema(curated, RELEVANT_COLUMNS + ["Load_date", "_run_id"], silver_table)

    curated.write.mode("append").format("parquet").saveAsTable(silver_table)
    logger.info("Silver write complete for %s | rows=%s", silver_table, curated.count())


def main() -> None:
    args = parse_common_args()
    cfg = load_yaml_config(args.config)
    app_prefix = cfg.get("spark", {}).get("app_name_prefix", "medallion_etl")
    spark = build_spark_session(f"{app_prefix}_silver_transform", cfg)
    try:
        run_id = args.run_id
        process_table(spark, cfg, run_id)
    except Exception as exc:
        log_exception(logger, "Silver transform failed", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
