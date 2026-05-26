from pathlib import Path

from jobs.common.logger import get_logger

LOGGER = get_logger(__name__)


def apply_hive_ddl(spark, ddl_path: str, database: str) -> None:
    sql_text = Path(ddl_path).read_text(encoding="utf-8")
    sql_text = sql_text.replace("{{database}}", database)
    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]
    for stmt in statements:
        LOGGER.info("Executing DDL statement for database '%s'", database)
        spark.sql(stmt)
