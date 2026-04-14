import logging
from pyspark.sql import SparkSession
from .delta_log_handler import DeltaTableHandler


def setup_pipeline_logging(
    spark: SparkSession,
    table_name: str,
    args,
    level: int = logging.INFO,
) -> DeltaTableHandler:
    """
    Configure the root logger with both a StreamHandler (console)
    and a DeltaTableHandler (Delta table).

    Returns the DeltaTableHandler so the caller can call
    handler.flush() in a finally block.
    """
    root = logging.getLogger()
    root.setLevel(level)

    # Console handler (so logs still show in Databricks driver output)
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        console = logging.StreamHandler()
        console.setLevel(level)
        fmt = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s:%(funcName)s() - %(message)s"
        )
        console.setFormatter(fmt)
        root.addHandler(console)

    # Delta table handler
    delta_handler = DeltaTableHandler(
        spark=spark, table_name=table_name,
        run_id=getattr(args, "databricks_run_id", None), correlation_id=getattr(args, "correlation_id", None), level=level,
    )
    delta_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s:%(funcName)s() - %(message)s"))
    root.addHandler(delta_handler)

    return delta_handler