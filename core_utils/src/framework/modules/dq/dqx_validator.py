import json
import logging
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine, ExtraParams

from framework.core.environment import EnvironmentManager
from framework.modules.audit.audit_logger import AuditLogger
from framework.modules.io.writer import DeltaManager


class DataQualityCheck:
    """
    Perform DQX quality checks and manage exception + summary reporting.

    This class wraps the Databricks Labs DQX Engine to support:
        - Applying DQX quality rules
        - Splitting data into passed/quarantined sets
        - Persisting exception records
        - Aggregating and writing summary statistics

    Official DQX documentation:
        https://databrickslabs.github.io/dqx/docs/reference/quality_checks/

    Attributes:
        spark (SparkSession): Active SparkSession.
        config (dict): Entire pipeline config including DQX YAML block.
        env_manager (EnvironmentManager): Pipeline/environment configuration.
        audit_logger (AuditLogger): Audit logger instance.
        delta_manager (DeltaManager): Delta manager instance.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Dict,
        env_manager: EnvironmentManager,
        audit_logger: AuditLogger,
        delta_manager: DeltaManager,
    ) -> None:

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.spark = spark
        self.config = config
        self.env_manager = env_manager
        self.audit_logger = audit_logger
        self.delta_manager = delta_manager

        # Core table metadata
        self.table_fqn = env_manager.construct_table_fqn(
            config.get("destination", {}).get("table")
        )
        self.dq_check = config.get("data_quality", {})

        # Runtime arguments captured
        self.job_id = str(env_manager.args.databricks_job_id)
        self.run_id = str(env_manager.args.databricks_run_id)
        self.correlation_id = str(env_manager.args.correlation_id)

        # DQ table paths
        self.exception_table_fqn = env_manager.get_data_quality_exceptions_fqn()
        self.summary_table_fqn = env_manager.get_data_quality_summary_fqn()

    # ----------------------------------------------------------------------
    # Summaries
    # ----------------------------------------------------------------------
    def summarize_warn_errors(self, df: DataFrame, criticallity: str) -> DataFrame:
        """
        Aggregate DQX failures by criticallity.

        Args:
            df (DataFrame): DataFrame containing dq_errors/dq_warnings.
            criticallity (str): One of {"warn", "error"}.

        Returns:
            DataFrame: Aggregated rows normalized for summary table ingestion.
        """
        column_name = "dq_errors" if criticallity == "error" else "dq_warnings"

        self.logger.info(
            f"Summarizing {criticallity}-level DQX failures."
        )

        failed = (
            df.filter(F.col(column_name).isNotNull())
            .select(
                "correlation_id",
                F.explode(F.col(column_name)).alias("quality_check")
            )
        )

        aggregated = (
            failed.groupBy(
                "correlation_id",
                F.col("quality_check.Name").alias("Name"),
                F.col("quality_check.Message").alias("Message"),
            )
            .agg(F.count("*").alias("count"))
            .withColumn("quality_check_type", F.lit(criticallity))
            .withColumn("source_table_path", F.lit(self.table_fqn))
        )

        return aggregated

    # ----------------------------------------------------------------------
    # Combined Summary
    # ----------------------------------------------------------------------
    def combine_summary(
        self, df_warn: DataFrame, df_error: DataFrame
    ) -> DataFrame:
        """
        Combine warning and error summaries and enrich with metadata.

        Args:
            df_warn (DataFrame): Warning summary.
            df_error (DataFrame): Error summary.

        Returns:
            DataFrame: Final combined summary DataFrame.
        """
        self.logger.info(f"Combining summary statistics.")

        combined = df_warn.unionByName(df_error)
        dq_json_str = json.dumps(self.dq_check)

        return combined.select(
            "correlation_id",
            "quality_check_type",
            "Name",
            "Message",
            "count",
            F.lit(self.table_fqn).alias("source_table_path"),
            F.lit(self.job_id).alias("databricks_job_id"),
            F.lit(self.run_id).alias("databricks_run_id"),
            F.parse_json(F.lit(dq_json_str)).alias("dqx_checks_yml"),
            F.current_timestamp().alias("processed_time"),
        )

    # ----------------------------------------------------------------------
    # Exception Normalization
    # ----------------------------------------------------------------------
    def transform_for_exception_table(self, df: DataFrame) -> DataFrame:
        """
        Normalize a quarantined DataFrame for the exception table structure.

        Args:
            df (DataFrame): Quarantined DataFrame.

        Returns:
            DataFrame: DataFrame formatted for the exception table.
        """
        metadata_cols = ["correlation_id"]
        non_metadata_cols = [c for c in df.columns if c not in metadata_cols]

        return df.select(
            F.lit(self.table_fqn).alias("source_table_path"),
            F.lit(self.correlation_id).alias("correlation_id"),
            F.lit(self.job_id).alias("databricks_job_id"),
            F.lit(self.run_id).alias("databricks_run_id"),
            F.current_timestamp().alias("processed_time"),
            F.to_variant_object(F.struct(*non_metadata_cols)).alias("exception_record"),
        )

    # ----------------------------------------------------------------------
    # Apply Checks
    # ----------------------------------------------------------------------
    def apply_checks(self, df: DataFrame, quarantine: bool = False) -> DataFrame:
        """
        Apply DQX rules to the DataFrame and manage quarantine/summary generation.

        Workflow:
            1. Validate DQ rules.
            2. Apply DQ engine, producing passed and quarantined outputs.
            3. If quarantined rows exist:
                - Write exception records.
                - Build + write summary statistics.
            4. Return:
                - Only passed rows if quarantine=True
                - Original input if quarantine=False

        Args:
            df (DataFrame): Input dataset to validate.
            quarantine (bool): Whether to remove records with errors.

        Returns:
            DataFrame: Validated output dataset.
        """
        if not self.dq_check:
            self.logger.info("DQX checks not set — skipping DQX execution.")
            return df

        self.logger.info(f"Initializing DQEngine.")

        # Set up DQX engine with custom column names for errors and warnings
        extra_params = ExtraParams(
            column_names={"errors": "dq_errors", "warnings": "dq_warnings"}
        )
        dq_engine = DQEngine(WorkspaceClient(), extra_params=extra_params)

        self.logger.info("Validating DQX configuration.")
        status = DQEngine.validate_checks(self.dq_check)
        if status.has_errors:
            self.logger.error(f"DQX validation failed: '{status}'")
            raise ValueError("Invalid DQX configuration.")

        self.logger.info("Applying DQX rules to the input DataFrame.")
        # Split DataFrame into passed and quarantined sets based on DQX checks
        passed_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(
            df, self.dq_check
        )

        if quarantine_df.isEmpty():
            self.logger.info("No DQ failures detected — skipping exception & summary writes.")
        else:
            # Write quarantined rows to exception table
            self.logger.info(
                f"Writing quarantined rows to exception table '{self.exception_table_fqn}'."
            )
            exception_df = self.transform_for_exception_table(quarantine_df)
            self.delta_manager.write_df(exception_df, self.exception_table_fqn)

            # Build and write summary statistics for DQ failures
            self.logger.info("Building summary statistics.")
            warn_df = self.summarize_warn_errors(quarantine_df, criticallity="warn")
            error_df = self.summarize_warn_errors(quarantine_df, criticallity="error")
            summary_df = self.combine_summary(warn_df, error_df)

            self.logger.info(f"Writing summary to '{self.summary_table_fqn}'.")
            self.delta_manager.write_df(summary_df, self.summary_table_fqn)

        self.logger.info(
            f"Returning {'only passed' if quarantine else 'all input'} records."
        )

        # Return only passed rows if quarantine=True, else return original input
        return passed_df if quarantine else df