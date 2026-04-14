from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any
from framework.core.base import MedallionBase
from framework.modules.utils.transformations import TransformationUtils
from framework.modules.io.reader import BaseFileIngester, CSVFileIngester, EBCDICFileIngester, DeltaTableIngester
from framework.modules.io.writer import DeltaManager
from framework.modules.audit.audit_logger import AuditLogger
from framework.modules.security.masker import Masker
from framework.modules.utils.common import to_bool
from pyspark.sql import functions as F

class BronzeIngester(MedallionBase):
    """
    Bronze Layer: Ingests raw data from various sources.
    """

    def __init__(self, args):
        """
        Initialize BronzeIngester with configs for pipeline, source, transform, and destination.
        """
        super().__init__(args)

        # Load pipeline, source, transform, and destination configs
        self.pipeline_config = self.config.get('pipeline_metadata', {})
        self.source_config = self.config.get('source', {})
        self.transform_config = self.config.get('transform', {})
        self.destination_config = self.config.get('destination', {})

        self.files_discovered = []
    

    def transform(self, df: DataFrame) -> DataFrame:
        """ Apply Minimal transformations for bronze layer"""
        self.logger.info("Applying bronze transformations")

        # 1. Add row_hash
        df = TransformationUtils.add_row_hash(df)

        # 2. Add new columns
        df = TransformationUtils.add_custom_columns(df, self.transform_config)

        # 3. Add metadata columns
        df = TransformationUtils.add_metadata_columns(df, self.args)

        # 4. Apply standardization (UPPER/lower/Trim)
        df = TransformationUtils.apply_standardize(self.spark, df, self.transform_config)

        # 5. Move row_hash to end
        df = TransformationUtils.move_columns_to_end(df,["row_hash"])
        return df

    def load(self, df: DataFrame) -> None:
        """
        Write raw data to bronze table using Delta format
        - Uses write mode from config (overwrite or append)
        """

        target_table = self.env_manager.construct_table_fqn(self.destination_config.get("table"))
        mode = self.destination_config.get("mode", "append")

        self.logger.info(f"Loading data to {target_table} (mode: {mode})")

        if not target_table:
            raise ValueError("No target table configured in destination_config.")

        if mode == "merge":
            merge_condition = self.destination_config.get("merge_condition", None)
            self.delta_manager.merge_df(df, target_table, merge_condition)
        elif mode == "append" or mode == "overwrite":
            self.delta_manager.write_df(df, target_table, mode, self.delta_manager.bronze_tbl_properties)

    def run(self) -> None:
        """Customized run sequence for bronze layer"""
        target_table = self.env_manager.construct_table_fqn(self.destination_config.get("table"))
        start_dtm = self.audit_logger._get_datetime_now()
        pipeline_name = self.pipeline_config.get("name")
        try:
            self.logger.info(f"Pipeline '{pipeline_name}' | Starting extract method...")
            df = self.extract()

            if df is None:
                self.logger.info(f"Pipeline '{pipeline_name}' | Skipping further operation.")
                return

            self.logger.info(f"Pipeline '{pipeline_name}' | Starting transform method...")
            df = self.transform(df)

            self.logger.info(f"Pipeline '{pipeline_name}' | Starting load method...")
            self.load(df)

            masker = Masker(self.spark, self.args, self.config, self.env_manager)
            masker.apply_masking(df, target_table)

            self._log_audit(df, target_table, start_dtm)

            self.logger.info(f"Pipeline '{pipeline_name}' | Successfully Completed.")

        except Exception as e:
            err_msg = (
                f"Error in pipeline = '{pipeline_name}' | "
                f"layer = '{self.args.layer}' | "
                f"config_file = '{self.args.config_file_name}': {str(e)}"
            )
            self.logger.error(err_msg, exc_info=True)
            self.audit_logger.log_error(e, err_msg)
            self._log_audit(None, target_table, start_dtm, failure=True)
            raise
        finally:
            self.audit_logger.log_run_summary()
            self.delta_handler.flush()

    def _log_audit(self, df: DataFrame, target_table: str, start_dtm: str, failure: bool = False) -> None:
        """Handles audit logging after load, including failures."""
        self.logger.info("Starting audit logging for file loads.")
        # Map file names to their full source paths
        file_path_map = {f.name: f.path for f in self.files_discovered}

        # If load failed or no DataFrame, log each file as failed and exit
        if failure or df is None:
            self.logger.warning("Load failed or DataFrame is None. Logging all files as failed.")
            for f in self.files_discovered:
                self.audit_logger.log_file_load(
                    source_name=f.path,
                    target_name=target_table,
                    operation_type=self.destination_config.get("mode"),
                    load_status="Failed",
                    start_dtm=start_dtm
                )
            self.logger.info("Audit logging for failed files completed.")
            return

        # Compute total records per file from the ingested DataFrame
        self.logger.info("Computing total records per file from ingested DataFrame.")
        records_total_df = df.groupBy("file_name").agg(F.count("*").alias("records_total"))
        records_total = {row["file_name"]: row["records_total"] for row in records_total_df.collect()}

        # Compute succeeded records per file from the target Delta table for this run
        self.logger.info("Computing succeeded records per file from target Delta table.")
        records_succeeded_df = (
            self.spark.table(target_table)
                .where(F.col("databricks_run_id") == self.args.databricks_run_id)
                .groupBy("file_name")
                .agg(F.count("*").alias("records_succeeded"))
        )
        records_succeeded = {row["file_name"]: row["records_succeeded"] for row in records_succeeded_df.collect()}

        # Log audit for each file: success if all records succeeded, else failed
        for file_name, total in records_total.items():
            succeeded = records_succeeded.get(file_name, 0)
            full_path = file_path_map.get(file_name)
            load_status = "Success" if total == succeeded else "Failed"
            self.logger.debug(f"File: {file_name}, Total: {total}, Succeeded: {succeeded}, Status: {load_status}")
            self.audit_logger.log_file_load(
                source_name=full_path,
                target_name=target_table,
                operation_type=self.destination_config.get("mode"),
                records_total=total,
                records_succeeded=succeeded,
                load_status=load_status,
                start_dtm=start_dtm
            )
        self.logger.info("Audit logging completed for all files.")