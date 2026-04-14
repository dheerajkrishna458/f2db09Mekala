from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any
from framework.core.base import MedallionBase
from framework.modules.utils.transformations import TransformationUtils
from framework.modules.io.reader import DeltaTableIngester
from framework.modules.io.writer import DeltaManager
from framework.modules.dq.dqx_validator import DataQualityCheck
from framework.modules.security.masker import Masker

class SilverRefiner(MedallionBase):
    """
    Silver Layer: Applies data quality and business rules.
    """

    def __init__(self, args):
        """
        Initialize SilverRefiner with configs for pipeline, source, transform, and destination.
        """
        super().__init__(args)

        # Load pipeline, source, transform, and destination configs
        self.pipeline_config = self.config.get('pipeline_metadata', {})
        self.source_config = self.config.get('source', {})
        self.transform_config = self.config.get('transform', {})
        self.destination_config = self.config.get('destination', {})

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply data quality rules and transformations"""
        self.logger.info("Applying silver transformations")

        # 1. Add new columns
        df = TransformationUtils.add_custom_columns(df, self.transform_config)

        # 2. Apply standardization
        df = TransformationUtils.apply_standardize(self.spark, df, self.transform_config)

        # 2. Apply type casting
        df = TransformationUtils.apply_casts(df, self.transform_config)

        return df

    def load(self, df: DataFrame) -> None:
        """
        Write validated/cleaned data to silver table
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
            self.delta_manager.write_df(df, target_table, mode, self.delta_manager.silver_tbl_properties)

    def run(self) -> None:
        """Customized run for silver layer"""
        target_table = self.env_manager.construct_table_fqn(self.destination_config.get("table"))
        start_dtm = self.audit_logger._get_datetime_now()
        try:
            pipeline_name = self.pipeline_config.get("name")

            self.logger.info(f"Pipeline '{pipeline_name}' | Starting extract method...")
            df = self.extract()

            if df is None:
                self.logger.info(f"Pipeline '{pipeline_name}' | No data extracted, Skipping further operation.")
                self.audit_logger.log_run_audit(df, self.config, "BronzeToSilver", "success", start_dtm)
                return

            self.logger.info(f"Pipeline '{pipeline_name}' | Starting transform method...")
            df = self.transform(df)

            # Apply data quality checks
            dqc = DataQualityCheck(self.spark, self.config, self.env_manager, self.audit_logger, self.delta_manager)
            df = dqc.apply_checks(df)

            self.logger.info(f"Pipeline '{pipeline_name}' | Starting load method...")
            self.load(df)

            masker = Masker(self.spark, self.args, self.config, self.env_manager)
            masker.apply_masking(df, target_table)

            self.audit_logger.log_run_audit(df, self.config, "success",start_dtm)

            self.logger.info(f"Pipeline '{pipeline_name}' | Successfully Completed.")

        except Exception as e:
            err_msg = (
                f"Error in pipeline = '{pipeline_name}' | "
                f"layer = '{self.args.layer}' | "
                f"config_file = '{self.args.config_file_name}': {str(e)}"
            )
            self.logger.error(err_msg, exc_info=True)
            self.audit_logger.log_error(e, err_msg)
            self.audit_logger.log_run_audit(None, self.config, "failure", start_dtm, exception=e)
            raise
        finally:
            self.audit_logger.log_run_summary()
            self.delta_handler.flush()