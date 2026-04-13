import argparse
import logging

class ArgParser:
    """Handles parsing of common CLI arguments for Medallion pipeline."""
    logger = logging.getLogger(f"{__name__}.{__qualname__}")

    @classmethod
    def parse(cls, entry_point=None, layer=None):
        # Set up argument parser for Medallion pipeline CLI
        parser = argparse.ArgumentParser()
        # Required arguments for config and user identification
        parser.add_argument("--config_file_name", required=True, help="YAML config file name")
        parser.add_argument("--config_directory_path", required=True, help="YAML config directory")
        parser.add_argument("--correlation_id", required=False, default="test_correlation_id", help="Correlation ID for tracking")
        parser.add_argument("--databricks_job_id", required=False, default="test_job_id", help="Databricks Job ID")
        parser.add_argument("--databricks_run_id", required=False, default="test_run_id", help="Databricks Run ID")
        parser.add_argument("--manual_intervention_reason", required=False, help="Reason for manual intervention")
        parser.add_argument("--email_id", required=True, help="TriggerName/Email Id of user")
        parser.add_argument("--entry_point", required=False, default=entry_point, help="Pipeline entry point")
        parser.add_argument("--layer", required=False, default=layer, help="Medallion layer")
        parser.add_argument("--test_schema", required=False, help="Test sandbox schema")
        parser.add_argument("--test_catalog", required=False, default="resource_sandbox", help="Test Sandbox catalog")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate and print execution plan without running the pipeline"
        )
        parser.add_argument(
            "--restore-to-timestamp",
            help="Delta timestamp for restore mode (future)"
        )
        # Parse known arguments, ignore unknown ones
        args, _ = parser.parse_known_args()
        # Log parsed arguments
        test_args = ["test_schema", "test_catalog"]
        cls.logger.info("Parsed argument: ")
        for k, v in vars(args).items():
            if k not in test_args:
                cls.logger.info(f"  {k} = {v}")
        return args