import logging
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any
from typing import Dict, Any

from framework.core.session import SessionManager
from framework.core.config import ConfigManager
from framework.core.environment import EnvironmentManager
from framework.modules.audit.audit_logger import AuditLogger
from framework.modules.io.reader import BaseFileIngester, CSVFileIngester, EBCDICFileIngester, DeltaTableIngester
from framework.modules.io.writer import DeltaManager
from framework.modules.audit.logging_setup import setup_pipeline_logging
from framework.modules.audit.yaml_telemetry_logger import YamlTelemetryLogger


class MedallionBase(ABC):
    """Abstract base class for medallion architecture layers"""

    def __init__(self, args):
        # Set up logger for this medallion layer class
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__qualname__}")
        self.args = args

        # Initialize Spark session, environment manager, audit logger, delta manager, and config manager
        self.spark = SessionManager.get_session()
        self.env_manager = EnvironmentManager(self.spark, self.args)
        self.audit_logger = AuditLogger(self.spark, self.args, self.env_manager)
        self.delta_manager = DeltaManager(self.spark, self.args)
        self.config_manager = ConfigManager(self.args)
        self.config_manager.load_config()

        self.config = self.config_manager.config
        self.source_config = self.config.get('source', {})

        # Set up Delta logging for pipeline code execution
        code_log_table = self.env_manager.get_code_log_table_fqn()
        self.delta_handler = None
        if code_log_table:
            self.delta_handler = setup_pipeline_logging(self.spark, code_log_table, self.args)

        # Initialize YAML telemetry logger and log YAML load history
        self.yaml_logger = YamlTelemetryLogger(self.spark, self.env_manager, self.delta_manager, self.config_manager)
        self.yaml_logger.log_yaml_load_history()

    def _get_ingestor(self, config: Dict) -> BaseFileIngester:
        """
        Return the correct ingestor class based on config.
        Used by the extract method.
        Imports from framework.modules.io.reader
        """
        match config.get("format"):
            case 'csv':
                return CSVFileIngester(self.spark, self.env_manager, config)
            case 'cobol':
                return EBCDICFileIngester(self.spark, self.env_manager, config)
            case _:
                return BaseFileIngester(self.spark, self.env_manager, config)
            

    def extract(self) -> DataFrame:
        """
        Read raw data from source path
        Supports CSV, Parquet, JSON, Delta formats

        Returns:
            DataFrame: Raw data from source
        """

        ingest_type = self.source_config.get("type")
        properties = self.source_config.get("properties", self.source_config)

        match ingest_type:
            case "file":
                ingester = self._get_ingestor(properties)
                df = ingester.read_df()

                # store discovered file paths (full source paths)
                self.files_discovered = getattr(ingester, "discovered_files", [])

            case "delta_table":
                df = DeltaTableIngester(self.spark, self.env_manager, properties).read_df()
            case _:
                raise ValueError(f"Unsupported source type: {ingest_type}")
        return df
    
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Apply layer-specific transformations
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Transformed data
        """
        pass
    
    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """
        Write transformed data to target (table, delta, etc.)
        
        Args:
            df: DataFrame to write
        """
        pass
    
    @abstractmethod
    def run(self) -> None:
        """
        Execute the pipeline for this layer.

        Concrete implementations may include additional steps such as
        validation, enrichment, or custom auditing logic. The base
        class does not provide a default so layers can override freely.
        """
        pass
