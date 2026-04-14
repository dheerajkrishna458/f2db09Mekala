import logging
from typing import Optional, Tuple
from pyspark.sql import SparkSession

class EnvironmentManager:
    """
    Simple environment manager that infers environment from Databricks workspace URL 
    and helps construct Unity Catalog volume/catalog and source paths.
    """
    
    WORKSPACE_TO_ENV = {
        "adb-xxxx.x.azuredatabricks.net": "dev",
        "adb-4314.14.azuredatabricks.net": "beta",
        "adb-60.0.azuredatabricks.net": "prod"
    }

    def __init__(self, spark: Optional[SparkSession], args):
        """
        :param spark: pyspark.sql.SparkSession
        :param args: arguments from ArgParser
        """
        self.spark = spark
        self.args = args
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Determine environment
        self.env = self.get_workspace_env()
        
        # Prefix used when constructing catalog names
        self.env_prefix = f"{self.env}_"
        
        # Sandbox overrides
        self.catalog = None
        self.schema = None
        
        # If in dev environment and schema is provided, set sandbox catalog/schema from args
        if self.env == "dev" and getattr(self.args, "test_schema", None) is not None:
            self._set_sandbox_catalog_schema()

    def _set_sandbox_catalog_schema(self):
        self.logger.info("Sandbox environment detected!!!")
        self.catalog = getattr(self.args, "test_catalog", "resource_sandbox")
        self.schema = getattr(self.args, "test_schema", None)
        self.logger.info(f"Sandbox details set: catalog='{self.catalog}', schema='{self.schema}'")

    def get_workspace_env(self) -> str:
        """Determines the environment based on the Databricks workspace URL."""
        workspace_url = ""
        if self.spark is not None:
            workspace_url = self.spark.conf.get("spark.databricks.workspaceUrl", "")
        
        env = self.WORKSPACE_TO_ENV.get(workspace_url)
        if env:
            return env
        
        self.logger.info(f"Unknown workspace URL '{workspace_url}'. Defaulting to 'dev'.")
        return "dev"

    def construct_volume_catalog_name(self, catalog_base: str) -> str:
        """Constructs the full catalog name for a Unity Catalog volume."""
        return f"{self.env_prefix}{catalog_base}"

    def construct_source_folder_path(self, source_path: str) -> str:
        """Constructs the complete base path for provided source_path."""
        src_catalog = self.construct_volume_catalog_name("deltalake")
        full_path = f"/Volumes/{src_catalog}/raw/{source_path.lstrip('/')}"
        return full_path

    def construct_catalog_name(self, catalog_base: str) -> str:
        """Constructs the full catalog name based on the environment."""
        if self.catalog:
            return self.catalog
        return f"{self.env_prefix}{catalog_base}"

    def construct_schema_name(self, schema_name: str) -> str:
        """Constructs the schema name based on the environment or sandbox."""
        if self.schema:
            return self.schema
        return schema_name

    def construct_table_name(self, schema_name: str, table: str) -> str:
        """Constructs the table name, optionally including schema prefix in sandbox."""
        if self.schema:
            # If in sandbox, we might want to suffix the table with the original schema
            # to avoid name collisions if multiple schemas are mapped to one sandbox schema.
            return f"{schema_name}_{table}"
        return table

    def _get_audit_catalog_schema(self) -> str:
        """Gets the FQN for the audit catalog and schema."""
        if self.catalog and self.schema:
            return f"{self.catalog}.{self.schema}"
        
        catalog = self.construct_catalog_name("deltalake_audit")
        return f"{catalog}.audit_logs"

    def get_error_logs_fqn(self) -> str:
        """Returns the FQN for the error_logs table."""
        return f"{self._get_audit_catalog_schema()}.error_logs"

    def get_run_load_history_fqn(self) -> str:
        """Returns the FQN for the run_load_history table."""
        return f"{self._get_audit_catalog_schema()}.run_load_history"

    def get_data_quality_exceptions_fqn(self) -> str:
        """Returns the FQN for the data_quality_exceptions table."""
        return f"{self._get_audit_catalog_schema()}.data_quality_exceptions"

    def get_data_quality_summary_fqn(self) -> str:
        """Returns the FQN for the data_quality_summary table."""
        return f"{self._get_audit_catalog_schema()}.data_quality_summary"

    def get_code_log_table_fqn(self) -> str:
        """Returns the FQN for the code_logs table."""
        return f"{self._get_audit_catalog_schema()}.code_logs"

    def parse_cleaned_table_path(self, table_fqn: str) -> Tuple[str, str, str]:
        """Removes env prefix and splits FQN into catalog, schema, table."""
        parts = table_fqn.split(".")
        if len(parts) != 3:
            raise ValueError(f"Expected 3 components in table FQN, got {len(parts)}: {table_fqn}")
        
        catalog, schema, table = parts
        # If the catalog starts with the env prefix (e.g. dev_), strip it
        if catalog.startswith(self.env_prefix):
            catalog = catalog[len(self.env_prefix):]
            
        return catalog, schema, table

    def construct_table_fqn(self, table_fqn: str) -> str:
        """
        Constructs the Table FQN based on the environment.
        :param table_fqn: Fully qualified table name (base name without environment)
        :return: Environment-specific FQN for the table
        """
        catalog, schema, table = self.parse_cleaned_table_path(table_fqn)
        
        catalog_name = self.construct_catalog_name(catalog)
        schema_name = self.construct_schema_name(schema)
        table_name = self.construct_table_name(schema, table)
        
        return f"{catalog_name}.{schema_name}.{table_name}"

    def get_masking_function_fqn(self, function_nm: str) -> str:
        """Constructs the fully qualified name (FQN) for a masking function."""
        catalog = f"{self.env_prefix}protect"
        schema = "masking"
        return f"{catalog}.{schema}.{function_nm}"