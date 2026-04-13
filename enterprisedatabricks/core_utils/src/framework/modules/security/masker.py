"""Pattern-based masking for PII detection and masking"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace
import logging

from framework.core.environment import EnvironmentManager

class Masker:
    """Manages masking operations for Delta Lake tables."""
    
    def __init__(self, spark, args, config, env_manager):
        """
        Initializes Masker for managing and applying column-level masking policies on Delta Lake tables.
        
        Parameters:
            spark (SparkSession): The active Spark session used for Delta Lake operations.
            args (dict): Additional arguments.
            config (dict): Configuration dictionary (parsed from YAML).
            env_manager (EnvironmentManager): Manages environment utilities.
            
        Attributes:
            spark (SparkSession): The active Spark session used for Delta Lake operations.
            args (dict): Additional arguments.
            config (dict): Configuration dictionary (parsed from YAML).
            env_manager (EnvironmentManager): Manages environment utilities.
            logger (logging.Logger): Logger for Masker operations.
        """
        self.spark = spark
        self.args = args
        self.config = config
        self.env_manager = env_manager
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
    def apply_masking(self, source_df: DataFrame, table_fqn: str):
        """
        Applies masking functions to columns in a Delta table based on registered masking configurations.
        
        Parameters:
            source_df : DataFrame
                The source DataFrame representing the table structure and columns.
            table_fqn : str
                Fully qualified name of the target Delta table.
                
        Workflow:
        ---------
        1. Retrieves masking configuration for the table from config dictionary.
        2. Compares registered masking policies with already applied column masks.
        3. Applies missing masking functions to eligible columns.
        
        Notes:
        ------
        - Only columns present in the source DataFrame and registered for masking will be altered.
        - Masking functions are applied using `ALTER TABLE ... ALTER COLUMN ... SET MASK ...` SQL syntax.
        """
        catalog, schema, tbl = self.env_manager.parse_cleaned_table_path(table_fqn)
        
        # Get masking config from YAML config dictionary
        masking_config_list = self.config.get("security", {}).get("masking", [])
        
        if not masking_config_list:
            self.logger.info(f"No masking configuration found for table '{table_fqn}'.")
            return
            
        self.logger.info(f"Initiating masking process for table '{table_fqn}'.")
        
        # Convert masking config list to DataFrame-like structure for processing
        masking_config_df = self.spark.createDataFrame(masking_config_list)
        
        # get any columns for given table that are registered to be masked
        masking_check_df = (
            masking_config_df
            .filter(col("column").isin(source_df.columns))
        )
        
        # get any columns for given table with masking functions already applied
        masking_functions_sql = f"""
            SELECT
                '{catalog}' AS table_catalog,
                table_schema,
                table_name,
                column_name,
                mask_name,
                regexp_extract(mask_name, '([^.]+)', 1) AS mask_name_simple,
                USING columns
            FROM
                system.information_schema.column_masks
            WHERE
                table_catalog = '{catalog}'
                AND table_schema = '{schema}'
                AND table_name = '{tbl}'
        """
        
        masking_functions_df = self.spark.sql(masking_functions_sql)
        
        # compare columns registered to what has been already been applied
        joined_df = masking_check_df.join(
            masking_functions_df,
            (masking_check_df.column == masking_functions_df.column_name) &
            (masking_check_df.policy == masking_functions_df.mask_name_simple),
            how='left'
        )
        
        # filter for columns registered for masking but missing the function
        filtered_df = joined_df.filter(joined_df.mask_name.isNull())
        
        if not filtered_df.isEmpty():
            self.logger.info(f"Found {{filtered_df.count()}} column(s) which require masking.")
            for row in filtered_df.collect():
                masking_function_fqn = self.env_manager.get_masking_function_fqn(row["policy"])
                masking_sql_statement = f"""
                    ALTER TABLE {table_fqn}
                    ALTER COLUMN {row['column']}
                    SET MASK {masking_function_fqn};
                """
                
                # check that the column registered to be masked exists. If so, apply masking function
                if row["column"] in source_df.columns:
                    try:
                        self.spark.sql(masking_sql_statement)
                        self.logger.info(
                            f"Masking function '{row['policy']}' successfully applied to column '{row['column']}'"
                        )
                    except Exception as e:
                        split_marker = "JVM stacktrace:"
                        if split_marker in str(e):
                            self.logger.error(f"Masking function does not exist: {{str(e).split(split_marker)[0].strip()}}")
                        else:
                            self.logger.error(f"Masking function does not exist: {e}")
                            
                        self.logger.info("Proceeding to the next column despite masking error.")
                else:
                    self.logger.info(
                        f"Column '{row['column']}' registered for masking does not exist in table '{table_fqn}'."
                    )
            self.logger.info("Masking operation completed for all columns.")
        else:
            self.logger.info("All masking functions are already applied. No further action required.")
