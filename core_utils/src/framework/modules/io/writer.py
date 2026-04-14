import time
import random
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

class DeltaManager:
    """
    Utility class for managing Delta tables in Databricks.
    Provides methods for checking table existence, setting table properties,
    performing robust Delta MERGE operations, and writing DataFrames to Delta tables.
    """

    def __init__(self, spark, args):
        """
        Initialize DeltaManager with Spark session and arguments.

        Args:
            spark (SparkSession): The Spark session.
            args (dict): Additional arguments for configuration.
        """
        self.spark = spark
        self.args = args
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.default_tbl_properties = {
            "delta.appendOnly": "false",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
            "delta.enableChangeDataFeed": "true",
            "delta.minReaderVersion": "3",
            "delta.minWriterVersion": "7",
            "delta.isolationLevel": "WriteSerializable",
        }

        self.bronze_tbl_properties = {
            "delta.enableRowTracking": "true"
        }
        self.silver_tbl_properties = {}
        self.gold_tbl_properties = {}

    def table_exists(self, table_fqn: str) -> bool:
        """
        Checks if a table exists in the Spark catalog.

        Args:
            table_fqn (str): Fully qualified table name.

        Returns:
            bool: True if table exists, False otherwise.
        """
        return self.spark.catalog.tableExists(table_fqn)

    def get_table_properties(self, table_fqn: str) -> dict:
        """
        Retrieves table properties for a Delta table.

        Args:
            table_fqn (str): Fully qualified table name.

        Returns:
            dict: Table properties as key-value pairs.
        """
        try:
            df = self.spark.sql(f"SHOW TBLPROPERTIES {table_fqn}")
            return {row.key.strip(): row.value.strip() for row in df.collect()}
        except Exception as e:
            raise Exception(f"Failed to get properties for {table_fqn}: {e}")

    def set_table_properties(
        self,
        table_fqn: str,
        extra_properties: dict = None
    ):
        """
        Sets Delta table properties using DEFAULT, BRONZE, SILVER, GOLD.

        Args:
            table_fqn (str): Fully qualified table name.
            extra_properties (dict, optional): Additional properties to set.
        """
        if not self.table_exists(table_fqn):
            raise Exception(f"Table {table_fqn} does not exist; cannot set properties.")

        # Merge default properties with any overrides
        required_properties = {
            **self.default_tbl_properties,
            **(extra_properties or {})
        }

        existing = self.get_table_properties(table_fqn)

        # Enable auto clustering if not already enabled
        if "clusterByAuto" not in existing:
            self.spark.sql(f"ALTER TABLE {table_fqn} CLUSTER BY AUTO")
            self.logger.info(f"Auto Liquid Clustering Enabled for {table_fqn}")

        # Find properties that need updating
        to_update = {}
        for k, v in required_properties.items():
            current = existing.get(k)
            if str(current).strip().lower().strip('"').strip("'") != str(v).strip().lower().strip('"').strip("'"):
                to_update[k] = v

        if not to_update:
            return

        set_clause = ", ".join([f"'{k}' = '{v}'" for k, v in to_update.items()])
        self.spark.sql(f"ALTER TABLE {table_fqn} SET TBLPROPERTIES ({set_clause})")

        self.logger.info(f"Updated table properties on {table_fqn}.")

    def merge_df(
            self,
            source_df,
            target_table,
            merge_condition,
            update_dict=None,
            insert_dict=None,
            update_condition=None):
        """
        Performs a robust MERGE operation on a Delta table.

        Args:
            source_df (DataFrame): The source DataFrame (aliased as 'src').
            target_table (str): The target Delta table FQN (aliased as 'tgt').
            merge_condition (str): The SQL condition for matching (e.g., 'tgt.id = src.id').
            update_dict (dict, optional): Columns to update on match.
            insert_dict (dict, optional): Columns to insert on no match.
            update_condition (str, optional): Additional condition for WHEN MATCHED.
        """
        if source_df.isEmpty():
            self.logger.info(f"merge: Source DF is empty, skipping merge for {target_table}.")
            return

        max_retries = 10

        for attempt in range(1, max_retries + 1):
            try:
                retry_msg = "" if attempt == 1 else f"Retry attempt ({attempt}/{max_retries}): "
                target = DeltaTable.forName(self.spark, target_table)
                merge_builder = target.alias("tgt").merge(source_df.alias("src"), merge_condition)

                # Set update actions for matched rows
                if update_dict:
                    if update_condition:
                        merge_builder = merge_builder.whenMatchedUpdate(
                            condition=update_condition, set=update_dict
                        )
                    else:
                        merge_builder = merge_builder.whenMatchedUpdate(set=update_dict)

                # Set insert actions for unmatched rows
                if insert_dict:
                    merge_builder = merge_builder.whenNotMatchedInsert(values=insert_dict)

                self.logger.info(f"{retry_msg}Executing MERGE on {target_table}...")
                merge_builder.execute()
                self.logger.info(f"{retry_msg}MERGE on {target_table} completed.")

                return

            except Exception as e:
                # Handle concurrency conflicts with retry
                is_conflict = ("ConcurrentAppendException" in str(e) or "ConcurrentMergeOperation" in str(e))

                if is_conflict and attempt < max_retries:
                    delay_seconds = random.uniform(5, 60)
                    self.logger.warning(f"Concurrency conflict detected on {target_table}: {e}\n\n" + "-" * 50)
                    self.logger.info(
                        f"Retrying Merge operation again in {delay_seconds:.2f}s "
                        f"(next attempt {attempt + 1}/{max_retries})..."
                    )
                    time.sleep(delay_seconds)
                    continue

                raise Exception(
                    f"Failed to MERGE into {target_table} after {attempt} attempt(s): {e}"
                )

    def write_df(self, df, target_table, mode="append", table_properties:dict= None):
        """
        Writes a DataFrame to a Delta table and sets table properties.

        Args:
            df (DataFrame): DataFrame to write.
            target_table (str): Target Delta table name.
            mode (str, optional): Write mode ("append", "overwrite", etc.).
            table_properties (dict, optional): Table properties to set.
        """
        (
            df.write.format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
            .saveAsTable(target_table)
        )
        self.set_table_properties(target_table, extra_properties=table_properties)

    def write_multi_split_df(self, df, target_table, mode="append"):
        """
        Writes a DataFrame produced from multi-split file ingestion.
        Kept as a dedicated entry point for parity with reader multi-split flow.

        Args:
            df (DataFrame): DataFrame to write.
            target_table (str): Target Delta table name.
            mode (str, optional): Write mode.
        """
        self.write_df(df, target_table, mode)