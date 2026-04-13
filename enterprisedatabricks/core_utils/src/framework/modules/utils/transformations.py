"""
Common transformation utilities used across all medallion layers
"""
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class TransformationUtils:
    """Reusable transformation methods for all layers"""
    
    @staticmethod
    def add_row_hash(df: DataFrame, columns_included: list = None) -> DataFrame:
        """Adds a SHA-256 hash column to the dataframe"""
        
        if columns_included is None:
            columns_included = df.columns
            
        # Concat the columns with '||' delimiter
        concat_columns_expr = F.concat_ws("||", *[F.col(f"`{c}`").cast("string") for c in columns_included])
        hashed_df = df.withColumn("row_hash", F.sha2(concat_columns_expr, 256))
        
        return hashed_df
        
    @staticmethod
    def move_columns_to_end(df: DataFrame, cols) -> DataFrame:
        """
        Move one or more columns to the end of a DataFrame.
        Args:
            df (DataFrame): Input DataFrame
            cols (str | list[str]): Column name or list of column names to move
        Returns:
            DataFrame: DataFrame with specified columns moved to the end
        """
        if isinstance(cols, str):
            cols = [cols]
            
        cols_to_move = [c for c in cols if c in df.columns]
        remaining_cols = [c for c in df.columns if c not in cols_to_move]
        
        return df.select(*remaining_cols, *cols_to_move)

    @staticmethod
    def add_metadata_columns(df: DataFrame, args, file_name: str = None) -> DataFrame:
        """
        Add standard metadata columns to DataFrame.
        
        Args:
            df: Input DataFrame
            args: Arguments object with job/run/email/correlation IDs
            file_name (Optional): File name to use for the 'file_name' column
            
        Returns:
            DataFrame with metadata columns added
        """
        # Extract metadata values from args object
        job_id = getattr(args, "databricks_job_id", None)
        run_id = getattr(args, "databricks_run_id", None)
        email_id = getattr(args, "email_id", None)
        correlation_id = getattr(args, "correlation_id", None)
        
        if file_name:
            file_name_col = F.lit(file_name)
        elif "file_name" in df.columns:
            file_name_col = F.col("file_name")
        else:
            file_name_col = F.col("_metadata.file_name")
            
        # Define metadata columns to add
        metadata_columns = {
            "file_name": file_name_col,
            "correlation_id": F.lit(correlation_id).cast("string"),
            "databricks_job_id": F.lit(job_id),
            "databricks_run_id": F.lit(run_id),
            "load_create_user_id": F.lit(email_id).cast("string"),
            "load_update_user_id": F.lit(None).cast("string"),
            "load_create_dtm": F.current_timestamp(),
            "load_update_dtm": F.lit(None).cast("timestamp")
        }
        
        # Add metadata columns to DataFrame
        df = df.withColumns(metadata_columns)
        
        logger.info("Added metadata columns.")
        return df
        
    @staticmethod
    def apply_standardize(spark: SparkSession, df: DataFrame, config: Dict) -> DataFrame:
        std = config.get("standardize", {}) or {}
        result = df
        
        # Apply column renaming if rename_map is present
        rename_map = std.get("rename_map", {})
        if rename_map:
            for old_name, new_name in rename_map.items():
                if old_name in result.columns:
                    result = result.withColumnRenamed(old_name, new_name)
            logger.info(f"Renamed columns using rename_map: {rename_map}")
            
        cols = result.columns
        dtypes = dict(result.dtypes)
        
        # Handle case transformation for all string columns
        case_option = std.get("case", "none")
        if case_option in ("lower", "upper"):
            exprs = {}
            for c in cols:
                if dtypes.get(c) in ("string", "varchar"):
                    if case_option == "lower":
                        exprs[c] = F.lower(F.col(f"`{c}`"))
                    elif case_option == "upper":
                        exprs[c] = F.upper(F.col(f"`{c}`"))
            if exprs:
                result = result.withColumns(exprs)
                logger.info(f"Applied {case_option} case to all string columns")
                
        # Trim all string columns if trim_strings is True
        if std.get("trim_strings", False):
            exprs = {}
            for c in cols:
                if dtypes.get(c) in ("string", "varchar"):
                    exprs[c] = F.trim(F.col(f"`{c}`"))
            if exprs:
                result = result.withColumns(exprs)
                logger.info("Trimmed all string columns (trim_strings=True)")
                
        # Strip whitespace for specific columns
        strip_cols = [c for c in std.get("strip_whitespace", []) if c in result.columns]
        if strip_cols:
            exprs = {c: F.trim(F.col(f"`{c}`")) for c in strip_cols}
            result = result.withColumns(exprs)
            logger.info(f"Stripped whitespace for columns: {strip_cols}")
            
        # Lowercase specific columns
        lower_cols = [c for c in std.get("lowercase", []) if c in result.columns]
        if lower_cols:
            exprs = {c: F.lower(F.col(f"`{c}`")) for c in lower_cols}
            result = result.withColumns(exprs)
            logger.info(f"Lowercased columns: {lower_cols}")
            
        # Uppercase specific columns
        upper_cols = [c for c in std.get("uppercase", []) if c in result.columns]
        if upper_cols:
            exprs = {c: F.upper(F.col(f"`{c}`")) for c in upper_cols}
            result = result.withColumns(exprs)
            logger.info(f"Uppercased columns: {upper_cols}")
            
        return result

    @staticmethod
    def apply_casts(df: DataFrame, config: Dict) -> DataFrame:
        """
        Cast specified columns to given types using config dictionary.
        
        Args:
            df: Input DataFrame
            config: Dictionary containing 'cast' key with list of dicts {'column': ..., 'to_type': ...}
            
        Returns:
            DataFrame with columns cast to specified types
        """
        casts = (config.get("casts", []) or [])
        logger.info(f"Casts: {casts}")
        for cast in casts:
            col_name = cast.get("column")
            to_type = cast.get("to_type")
            if col_name in df.columns and to_type:
                df = df.withColumn(col_name, F.col(col_name).cast(to_type))
                logger.info(f"Casted column {col_name} to {to_type}")
        return df

    @staticmethod
    def replace_special_chars_in_column_name(df: DataFrame, special_char: str, replacement: str) -> DataFrame:
        """
        Replace specified special character in column names with a specified character.
        
        Args:
            df: Input DataFrame
            special_char: Character to replace
            replacement: Character to replace with
            
        Returns:
            DataFrame with updated column names
        """
        new_columns = []
        for col in df.columns:
            new_col = col.replace(special_char, replacement) # Replace specified special character
            new_columns.append(new_col)
            
        df = df.toDF(*new_columns)
        logger.info(f"Replaced '{special_char}' in column names with '{replacement}'")
        return df
        
    @staticmethod
    def add_custom_columns(
        df: DataFrame,
        config: Dict
    ) -> DataFrame:
        """
        Add custom calculated columns
        
        Args:
            df: Input DataFrame
            config: List of dicts (['name': ..., 'expr': ...]) under 'with_columns'
            
        Returns:
            DataFrame with new columns
        """
        from pyspark.sql.functions import expr
        columns = config.get("with_columns", [])
        
        for col in columns:
            col_name = col.get("name")
            col_expression = col.get("expr")
            if col_name and col_expression:
                try:
                    df = df.withColumn(col_name, expr(col_expression))
                    logger.info(f"Added column: {col_name}")
                except Exception as e:
                    logger.warning(f"Failed to add column {col_name}: {str(e)}")
                    
        return df
        
    @staticmethod
    def remove_duplicates(df: DataFrame, subset: List[str] = None) -> DataFrame:
        """
        Remove duplicate rows
        
        Args:
            df: Input DataFrame
            subset: Columns to consider for duplication (all if None)
            
        Returns:
            DataFrame with duplicates removed
        """
        original_count = df.count()
        
        if subset:
            df = df.dropDuplicates(subset)
            logger.info(f"Removed duplicates based on columns: {subset}")
        else:
            df = df.dropDuplicates()
            logger.info("Removed exact duplicates")
            
        final_count = df.count()
        logger.info(f"Rows removed: {original_count - final_count}")
        
        return df

    @staticmethod
    def fill_nulls(df: DataFrame, fill_values: Dict[str, Any]) -> DataFrame:
        """
        Fill null values with specified defaults
        
        Args:
            df: Input DataFrame
            fill_values: Dictionary of {column: fill_value}
            
        Returns:
            DataFrame with nulls filled
        """
        df = df.fillna(fill_values)
        logger.info(f"Filled null values in {len(fill_values)} columns")
        return df

    @staticmethod
    def drop_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Drop specified columns
        
        Args:
            df: Input DataFrame
            columns: List of column names to drop
            
        Returns:
            DataFrame with columns dropped
        """
        existing_cols = [col for col in columns if col in df.columns]
        df = df.drop(*existing_cols)
        logger.info(f"Dropped {len(existing_cols)} columns")
        return df

    @staticmethod
    def select_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Select only specified columns
        
        Args:
            df: Input DataFrame
            columns: List of column names to keep
            
        Returns:
            DataFrame with only specified columns
        """
        existing_cols = [col for col in columns if col in df.columns]
        df = df.select(*existing_cols)
        logger.info(f"Selected {len(existing_cols)} columns")
        return df

    @staticmethod
    def apply_filters(
        df: DataFrame,
        config: Dict
    ) -> DataFrame:
        """
        Apply filter expressions from config to DataFrame
        
        Args:
            df: Input DataFrame
            config: Dictionary containing "filters" as a list of SQL expressions
            
        Returns:
            DataFrame with filters applied
        """
        filters = (config.get("filters", []) or [])
        for expr in filters:
            df = df.filter(F.expr(expr))
        logger.info(f"Applied {len(filters)} filters")
        return df

    @staticmethod
    def cast_all_columns_to_string(df: DataFrame) -> DataFrame:
        """
        Cast all columns in the DataFrame to string type.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with all columns cast to string
        """
        for col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("string"))
        return df

    @staticmethod
    def apply_aggregations(spark: SparkSession, df: DataFrame, config: Dict) -> DataFrame:
        """
        Apply aggregations to DataFrame based on config.
        
        Args:
            spark: SparkSession
            df: Input DataFrame
            config: Dictionary containing 'aggregations' with 'group_by' and 'metrics'
            
        Returns:
            Aggregated DataFrame
        """
        agg = (config.get("aggregations", {}) or {})
        group_by = agg.get("group_by") or []
        metrics = agg.get("metrics") or {}
        
        if not group_by or not metrics:
            logger.info(
                "No group_by or metrics specified for aggregation; "
                "returning original DataFrame"
            )
            return df
            
        grp = df.groupBy([F.col(c) for c in group_by])
        agg_exprs = [F.expr(m["expr"]).alias(m["name"]) for m in metrics if "expr" in m and "name" in m]
        
        logger.info("Applying aggregations with arguments:")
        logger.info(f"  group_by: {group_by}")
        logger.info(f"  metrics:")
        for idx, metric in enumerate(metrics, 1):
            for k, v in metric.items():
                logger.info(f"    [{idx}] {k}: {v}")
                
        return grp.agg(*agg_exprs)
