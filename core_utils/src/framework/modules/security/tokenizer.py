"""Tokenization strategies for sensitive data"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat_ws, lit
import logging

logger = logging.getLogger(__name__)

class Tokenizer:
    """Hash and token-based masking for PII"""
    
    @staticmethod
    def hash_column(df: DataFrame, column: str, salt: str = "") -> DataFrame:
        """Hash sensitive column using SHA-256"""
        logger.info(f"Hashing column: {column}")
        df = df.withColumn(
            f"{column}_token",
            sha2(concat_ws("", col(column), lit(salt)), 256)
        )
        return df

    @staticmethod
    def create_token_mapping(df: DataFrame, column: str) -> DataFrame:
        """Create unique token mapping for column values"""
        logger.info(f"Creating token mapping for: {column}")
        return df
