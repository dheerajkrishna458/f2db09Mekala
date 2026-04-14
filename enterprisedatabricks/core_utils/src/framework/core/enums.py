# framework/core/enums.py

"""
Centralised enums for the Medallion pipeline framework.

Keep this lean — only promote a string to an enum when it is
referenced in more than one place or used for routing/branching.
"""

from enum import Enum


class Layer(str, Enum):
    """Medallion architecture layers."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD   = "gold"


class SourceType(str, Enum):
    """Supported ingestion source types (maps to YAML `source.type`)."""
    FILE        = "file"
    DELTA_TABLE = "delta_table"
    JDBC        = "jdbc"
    CLOUDFILES  = "cloudfiles"
    REST        = "rest"


class FileFormat(str, Enum):
    """Recognised file formats for file-based ingestion."""
    CSV     = "csv"
    PARQUET = "parquet"
    JSON    = "json"
    DELTA   = "delta"
    COBOL   = "cobol"


class WriteMode(str, Enum):
    """Delta write modes."""
    APPEND    = "append"
    OVERWRITE = "overwrite"
    MERGE     = "merge"
