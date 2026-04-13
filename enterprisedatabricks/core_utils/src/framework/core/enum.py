# framework/core/enum.py
"""
Centralised enumerations for the Medallion pipeline framework.

These enums provide type-safe constants used across the codebase —
arg parsing, config validation, IngestionFactory routing, plan
building, and layer execution.
"""

from enum import Enum


class Layer(str, Enum):
    """Medallion architecture layers."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class SourceType(str, Enum):
    """Supported ingestion source types (maps to IngestionFactory routing)."""
    FILE = "file"
    DELTA_TABLE = "delta_table"
    JDBC = "jdbc"
    CLOUDFILES = "cloudfiles"
    REST = "rest"


class FileFormat(str, Enum):
    """Supported file formats for file-based ingestion."""
    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"
    DELTA = "delta"
    COBOL = "cobol"
    AVRO = "avro"


class WriteMode(str, Enum):
    """Delta write modes for destination config."""
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"
    ERROR_IF_EXISTS = "errorifexists"


class RunMode(str, Enum):
    """Pipeline run modes."""
    EXECUTE = "execute"
    DRY_RUN = "dry_run"
    RESTORE = "restore"


class LoadStatus(str, Enum):
    """Audit log load status values."""
    SUCCESS = "success"
    FAILURE = "failure"


class DQCriticality(str, Enum):
    """Data quality check criticality levels."""
    WARN = "warn"
    ERROR = "error"


class Environment(str, Enum):
    """Deployment environments."""
    DEV = "dev"
    BETA = "beta"
    PROD = "prod"
