"""
IngestionFactory — Traffic controller for YAML-driven ingestion.

Translates a YAML declaration into a concrete ingestion strategy
without knowing anything about the data itself.

Usage:
    ingester = IngestionFactory.create(
        spark=spark_session,      # None during dry-run
        config=validated_config,
        env_manager=env_manager,
    )

The factory reads the source `type` from the YAML config and returns
the corresponding ingester instance.  When ``spark`` is ``None``
(dry-run mode) the factory still validates that the declared source
type is recognised and would be routable, but does **not** attempt to
instantiate a Spark reader.
"""

import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class IngestionFactory:
    """
    Factory that maps a YAML source declaration to a concrete ingester.

    Supported source types
    ----------------------
    - ``file``         → routes to format-specific file ingesters
                         (CSV, Parquet, JSON, EBCDIC/Cobol, etc.)
    - ``delta_table``  → reads from an existing Delta table
    - ``jdbc``         → (future) JDBC connector
    - ``cloudfiles``   → (future) Auto Loader / Cloud Files
    - ``rest``         → (future) REST API connector

    The factory is intentionally **data-agnostic**: it only inspects the
    ``source`` block of the YAML config to decide which ingester to
    return.  All data-specific logic lives inside the ingesters themselves.
    """

    # ------------------------------------------------------------------
    # Registry of supported source types
    # ------------------------------------------------------------------
    _SUPPORTED_TYPES = {
        "file",
        "delta_table",
        "jdbc",
        "cloudfiles",
        "rest",
    }

    # File-format → ingester class name mapping (used when source.type == "file")
    # These are resolved lazily to avoid importing pyspark at module level
    _FILE_FORMAT_CLASS_MAP = {
        "csv":   "CSVFileIngester",
        "cobol": "EBCDICFileIngester",
    }

    # ------------------------------------------------------------------
    # Lazy import helper
    # ------------------------------------------------------------------
    @staticmethod
    def _import_readers():
        """Import reader classes lazily to avoid pyspark dependency at import time."""
        from framework.modules.io.reader import (
            BaseFileIngester,
            CSVFileIngester,
            EBCDICFileIngester,
            DeltaTableIngester,
        )
        return {
            "BaseFileIngester": BaseFileIngester,
            "CSVFileIngester": CSVFileIngester,
            "EBCDICFileIngester": EBCDICFileIngester,
            "DeltaTableIngester": DeltaTableIngester,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    @classmethod
    def create(
        cls,
        spark,
        config: Dict[str, Any],
        env_manager,
    ):
        """
        Create and return the appropriate ingester for the given config.

        Parameters
        ----------
        spark : SparkSession or None
            Active Spark session.  Pass ``None`` for dry-run mode — the
            factory will validate the routing but skip Spark instantiation.
        config : dict
            Full pipeline YAML config (must contain a ``source`` block).
        env_manager : EnvironmentManager
            Environment manager for path/catalog resolution.

        Returns
        -------
        ingester instance or None
            An ingester ready to call ``.read_df()`` on, or ``None`` when
            running in dry-run mode.

        Raises
        ------
        ValueError
            If the declared source type or file format is not supported.
        """
        source_config = config.get("source", {})
        source_type = source_config.get("type")
        properties = source_config.get("properties", source_config)

        if not source_type:
            raise ValueError(
                "YAML config is missing 'source.type'. "
                "Cannot determine ingestion strategy."
            )

        # Validate the source type is supported
        cls._validate_source_type(source_type)

        logger.info(
            "IngestionFactory routing: source.type='%s'", source_type
        )

        # ----- Dry-run: validate only, do not build ingester -----
        if spark is None:
            logger.info(
                "Dry-run mode: source type '%s' validated successfully. "
                "Skipping ingester instantiation.",
                source_type,
            )
            # For file types, also validate the format is supported
            if source_type == "file":
                cls._validate_file_format(properties)
            return None

        # ----- Real run: build and return the concrete ingester -----
        readers = cls._import_readers()

        match source_type:
            case "file":
                return cls._create_file_ingester(spark, env_manager, properties, readers)

            case "delta_table":
                return readers["DeltaTableIngester"](spark, env_manager, properties)

            case "jdbc":
                return cls._create_jdbc_ingester(spark, env_manager, properties)

            case "cloudfiles":
                return cls._create_cloudfiles_ingester(spark, env_manager, properties)

            case "rest":
                return cls._create_rest_ingester(spark, env_manager, properties)

            case _:
                raise ValueError(f"Unsupported source type: {source_type}")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------
    @classmethod
    def _validate_source_type(cls, source_type: str) -> None:
        """Raises ValueError if the source type is not in the registry."""
        if source_type not in cls._SUPPORTED_TYPES:
            raise ValueError(
                f"Unsupported source type: '{source_type}'. "
                f"Supported types: {sorted(cls._SUPPORTED_TYPES)}"
            )

    @classmethod
    def _validate_file_format(cls, properties: Dict) -> None:
        """Validates that a file-based source has a recognised format."""
        file_format = (properties.get("format") or "").lower()
        if not file_format:
            raise ValueError(
                "File source is missing 'format' in properties. "
                "Expected one of: csv, parquet, json, delta, cobol, etc."
            )
        logger.info(
            "Dry-run mode: file format '%s' validated.", file_format
        )

    @classmethod
    def _create_file_ingester(cls, spark, env_manager, properties: Dict, readers: Dict):
        """Route to the correct file ingester based on format."""
        file_format = (properties.get("format") or "").lower()

        if not file_format:
            raise ValueError("File source is missing 'format' in properties.")

        # Check if there's a specialised ingester for this format
        class_name = cls._FILE_FORMAT_CLASS_MAP.get(file_format, "BaseFileIngester")
        ingester_class = readers[class_name]

        logger.info(
            "Creating %s for format '%s'.",
            ingester_class.__name__,
            file_format,
        )
        return ingester_class(spark, env_manager, properties)

    @classmethod
    def _create_jdbc_ingester(cls, spark, env_manager, properties: Dict):
        """
        Create a JDBC ingester.

        Future implementation — will read from JDBC sources using
        Spark's JDBC connector.
        """
        raise NotImplementedError(
            "JDBC ingestion is planned but not yet implemented. "
            "Expected config: source.type='jdbc' with connection_url, "
            "driver, query/table properties."
        )

    @classmethod
    def _create_cloudfiles_ingester(cls, spark, env_manager, properties: Dict):
        """
        Create a Cloud Files (Auto Loader) ingester.

        Future implementation — will use Databricks Auto Loader for
        incremental file ingestion.
        """
        raise NotImplementedError(
            "CloudFiles (Auto Loader) ingestion is planned but not yet "
            "implemented. Expected config: source.type='cloudfiles' with "
            "path, format, and schema properties."
        )

    @classmethod
    def _create_rest_ingester(cls, spark, env_manager, properties: Dict):
        """
        Create a REST API ingester.

        Future implementation — will call REST APIs and convert
        responses to DataFrames.
        """
        raise NotImplementedError(
            "REST API ingestion is planned but not yet implemented. "
            "Expected config: source.type='rest' with url, method, "
            "headers, and auth properties."
        )
