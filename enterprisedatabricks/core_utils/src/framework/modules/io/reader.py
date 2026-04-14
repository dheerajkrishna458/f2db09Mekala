import logging
import re
from datetime import date, timedelta
from collections import defaultdict
from typing import List, Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from framework.core.config import ConfigManager
from framework.core.session import SessionManager
from framework.modules.utils.common import to_bool


class BaseFileIngester:
    """
    Ingester implementation for files using standard Spark approach.
    """

    def __init__(self, spark: SparkSession, env_manager, config: Dict):
        """
        Args:
            spark: Active Spark Session.
            env_manager: Manages environment-specific information.
            config: Dictionary identifying source properties (e.g., source_path, format, options).
        """
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__qualname__}")

        self.spark = spark
        self.dbutils = SessionManager.get_dbutils(spark)
        self.env_manager = env_manager
        self.config = config
        self.discovered_files = []

    # -----------------------------
    # Path Discovery helpers
    # -----------------------------
    def get_last_n_days_paths(self, base_path: str, n: int = 1) -> List[str]:
        base_path = base_path.rstrip("/")
        today = date.today()
        oldest = today - timedelta(days=n - 1)

        validated = []

        def list_numeric_folders(path):
            try:
                # List folders and filter those with numeric names (e.g., year, month, day)
                return sorted(
                    [int(e.name.rstrip("/")) for e in self.dbutils.fs.ls(path)
                    if e.name.rstrip("/").isdigit()],
                    reverse=True
                )
            except Exception:
                return []

        # YEARS
        years = [y for y in list_numeric_folders(base_path) if y >= oldest.year]

        for y in years:
            yp = f"{base_path}/{y:04d}"
            months = list_numeric_folders(yp)
            if y == oldest.year:
                months = [m for m in months if m >= oldest.month]

            # MONTHS
            for m in months:
                mp = f"{yp}/{m:02d}"
                days = list_numeric_folders(mp)
                if y == oldest.year and m == oldest.month:
                    days = [d for d in days if d >= oldest.day]

                # DAYS
                validated.extend(f"{mp}/{d:02d}" for d in days)

        validated.sort(reverse=True)

        return validated

    def _get_files_with_extension(self, folder_paths: List[str], ext: str) -> List:
        """
        Returns FileInfo objects for files ending with `ext` from given folder paths.
        """
        matched = []
        self.logger.info(f"Filtering files with extension '{ext}' in discovered folders.")
        for folder in folder_paths:
            try:
                files = self.dbutils.fs.ls(folder)
                for f in files:
                    # Add files matching the given extension
                    if (not ext or f.path.endswith(ext)):
                        matched.append(f)
            except Exception as e:
                self.logger.info(f"Could not list files in {folder}: {e}")
        return matched

    def _column_contains_invalid_characters(self, elements: List[str]) -> List[str]:
        """
        Checks if any column names contain characters invalid for Delta column names.
        """
        pattern = re.compile(r"[ ,;{}()\n\t=]")
        return [element for element in elements if pattern.search(element)]

    def _discover_date_hierarchy(self, base_path: str, discover_cfg: Dict, ext: str) -> List:
        """
        Implements the 'date_hierarchy' discovery strategy.
        Returns a list of FileInfo objects.
        """
        last_n_days = int(discover_cfg.get("last_n_days", 1))
        pick = discover_cfg.get("pick")
        self.logger.info(
            f"Discovering files for '{pick}' data over the last {last_n_days} day(s) with extension '{ext}'"
        )
        # Find folders for the last n days using date hierarchy
        folders = self.get_last_n_days_paths(base_path, last_n_days)

        if not folders:
            self.logger.info(f"No date-hierarchy folders found under '{base_path}'")
            return []

        return self._get_files_with_extension(folders, ext)

    def _discover_paths(self, base_path: str, cfg: Dict) -> List:
        """
        Returns a list of FileInfo objects to pass into Spark reader.

        Example:
            [
                FileInfo(path='<file_path>', name='<file_name1.csv>', size=4096, modificationTime=1774253613335),
                FileInfo(path='<file_path>', name='<file_name2.csv>', size=2048, modificationTime=1774253613340)
            ]
        """
        discover = cfg.get("discover") or {}
        ext = cfg.get("extension", "")

        discover_type = discover.get("type")
        if discover_type == "date_hierarchy":
            # Use date hierarchy discovery if specified
            return self._discover_date_hierarchy(base_path, discover, ext)

        # Default behavior: either read the folder as-is, or pick files by extension at root
        if ext:
            return self._get_files_with_extension([base_path], ext)
        # If no extension, return all files in the base_path folder
        try:
            files = self.dbutils.fs.ls(base_path)
            return [f for f in files if not f.isDir()]
        except Exception as e:
            self.logger.info(f"Could not list files in {base_path}: {e}")
            return []

    def _resolve_and_discover_files(self) -> Optional[List]:
        """Resolves source path and discovers files to ingest."""
        # Resolve source path
        source_path = self.env_manager.construct_source_folder_path(self.config["source_path"])
        self.logger.info(f"Resolved source path: {source_path}")

        # Discover files to ingest
        file_infos = self._discover_paths(source_path, self.config)
        self.discovered_files = file_infos

        for f in file_infos:
            self.logger.info(f"Discovered: {f.path}")

        if not file_infos:
            self.logger.warning(f"No files found to read for path: {source_path}")
            return None

        return file_infos

    # -----------------------------
    # Public Method
    # -----------------------------

    def read_df(self) -> Optional[DataFrame]:
        """Reads data using a Spark standard read operation."""

        file_infos = self._resolve_and_discover_files()
        if not file_infos:
            return

        file_paths = [f.path for f in file_infos if not f.isDir()]
        file_format = self.config.get("format")
        options = self.config.get("options", {}) or {}

        # Standard read for other formats
        reader = self.spark.read.format(file_format).options(**options)
        df = reader.load(file_paths)

        self.logger.info("DataFrame read operation completed.")
        return df


class CSVFileIngester(BaseFileIngester):
    """
    Ingester implementation for csv files using standard Spark approach.
    """
    def _sanitize_column_names(self, df: DataFrame) -> DataFrame:
        """
        Replaces Delta-invalid characters in top-level column names.
        """
        invalid_pattern = re.compile(r"[ ,;{}()\n\t=]")

        used_names = set()
        new_names: List[str] = []
        changed = False

        for col_name in df.columns:
            sanitized = invalid_pattern.sub("_", col_name).strip("_")
            sanitized = re.sub(r"_+", "_", sanitized)
            if not sanitized:
                sanitized = "col"

            base = sanitized
            suffix = 1
            while sanitized in used_names:
                sanitized = f"{base}_{suffix}"
                suffix += 1

            used_names.add(sanitized)
            new_names.append(sanitized)
            if sanitized != col_name:
                changed = True

        if changed:
            self.logger.info("Sanitized invalid characters in DataFrame column names for Delta compatibility.")
            return df.toDF(*new_names)
        return df

    def _get_multi_split_header_mode(self, cfg: Dict) -> str:
        """
        Returns configured multi-split header mode.
        Supported: auto | no_header | all_header | one_header
        """
        discover_cfg = cfg.get("discover", {}) or {}
        multi_split_cfg = cfg.get("multi_split", discover_cfg.get("multi_split", {}))

        mode = "auto"
        if isinstance(multi_split_cfg, dict):
            mode = (multi_split_cfg.get("header") or "auto").strip().lower()

        supported = {"auto", "no_header", "all_header", "one_header"}
        if mode not in supported:
            self.logger.warning(
                "Unsupported multi_split.header mode '%s'. Falling back to 'auto'.",
                mode,
            )
            return "auto"
        return mode

    def _is_multi_split_csv(self, file_paths: List[str], cfg: Dict) -> bool:
        """
        Determines whether mixed-header multi-split handling should be used.
        """
        multi_split_cfg = cfg.get("multi_split")

        # Check if multi_split is enabled in config
        if isinstance(multi_split_cfg, dict):
            is_enabled = to_bool(multi_split_cfg.get("enabled"))
        else:
            is_enabled = to_bool(multi_split_cfg)

        # Only proceed if multi_split is enabled
        if not is_enabled:
            return False

        folder_counts = defaultdict(int)
        # Count files per folder to detect multi-split scenario
        for path in file_paths:
            if path.endswith("/"):
                continue
            folder = path.rsplit("/", 1)[0] if "/" in path else path
            folder_counts[folder] += 1

        # Multi-split CSV detected if any folder has more than one file
        return any(count > 1 for count in folder_counts.values())

    def _get_paths_with_and_without_header(self, file_paths_list: List[str]) -> tuple[List[str], List[str]]:
        """
        Splits CSV paths into likely header and no-header groups for multi-split ingestion.
        """
        options = self.config.get("options", {}) or {}
        ext = self.config.get("extension", "")

        if ext:
            ext = ext.lstrip(".")
            paths_likely_with_header = [p for p in file_paths_list if p.lower().endswith(f"_1.{ext.lower()}")]
        else:
            paths_likely_with_header = [
                p for p in file_paths_list
                if re.search(r"_1\.[^/]+$", p, flags=re.IGNORECASE)
            ]

        reader_options = {
            "encoding": options.get("encoding", "UTF-8"),
            "delimiter": options.get("delimiter", ","),
            "header": True,
            "inferSchema": options.get("inferSchema", "false"),
        }

        reference_columns = None
        if paths_likely_with_header:
            reference_columns = self.spark.read.options(**reader_options).csv(paths_likely_with_header).columns
            invalid_reference_columns = self._column_contains_invalid_characters(reference_columns)
            if invalid_reference_columns:
                self.logger.info(
                    "Header inference from *_1 file(s) appears invalid (%s). Treating all split files as no-header.",
                    invalid_reference_columns,
                )
                paths_likely_with_no_header = list(paths_likely_with_header)
                paths_likely_with_header = []
                remaining_paths = [p for p in file_paths_list if p not in paths_likely_with_no_header]
                paths_likely_with_no_header.extend(remaining_paths)
                return paths_likely_with_header, paths_likely_with_no_header

        remaining_paths = [p for p in file_paths_list if p not in paths_likely_with_header]
        paths_likely_with_no_header: List[str] = []

        for path in remaining_paths:
            try:
                columns_list = self.spark.read.options(**reader_options).csv(path).columns
                if reference_columns:
                    if reference_columns == columns_list:
                        paths_likely_with_header.append(path)
                    else:
                        paths_likely_with_no_header.append(path)
                else:
                    invalid_columns = self._column_contains_invalid_characters(columns_list)
                    if invalid_columns:
                        self.logger.info(
                            "Path %s appears to be no-header split file due to invalid header chars: %s",
                            path,
                            invalid_columns,
                        )
                        paths_likely_with_no_header.append(path)
                    else:
                        paths_likely_with_header.append(path)
            except Exception as error:
                self.logger.info(
                    "Exception while inferring header for path %s. Treating as no-header split. Error: %s",
                    path,
                    str(error)[:255],
                )
                paths_likely_with_no_header.append(path)

        return paths_likely_with_header, paths_likely_with_no_header

    def _get_one_header_split_paths(self, file_paths_list: List[str]) -> tuple[List[str], List[str]]:
        """
        Returns (header_paths, no_header_paths) for one_header mode.
        Header paths are detected using *_1.<ext> naming convention.
        """
        ext = (self.config.get("extension") or "").strip().lstrip(".").lower()

        if ext:
            header_paths = [p for p in file_paths_list if p.lower().endswith(f"_1.{ext}")]
        else:
            header_paths = [p for p in file_paths_list if re.search(r"_1\.[^/]+$", p, flags=re.IGNORECASE)]

        no_header_paths = [p for p in file_paths_list if p not in header_paths]
        return header_paths, no_header_paths
    
    def _read_multi_split_paths_as_df(self, file_paths_list: List[str]) -> DataFrame:
        """
        Reads multi-split CSV files honoring multi_split.header mode:
        - no_header
        - all_header
        - one_header
        - auto
        """
        mode = self._get_multi_split_header_mode(self.config)

        options = self.config.get("options", {}) or {}
        base_reader_options = {
            "encoding": options.get("encoding", "UTF-8"),
            "delimiter": options.get("delimiter", ","),
            "inferSchema": options.get("inferSchema", "false"),
            "mode": options.get("mode", "PERMISSIVE"),
        }

        header_reader_options = {**base_reader_options, "header": True}
        no_header_reader_options = {**base_reader_options, "header": False}

        if "quote" in options:
            header_reader_options["quote"] = options["quote"]
            no_header_reader_options["quote"] = options["quote"]
        if "escape" in options:
            header_reader_options["escape"] = options["escape"]
            no_header_reader_options["escape"] = options["escape"]

        if mode == "no_header":
            return (
                self.spark.read
                .options(**no_header_reader_options)
                .csv(file_paths_list)
                .select("*", F.col("_metadata.file_name").alias("file_name"))
            )

        if mode == "all_header":
            return (
                self.spark.read
                .options(**header_reader_options)
                .csv(file_paths_list)
                .select("*", F.col("_metadata.file_name").alias("file_name"))
            )

        if mode == "one_header":
            paths_likely_with_header, paths_likely_with_no_header = self._get_one_header_split_paths(file_paths_list)
            if not paths_likely_with_header:
                raise ValueError(
                    "multi_split.header='one_header' requires at least one header file (for example: *_1.<ext>)."
                )
        else:
            paths_likely_with_header, paths_likely_with_no_header = self._get_paths_with_and_without_header(file_paths_list)

        # auto mode fallback: if no header file is detected, treat all as no-header.
        if mode == "auto" and not paths_likely_with_header:
            self.logger.info("multi_split.header='auto' detected no header files. Reading all splits as no-header.")
            return (
                self.spark.read
                .options(**no_header_reader_options)
                .csv(file_paths_list)
                .select("*", F.col("_metadata.file_name").alias("file_name"))
            )

        header_base_df = (
            self.spark.read
            .options(**header_reader_options)
            .csv(paths_likely_with_header)
        )
        header_df = header_base_df.select("*", F.col("_metadata.file_name").alias("file_name"))

        if not paths_likely_with_no_header:
            return header_df

        no_header_df = (
            self.spark.read
            .options(**no_header_reader_options)
            .schema(header_base_df.schema)
            .csv(paths_likely_with_no_header)
            .select("*", F.col("_metadata.file_name").alias("file_name"))
        )

        return header_df.unionByName(no_header_df)

    def read_df(self) -> Optional[DataFrame]:
        """Reads data using a Spark standard read operation."""

        file_infos = self._resolve_and_discover_files()
        if not file_infos:
            return

        file_paths = [f.path for f in file_infos if not f.isDir()]
        file_format = self.config.get("format")
        options = self.config.get("options", {}) or {}

        # Handle multi-split CSVs
        if self._is_multi_split_csv(file_paths, self.config):
            mode = self._get_multi_split_header_mode(self.config)
            self.logger.info(f"Detected multi-split CSV input; applying header_mode: '{mode}'.")
            df = self._read_multi_split_paths_as_df(file_paths)
            df = self._sanitize_column_names(df)
            self.logger.info("DataFrame read operation completed with multi-split handling.")
            return df

        # Standard read for other formats
        reader = self.spark.read.format(file_format).options(**options)
        df = reader.load(file_paths)

        # Sanitize CSV column names
        df = self._sanitize_column_names(df)

        self.logger.info("DataFrame read operation completed.")
        return df

class EBCDICFileIngester(BaseFileIngester):
    """
    Ingester implementation for EBCDIC/COBOL files using cobrix approach.
    """

    def _read_ebcdic_files_as_df(self, file_infos, cobol_reader) -> Optional[DataFrame]:
        """
        Reads EBCDIC/Cobol files using the provided cobol_reader and returns a unified DataFrame.

        Args:
            file_infos: List of FileInfo objects representing EBCDIC files to ingest.
            cobol_reader: Spark DataFrameReader configured for Cobol format.

        Returns:
            DataFrame containing all records from the input files, with an added 'file_name' column.
            Returns None if no files are found.
        """
        dfs = []
        for file in file_infos:
            # Read each EBCDIC file and add a column with the file name for traceability
            df = cobol_reader.load(file.path)
            df = df.withColumn("file_name", F.lit(file.name))
            dfs.append(df)

        if not dfs:
            # Return None if no files were ingested
            return None

        # Union all DataFrames to produce a single DataFrame
        df_union = dfs[0]
        for df_next in dfs[1:]:
            df_union = df_union.unionByName(df_next)
        return df_union

    def read_df(self) -> Optional[DataFrame]:
        """Reads data using a Spark standard read operation."""

        file_infos = self._resolve_and_discover_files()
        if not file_infos:
            return

        file_paths = [f.path for f in file_infos if not f.isDir()]
        file_format = self.config.get("format")
        options = self.config.get("options", {}) or {}

        # Handle EBCDIC/Cobol files
        copybook_path = self.env_manager.construct_source_folder_path(options.get("copybook"))
        options["copybook"] = copybook_path
        self.logger.info(f"Resolved EBCDIC copybook path: {copybook_path}")
        cobol_reader = self.spark.read.format("cobol").options(**options)
        return self._read_ebcdic_files_as_df(file_infos, cobol_reader)


class DeltaTableIngester:
    """
    Ingester implementation for reading directly from a Delta Table.
    Useful for Silver/Gold transformations where source is already in Delta Lake.
    """
    def __init__(self, spark, env_manager, config):
        """
        Args:
            spark: Active Spark Session.
            env_manager: Manages environment-specific information.
            config: Dictionary identifying source properties (e.g., table name).
        """
        self.spark = spark
        self.env_manager = env_manager
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def read_df(self):
        
        table_fqn = self.env_manager.construct_table_fqn(self.config['table'])
        self.logger.info(f"Reading from Delta Table: {table_fqn}")

        # Read the Delta table as a DataFrame
        df = self.spark.table(table_fqn)

        self.logger.info("Delta Table read operation completed.")
        return df
