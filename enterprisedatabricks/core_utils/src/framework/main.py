# framework/main.py

from pathlib import Path
import yaml
import logging
import inspect
import sys

from framework.core.arg_parser import ArgParser
from framework.core.config import ConfigManager
from framework.core.runtime import RuntimeContext
from framework.core.plan import build_execution_plan
from framework.core.plan_printer import print_plan
from framework.factory.ingestion_factory import IngestionFactory

# Log structure: Time - Level - LoggerName:FunctionName() - Message
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s:%(funcName)s() - %(message)s"
)

# Global logger instance
logger = logging.getLogger(__name__)


def load_yaml(path: str) -> dict:
    """Load and validate a YAML file, ensuring root is a mapping."""
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("YAML root must be a mapping")
    return data


def _handle_dry_run(args) -> None:
    """
    Dry-run path: validate config, build execution plan, print, and exit.

    This follows the **same pipeline path** as a real run up to the
    point of side effects:
        1. Load + validate YAML
        2. Build RuntimeContext
        3. IngestionFactory.create (spark=None → validates routing only)
        4. Build execution plan
        5. Print plan
        6. Return (no Spark, no reads, no writes)
    """
    # 1. Load & validate YAML
    raw = load_yaml(
        str(Path(args.config_directory_path) / args.layer / args.config_file_name)
    )
    ConfigManager.validate_config(["pipeline_metadata", "source"], raw)

    config = raw
    pipeline = config.get("pipeline_metadata", {})

    # 2. Build runtime context
    context = RuntimeContext(
        environment=pipeline.get("environment", "unknown"),
        pipeline_name=pipeline.get("name", "unknown"),
        run_mode=pipeline.get("run_mode", "execute"),
        dry_run=True,
    )

    # 3. Instantiate ingestion (no load)
    # spark is intentionally None in dry-run
    ingester = IngestionFactory.create(
        spark=None,
        config=config,
        env_manager=None,
    )

    # 4. Build execution plan
    plan = build_execution_plan(config)

    # 5. Print plan
    print_plan(plan)

    logger.info("Dry-run completed for pipeline '%s'.", context.pipeline_name)


def run_layer(layer_name: str, layer_class):
    """
    Utility function to run a specific medallion layer.

    Args:
        layer_name: Name of the layer (bronze, silver, gold)
        layer_class: The class constructor for the layer
    """
    logger.info(f"---- STARTING {layer_name.upper()} LAYER EXECUTION ----")

    # Get the calling function's name for entry_point tracking
    caller = inspect.stack()[1].function

    # Parse CLI arguments
    args = ArgParser.parse(entry_point=caller, layer=layer_name)

    # ── Dry-run branch ────────────────────────────────────────────
    if getattr(args, "dry_run", False):
        _handle_dry_run(args)
        return
    # ──────────────────────────────────────────────────────────────

    # Instantiate and run the layer
    layer = layer_class(args)
    layer.run()

    logger.info(f"---- {layer_name.upper()} LAYER EXECUTION COMPLETED ----")


def run_bronze():
    """Entry point for Bronze layer ingestion"""
    from framework.layers.bronze.bronze_ingestor import BronzeIngester
    run_layer("bronze", BronzeIngester)


def run_silver():
    """Entry point for Silver layer refinement"""
    from framework.layers.silver.silver_refiner import SilverRefiner
    run_layer("silver", SilverRefiner)


def run_gold():
    """Entry point for Gold layer aggregation"""
    from framework.layers.gold.gold_aggregator import GoldAggregator
    run_layer("gold", GoldAggregator)


def main():
    """
    Unified main entry point — the exact place where dry-run
    branches, and only once.

    Flow:
        1. parse args (--config_file_name, --config_directory_path, --dry-run, ...)
        2. load YAML
        3. validate_config()
        4. build RuntimeContext
        5. IngestionFactory.create  → validates connector
        6. if dry-run: build plan → print → exit(0)
        7. else: real execution path
    """
    args = ArgParser.parse()

    # 1. Load & validate YAML
    raw = load_yaml(
        str(Path(args.config_directory_path) / args.layer / args.config_file_name)
    )
    ConfigManager.validate_config(["pipeline_metadata", "source"], raw)

    config = raw
    pipeline = config.get("pipeline_metadata", {})

    # 2. Build runtime context
    context = RuntimeContext(
        environment=pipeline.get("environment", "unknown"),
        pipeline_name=pipeline.get("name", "unknown"),
        run_mode=pipeline.get("run_mode", "execute"),
        dry_run=getattr(args, "dry_run", False),
    )

    # 3. Instantiate ingestion (no load)
    # spark is intentionally None in dry-run
    ingester = IngestionFactory.create(
        spark=None,
        config=config,
        env_manager=None,
    )

    # 4. If dry-run: build + print execution plan and exit
    if context.dry_run:
        plan = build_execution_plan(config)
        print_plan(plan)
        return

    # 5. Real execution (future / existing)
    # df = ingester.load()
    # bronze/silver/gold execution happens here

    raise NotImplementedError("Execution path not wired yet")


if __name__ == "__main__":
    main()