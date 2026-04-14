# framework/main.py

from pathlib import Path
import yaml
import logging
import inspect

from framework.core.arg_parser import ArgParser
from framework.core.config import ConfigManager
from framework.core.plan import build_execution_plan
from framework.core.plan_printer import print_plan

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

    Flow:
        1. Load + validate YAML
        2. Build execution plan
        3. Print plan
        4. Return (no Spark, no reads, no writes)
    """
    # 1. Load & validate YAML
    raw = load_yaml(
        str(Path(args.config_directory_path) / args.layer / args.config_file_name)
    )
    ConfigManager.validate_config(["pipeline_metadata", "source"], raw)

    # 2. Build execution plan
    plan = build_execution_plan(raw)

    # 3. Print plan
    print_plan(plan)

    pipeline_name = raw.get("pipeline_metadata", {}).get("name", "unknown")
    logger.info("Dry-run completed for pipeline '%s'.", pipeline_name)


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
